// Package store persists versioned rule snapshots, verdict snapshots
// and the error ledger in SQLite. All multi-row writes happen inside a
// single transaction so a killed process never leaves partial state:
// committed data survives, uncommitted data is discarded wholesale.
package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"

	_ "modernc.org/sqlite"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/engine"
	"fwadjudge/internal/fixed"
	"fwadjudge/internal/ledger"
	"fwadjudge/internal/model"
)

// CommitMark is a fixed sampling marker; no wall-clock time is stored.
const CommitMark = "SAMPLE@FIXED-0"

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(delete)")
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS snapshots(
			snapshot_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			content_hash TEXT NOT NULL,
			rule_count INTEGER NOT NULL,
			replaced INTEGER NOT NULL DEFAULT 0,
			commit_mark TEXT NOT NULL,
			PRIMARY KEY(snapshot_id, version))`,
		`CREATE TABLE IF NOT EXISTS rules(
			snapshot_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			rule_index INTEGER NOT NULL,
			direction TEXT NOT NULL,
			protocol TEXT NOT NULL,
			src_cidr TEXT NOT NULL,
			dst_cidr TEXT NOT NULL,
			port_lo INTEGER NOT NULL,
			port_hi INTEGER NOT NULL,
			action TEXT NOT NULL,
			priority INTEGER NOT NULL,
			rule_hash TEXT NOT NULL,
			PRIMARY KEY(snapshot_id, version, rule_index))`,
		`CREATE TABLE IF NOT EXISTS verdicts(
			snapshot_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			probe_id TEXT NOT NULL,
			action TEXT NOT NULL,
			hit_rule INTEGER NOT NULL,
			implicit INTEGER NOT NULL,
			PRIMARY KEY(snapshot_id, version, probe_id))`,
		`CREATE TABLE IF NOT EXISTS divergences(
			snap_a TEXT NOT NULL,
			snap_b TEXT NOT NULL,
			probe_id TEXT NOT NULL,
			rule_a INTEGER NOT NULL,
			rule_b INTEGER NOT NULL,
			field TEXT NOT NULL,
			class TEXT NOT NULL,
			lo_a TEXT NOT NULL,
			hi_a TEXT NOT NULL,
			lo_b TEXT NOT NULL,
			hi_b TEXT NOT NULL,
			PRIMARY KEY(snap_a, snap_b, probe_id))`,
		`CREATE TABLE IF NOT EXISTS ledger(
			snapshot_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			rule_index INTEGER NOT NULL,
			field TEXT NOT NULL,
			amount INTEGER NOT NULL,
			PRIMARY KEY(snapshot_id, version, seq))`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotMeta describes one committed snapshot version.
type SnapshotMeta struct {
	SnapshotID  string
	Version     int
	ContentHash string
	RuleCount   int
	Replaced    bool
}

func hashRule(r *model.Rule) string {
	sum := sha256.Sum256([]byte(r.Canonical()))
	return hex.EncodeToString(sum[:])
}

func hashContent(hashes []string) string {
	h := sha256.New()
	for _, s := range hashes {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// CommitSnapshot commits one parsed rule snapshot in a single
// transaction with the next incremental version (or forcedVersion when
// > 0, used only by external fault injection). It detects
// whole-replacement updates by comparing rule content hashes with the
// previous version. Ledger entries staged during parsing are committed
// atomically in the same transaction.
func (s *Store) CommitSnapshot(snapshotID string, rules []model.Rule, entries []ledger.Entry, forcedVersion int) (SnapshotMeta, *codes.Error) {
	meta := SnapshotMeta{SnapshotID: snapshotID, RuleCount: len(rules)}
	tx, err := s.db.Begin()
	if err != nil {
		return meta, codes.New(codes.ESnapshotVersion, err.Error())
	}
	defer tx.Rollback()

	version := forcedVersion
	if version <= 0 {
		row := tx.QueryRow(`SELECT COALESCE(MAX(version),0)+1 FROM snapshots WHERE snapshot_id=?`, snapshotID)
		if err := row.Scan(&version); err != nil {
			return meta, codes.New(codes.ESnapshotVersion, err.Error())
		}
	}
	meta.Version = version

	sorted := make([]model.Rule, len(rules))
	copy(sorted, rules)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Index < sorted[j].Index })
	hashes := make([]string, 0, len(sorted))
	for i := range sorted {
		h := hashRule(&sorted[i])
		hashes = append(hashes, h)
		if _, err := tx.Exec(`INSERT INTO rules
			(snapshot_id,version,rule_index,direction,protocol,src_cidr,dst_cidr,port_lo,port_hi,action,priority,rule_hash)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			snapshotID, version, sorted[i].Index, string(sorted[i].Direction), string(sorted[i].Protocol),
			sorted[i].SrcCIDR, sorted[i].DstCIDR, sorted[i].PortLo, sorted[i].PortHi,
			string(sorted[i].Action), sorted[i].Priority, h); err != nil {
			return meta, codes.New(codes.ESnapshotVersion, err.Error())
		}
	}
	meta.ContentHash = hashContent(hashes)

	// Whole-replacement detection: the version number may increment,
	// but if no rule content survives from the previous version the
	// ruleset was replaced wholesale.
	prevHashes := map[string]bool{}
	rows, err := tx.Query(`SELECT rule_hash FROM rules WHERE snapshot_id=? AND version=?`, snapshotID, version-1)
	if err != nil {
		return meta, codes.New(codes.ESnapshotVersion, err.Error())
	}
	prevExists := false
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			rows.Close()
			return meta, codes.New(codes.ESnapshotVersion, err.Error())
		}
		prevExists = true
		prevHashes[h] = true
	}
	rows.Close()
	if prevExists && len(hashes) > 0 {
		meta.Replaced = true
		for _, h := range hashes {
			if prevHashes[h] {
				meta.Replaced = false
				break
			}
		}
	}
	replaced := 0
	if meta.Replaced {
		replaced = 1
	}
	if _, err := tx.Exec(`INSERT INTO snapshots(snapshot_id,version,content_hash,rule_count,replaced,commit_mark)
		VALUES(?,?,?,?,?,?)`, snapshotID, version, meta.ContentHash, meta.RuleCount, replaced, CommitMark); err != nil {
		return meta, codes.New(codes.ESnapshotVersion, err.Error())
	}
	for _, e := range entries {
		if _, err := tx.Exec(`INSERT INTO ledger(snapshot_id,version,seq,rule_index,field,amount)
			VALUES(?,?,?,?,?,?)`, snapshotID, version, e.Seq, e.RuleIndex, e.Field, e.Amount); err != nil {
			return meta, codes.New(codes.ESnapshotVersion, err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		return meta, codes.New(codes.ESnapshotVersion, err.Error())
	}
	return meta, nil
}

// LoadChain validates version-chain continuity for one snapshot and
// returns the latest continuous version. A gap yields
// E_SNAPSHOT_VERSION and the caller must fall back to the returned
// (latest continuous) version.
func (s *Store) LoadChain(snapshotID string) (int, *codes.Error) {
	rows, err := s.db.Query(`SELECT version FROM snapshots WHERE snapshot_id=? ORDER BY version`, snapshotID)
	if err != nil {
		return 0, codes.New(codes.ESnapshotVersion, err.Error())
	}
	defer rows.Close()
	latest := 0
	expect := 1
	var gapAt int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return 0, codes.New(codes.ESnapshotVersion, err.Error())
		}
		if v != expect && gapAt == 0 {
			gapAt = v
		}
		if v == expect {
			latest = v
			expect++
		}
	}
	if gapAt > 0 {
		return latest, codes.At(codes.ESnapshotVersion,
			fmt.Sprintf("version chain gap at %d, rolled back to %d", gapAt, latest),
			snapshotID, -1, -1)
	}
	return latest, nil
}

// LoadRules loads the rules of one committed snapshot version.
func (s *Store) LoadRules(snapshotID string, version int) ([]model.Rule, error) {
	rows, err := s.db.Query(`SELECT rule_index,direction,protocol,src_cidr,dst_cidr,port_lo,port_hi,action,priority
		FROM rules WHERE snapshot_id=? AND version=? ORDER BY rule_index`, snapshotID, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Rule
	for rows.Next() {
		var r model.Rule
		var dir, proto, act string
		if err := rows.Scan(&r.Index, &dir, &proto, &r.SrcCIDR, &r.DstCIDR,
			&r.PortLo, &r.PortHi, &act, &r.Priority); err != nil {
			return nil, err
		}
		r.SnapshotID = snapshotID
		r.Direction = model.Direction(dir)
		r.Protocol = model.Protocol(proto)
		r.Action = model.Action(act)
		if err := r.Normalize(); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// VerdictRow is one persisted verdict.
type VerdictRow struct {
	SnapshotID string
	Version    int
	ProbeID    string
	Action     string
	HitRule    int
	Implicit   bool
}

// CommitVerdicts commits one whole round of verdicts, divergences and
// ledger entries in a single transaction. versions maps each snapshot
// id to the rule-snapshot version the verdicts were computed against.
func (s *Store) CommitVerdicts(versions map[string]int, verdicts []engine.Verdict, divs []engine.Divergence, entries []ledger.Entry) *codes.Error {
	tx, err := s.db.Begin()
	if err != nil {
		return codes.New(codes.ESnapshotVersion, err.Error())
	}
	defer tx.Rollback()
	for _, v := range verdicts {
		implicit := 0
		if v.Implicit {
			implicit = 1
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO verdicts(snapshot_id,version,probe_id,action,hit_rule,implicit)
			VALUES(?,?,?,?,?,?)`, v.SnapshotID, versions[v.SnapshotID], v.ProbeID, string(v.Action), v.HitRule, implicit); err != nil {
			return codes.New(codes.ESnapshotVersion, err.Error())
		}
	}
	for _, d := range divs {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO divergences(snap_a,snap_b,probe_id,rule_a,rule_b,field,class,lo_a,hi_a,lo_b,hi_b)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			d.SnapA, d.SnapB, d.ProbeID, d.RuleA, d.RuleB, d.Field, d.Class,
			d.LoA.String(), d.HiA.String(), d.LoB.String(), d.HiB.String()); err != nil {
			return codes.New(codes.ESnapshotVersion, err.Error())
		}
	}
	for _, e := range entries {
		if _, err := tx.Exec(`INSERT INTO ledger(snapshot_id,version,seq,rule_index,field,amount)
			VALUES(?,?,?,?,?,?)`, e.SnapshotID, versions[e.SnapshotID], e.Seq, e.RuleIndex, e.Field, e.Amount); err != nil {
			return codes.New(codes.ESnapshotVersion, err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		return codes.New(codes.ESnapshotVersion, err.Error())
	}
	return nil
}

// ListSnapshots returns all committed snapshot versions, sorted by
// snapshot id and version.
func (s *Store) ListSnapshots() ([]SnapshotMeta, error) {
	rows, err := s.db.Query(`SELECT snapshot_id,version,content_hash,rule_count,replaced FROM snapshots
		ORDER BY snapshot_id, version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotMeta
	for rows.Next() {
		var m SnapshotMeta
		var replaced int
		if err := rows.Scan(&m.SnapshotID, &m.Version, &m.ContentHash, &m.RuleCount, &replaced); err != nil {
			return nil, err
		}
		m.Replaced = replaced != 0
		out = append(out, m)
	}
	return out, rows.Err()
}

// LedgerEntries returns committed ledger entries of one snapshot.
func (s *Store) LedgerEntries(snapshotID string) ([]ledger.Entry, error) {
	rows, err := s.db.Query(`SELECT version,seq,rule_index,field,amount FROM ledger
		WHERE snapshot_id=? ORDER BY version, seq`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ledger.Entry
	for rows.Next() {
		var e ledger.Entry
		var version int
		if err := rows.Scan(&version, &e.Seq, &e.RuleIndex, &e.Field, &e.Amount); err != nil {
			return nil, err
		}
		e.SnapshotID = snapshotID
		out = append(out, e)
	}
	return out, rows.Err()
}

// LedgerBound returns the aggregated committed error bound (ErrScale
// units) for one snapshot.
func (s *Store) LedgerBound(snapshotID string) (int64, error) {
	var sum sql.NullInt64
	row := s.db.QueryRow(`SELECT SUM(amount) FROM ledger WHERE snapshot_id=?`, snapshotID)
	if err := row.Scan(&sum); err != nil {
		return 0, err
	}
	return sum.Int64, nil
}

// VerdictCount returns the number of committed verdicts of a snapshot.
func (s *Store) VerdictCount(snapshotID string) (int, error) {
	var n int
	row := s.db.QueryRow(`SELECT COUNT(*) FROM verdicts WHERE snapshot_id=?`, snapshotID)
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// Divergences returns all committed divergence rows, stably sorted.
func (s *Store) Divergences() ([]engine.Divergence, error) {
	rows, err := s.db.Query(`SELECT snap_a,snap_b,probe_id,rule_a,rule_b,field,class,lo_a,hi_a,lo_b,hi_b
		FROM divergences ORDER BY snap_a, snap_b, probe_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []engine.Divergence
	for rows.Next() {
		var d engine.Divergence
		var loA, hiA, loB, hiB string
		if err := rows.Scan(&d.SnapA, &d.SnapB, &d.ProbeID, &d.RuleA, &d.RuleB,
			&d.Field, &d.Class, &loA, &hiA, &loB, &hiB); err != nil {
			return nil, err
		}
		d.LoA = mustFixed(loA)
		d.HiA = mustFixed(hiA)
		d.LoB = mustFixed(loB)
		d.HiB = mustFixed(hiB)
		out = append(out, d)
	}
	return out, rows.Err()
}

func mustFixed(s string) fixed.Fixed {
	v, _, err := fixed.ParseDecimal(s)
	if err != nil {
		return 0
	}
	return v
}
