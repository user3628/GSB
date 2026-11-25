// Package store implements transactional, rollback-able pruning version
// persistence on top of pure-Go SQLite plus a real OS file lock.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"headprune/internal/model"
	"headprune/internal/store/filelock"

	_ "modernc.org/sqlite"
)

// VersionRow is one committed pruning decision version.
type VersionRow struct {
	Version        int64  `json:"version"`
	Status         string `json:"status"`
	HeadFP         string `json:"head_fingerprint"`
	ProbeDigest    string `json:"probe_digest"`
	QuantFP        string `json:"quant_fingerprint"`
	DecisionDigest string `json:"decision_digest"`
	InputDigest    string `json:"input_digest"`
	DecisionJSON   string `json:"decision_json"`
	ParentVersion  int64  `json:"parent_version"`
	Kind           string `json:"kind"`
}

// LineageRow tracks one observed input snapshot for replacement detection.
type LineageRow struct {
	Kind         string `json:"kind"`
	ContentHash  string `json:"content_hash"`
	Seq          int64  `json:"seq"`
	Relation     string `json:"relation"`
	HeadID       string `json:"head_id,omitempty"`
	FirstVersion int64  `json:"first_version"`
}

// Store wraps the database handle and the lock-file path.
type Store struct {
	db       *sql.DB
	lockPath string
}

// Open creates/opens the SQLite database and applies the schema.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "headprune.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, lockPath: filepath.Join(dir, "headprune.db.lock")}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS versions (
			version INTEGER PRIMARY KEY,
			status TEXT NOT NULL CHECK(status IN ('active','rolled_back','archived')),
			kind TEXT NOT NULL CHECK(kind IN ('commit','rollback')),
			head_fingerprint TEXT NOT NULL,
			probe_digest TEXT NOT NULL,
			quant_fingerprint TEXT NOT NULL,
			decision_digest TEXT NOT NULL,
			input_digest TEXT NOT NULL,
			decision_json TEXT NOT NULL,
			parent_version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS rollback_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			from_version INTEGER NOT NULL,
			target_version INTEGER NOT NULL,
			new_version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS lineage (
			kind TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			seq INTEGER NOT NULL,
			relation TEXT NOT NULL,
			head_id TEXT NOT NULL DEFAULT '',
			first_version INTEGER NOT NULL,
			PRIMARY KEY(kind, content_hash)
		)`,
		`CREATE TABLE IF NOT EXISTS head_error_cache (
			input_digest TEXT NOT NULL,
			head_id TEXT NOT NULL,
			row_json TEXT NOT NULL,
			source_version INTEGER NOT NULL,
			PRIMARY KEY(input_digest, head_id)
		)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// CurrentVersion returns the highest version and the active row count.
func (s *Store) CurrentVersion() (int64, error) {
	var v sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(version) FROM versions`).Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return v.Int64, nil
}

// ActiveVersion returns the latest version whose status is active.
func (s *Store) ActiveVersion() (int64, error) {
	var v sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(version) FROM versions WHERE status='active'`).Scan(&v)
	if err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return v.Int64, nil
}

// GetVersion reads one version; rolled-back rows remain queryable.
func (s *Store) GetVersion(v int64) (VersionRow, error) {
	row := s.db.QueryRow(`SELECT version,status,kind,head_fingerprint,probe_digest,
		quant_fingerprint,decision_digest,input_digest,decision_json,parent_version
		FROM versions WHERE version=?`, v)
	var vr VersionRow
	err := row.Scan(&vr.Version, &vr.Status, &vr.Kind, &vr.HeadFP, &vr.ProbeDigest,
		&vr.QuantFP, &vr.DecisionDigest, &vr.InputDigest, &vr.DecisionJSON, &vr.ParentVersion)
	if err == sql.ErrNoRows {
		return VersionRow{}, &model.BizError{Code: model.CodeStaleVersion,
			Message: fmt.Sprintf("version %d does not exist", v), CurrentVer: mustCurrent(s)}
	}
	return vr, err
}

func mustCurrent(s *Store) int64 {
	v, _ := s.CurrentVersion()
	return v
}

// History returns every version, newest first.
func (s *Store) History() ([]VersionRow, error) {
	rows, err := s.db.Query(`SELECT version,status,kind,head_fingerprint,probe_digest,
		quant_fingerprint,decision_digest,input_digest,decision_json,parent_version
		FROM versions ORDER BY version DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VersionRow
	for rows.Next() {
		var vr VersionRow
		if err := rows.Scan(&vr.Version, &vr.Status, &vr.Kind, &vr.HeadFP, &vr.ProbeDigest,
			&vr.QuantFP, &vr.DecisionDigest, &vr.InputDigest, &vr.DecisionJSON, &vr.ParentVersion); err != nil {
			return nil, err
		}
		out = append(out, vr)
	}
	return out, rows.Err()
}

// CommitInput carries the persisted artifacts of one decision.
type CommitInput struct {
	HeadFP         string
	ProbeDigest    string
	QuantFP        string
	DecisionDigest string
	InputDigest    string
	DecisionJSON   []byte
	ExpectedNext   int64
}

// CommitVersion conditionally inserts the next monotonic version under a
// transaction and OS file lock. Stale or duplicate versions are rejected.
func (s *Store) CommitVersion(in CommitInput) (VersionRow, error) {
	lock, err := filelock.Acquire(s.lockPath)
	if err != nil {
		return VersionRow{}, fmt.Errorf("acquire file lock: %w", err)
	}
	defer lock.Release()
	tx, err := s.db.Begin()
	if err != nil {
		return VersionRow{}, err
	}
	defer tx.Rollback()

	current, err := queryMax(tx)
	if err != nil {
		return VersionRow{}, err
	}
	if in.ExpectedNext != current+1 {
		return VersionRow{}, staleErr(current,
			fmt.Sprintf("expected next version %d but database is at %d", in.ExpectedNext, current))
	}
	parent := current
	res, err := tx.Exec(`INSERT INTO versions
		(version,status,kind,head_fingerprint,probe_digest,quant_fingerprint,
		 decision_digest,input_digest,decision_json,parent_version)
		VALUES (?, 'active','commit', ?,?,?,?,?,?,?)`,
		in.ExpectedNext, in.HeadFP, in.ProbeDigest, in.QuantFP,
		in.DecisionDigest, in.InputDigest, string(in.DecisionJSON), parent)
	if err != nil {
		return VersionRow{}, &model.BizError{Code: model.CodeStaleVersion,
			Message: "duplicate version rejected by primary key", CurrentVer: current}
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return VersionRow{}, staleErr(current, "conditional insert affected no rows")
	}
	if err := tx.Commit(); err != nil {
		return VersionRow{}, err
	}
	return s.GetVersion(in.ExpectedNext)
}

// Rollback restores an earlier version by appending a new commit carrying the
// target's pruning set and decision basis. Versions between target and the
// current tip become rolled_back; rows older than keepGenerations generations
// are marked archived but remain queryable, honoring the retention policy.
func (s *Store) Rollback(target int64, keepGenerations int) (VersionRow, error) {
	lock, err := filelock.Acquire(s.lockPath)
	if err != nil {
		return VersionRow{}, fmt.Errorf("acquire file lock: %w", err)
	}
	defer lock.Release()
	tx, err := s.db.Begin()
	if err != nil {
		return VersionRow{}, err
	}
	defer tx.Rollback()

	current, err := queryMax(tx)
	if err != nil {
		return VersionRow{}, err
	}
	var targetRow VersionRow
	row := tx.QueryRow(`SELECT version,status,kind,head_fingerprint,probe_digest,
		quant_fingerprint,decision_digest,input_digest,decision_json,parent_version
		FROM versions WHERE version=? AND status='active'`, target)
	err = row.Scan(&targetRow.Version, &targetRow.Status, &targetRow.Kind, &targetRow.HeadFP,
		&targetRow.ProbeDigest, &targetRow.QuantFP, &targetRow.DecisionDigest,
		&targetRow.InputDigest, &targetRow.DecisionJSON, &targetRow.ParentVersion)
	if err == sql.ErrNoRows {
		return VersionRow{}, staleErr(current, fmt.Sprintf("rollback target %d missing or already rolled back", target))
	}
	if err != nil {
		return VersionRow{}, err
	}
	next := current + 1
	if _, err := tx.Exec(`UPDATE versions SET status='rolled_back' WHERE version>?`, target); err != nil {
		return VersionRow{}, err
	}
	if _, err := tx.Exec(`INSERT INTO versions
		(version,status,kind,head_fingerprint,probe_digest,quant_fingerprint,
		 decision_digest,input_digest,decision_json,parent_version)
		VALUES (?, 'active','rollback', ?,?,?,?,?,?,?)`,
		next, targetRow.HeadFP, targetRow.ProbeDigest, targetRow.QuantFP,
		targetRow.DecisionDigest, targetRow.InputDigest, targetRow.DecisionJSON, target); err != nil {
		return VersionRow{}, &model.BizError{Code: model.CodeStaleVersion,
			Message: "rollback insert conflict", CurrentVer: current}
	}
	if _, err := tx.Exec(`INSERT INTO rollback_events(from_version,target_version,new_version)
		VALUES (?,?,?)`, current, target, next); err != nil {
		return VersionRow{}, err
	}
	if keepGenerations > 0 {
		cutoff := next - int64(keepGenerations)
		if cutoff > 0 {
			if _, err := tx.Exec(`UPDATE versions SET status='archived'
				WHERE version<=? AND version<>?`, cutoff, target); err != nil {
				return VersionRow{}, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return VersionRow{}, err
	}
	return s.GetVersion(next)
}

// VerifyDigest checks a stored version against an externally supplied digest
// and returns E_STALE_VERSION on mismatch.
func (s *Store) VerifyDigest(v int64, decisionDigest string) error {
	vr, err := s.GetVersion(v)
	if err != nil {
		return err
	}
	if vr.DecisionDigest != decisionDigest {
		return &model.BizError{Code: model.CodeStaleVersion,
			Message: fmt.Sprintf("decision digest mismatch for version %d", v), CurrentVer: vr.Version}
	}
	return nil
}

func queryMax(tx *sql.Tx) (int64, error) {
	var v sql.NullInt64
	if err := tx.QueryRow(`SELECT MAX(version) FROM versions`).Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return v.Int64, nil
}

func staleErr(current int64, msg string) *model.BizError {
	return &model.BizError{Code: model.CodeStaleVersion, Message: msg, CurrentVer: current}
}

// Lineage helpers -----------------------------------------------------------

// UpsertLineage records one observed input hash with its detected relation.
func (s *Store) UpsertLineage(kind, hash string, seq int64, relation, headID string, version int64) error {
	_, err := s.db.Exec(`INSERT INTO lineage(kind,content_hash,seq,relation,head_id,first_version)
		VALUES (?,?,?,?,?,?)
		ON CONFLICT(kind,content_hash) DO UPDATE SET relation=excluded.relation, seq=excluded.seq`,
		kind, hash, seq, relation, headID, version)
	return err
}

// Lineage returns all known input hashes of one kind.
func (s *Store) Lineage(kind string) ([]LineageRow, error) {
	rows, err := s.db.Query(`SELECT kind,content_hash,seq,relation,head_id,first_version
		FROM lineage WHERE kind=? ORDER BY seq`, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LineageRow
	for rows.Next() {
		var r LineageRow
		if err := rows.Scan(&r.Kind, &r.ContentHash, &r.Seq, &r.Relation, &r.HeadID, &r.FirstVersion); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CacheHeadErrors writes per-head error rows. Each map key is the composite
// "<cache-key>|<head-id>"; the cache key identifies the head/sentence/policy
// content while head-id is the stable row identifier.
func (s *Store) CacheHeadErrors(_ string, version int64, rowsJSON map[string][]byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	keys := make([]string, 0, len(rowsJSON))
	for k := range rowsJSON {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sep := strings.LastIndex(k, "|")
		cacheKey, headID := k[:sep], k[sep+1:]
		if _, err := tx.Exec(`INSERT INTO head_error_cache(input_digest,head_id,row_json,source_version)
			VALUES (?,?,?,?) ON CONFLICT(input_digest,head_id) DO UPDATE SET
			row_json=excluded.row_json, source_version=excluded.source_version`,
			cacheKey, headID, string(rowsJSON[k]), version); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CachedHeadError reads one cached row and its source version.
func (s *Store) CachedHeadError(inputDigest, headID string) ([]byte, int64, bool, error) {
	var raw string
	var ver int64
	err := s.db.QueryRow(`SELECT row_json,source_version FROM head_error_cache
		WHERE input_digest=? AND head_id=?`, inputDigest, headID).Scan(&raw, &ver)
	if err == sql.ErrNoRows {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	return []byte(raw), ver, true, nil
}

// RetagCacheVersion moves the cached rows of one input digest onto the
// committed version so reuse can cite the exact source version.
func (s *Store) RetagCacheVersion(inputDigest string, version int64) error {
	_, err := s.db.Exec(`UPDATE head_error_cache SET source_version=? WHERE input_digest=?`,
		version, inputDigest)
	return err
}

// MarshalVersionRow is a small helper used by the transport layer.
func MarshalVersionRow(vr VersionRow) []byte {
	b, _ := json.Marshal(vr)
	return b
}
