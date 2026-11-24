// Package snapshot persists adjudicated path results as versioned snapshots
// in a pure-Go SQLite database. Each batch commit is a single transaction
// with a strictly increasing version number; loads validate version-chain
// contiguity and roll back to the most recent contiguous version on gaps.
// Content comparison distinguishes whole-replacement versions from mere
// version-number bumps.
package snapshot

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"

	"pathcourt/internal/bizcode"
	"pathcourt/internal/reparse"
	"pathcourt/internal/resolver"
)

// FixedSampleMarker is the only timestamp-like value ever stored or
// exported, keeping reports deterministic.
const FixedSampleMarker = "SAMPLE-T0"

// ErrVersionGap is returned when the stored version chain has a hole.
var ErrVersionGap = errors.New("snapshot version chain gap detected")

// Row is one persisted path verdict.
type Row struct {
	ManifestID string           `json:"manifest_id"`
	PathIndex  int              `json:"path_index"`
	Input      string           `json:"input"`
	Result     *resolver.Result `json:"result"`
}

// Version is a loaded snapshot version.
type Version struct {
	Version     int64  `json:"version"`
	RootID      string `json:"root_id"`
	RootPath    string `json:"root_path"`
	ManifestID  string `json:"manifest_id"`
	ContentHash string `json:"content_hash"`
	Rows        []Row  `json:"rows"`
	SampleMark  string `json:"sample_mark"`
}

// ChangeKind classifies the difference between two adjacent versions.
type ChangeKind string

const (
	ChangeIdentical        ChangeKind = "identical"
	ChangeIncremental      ChangeKind = "incremental"
	ChangeWholeReplacement ChangeKind = "whole-replacement"
)

// Diff compares two versions by content rather than version number.
type Diff struct {
	From       int64      `json:"from"`
	To         int64      `json:"to"`
	Kind       ChangeKind `json:"kind"`
	Changed    int        `json:"changed"`
	SharedKeys int        `json:"shared_keys"`
	FromKeys   int        `json:"from_keys"`
	ToKeys     int        `json:"to_keys"`
}

// Store is the SQLite-backed snapshot store.
type Store struct {
	db *sql.DB

	mu          sync.Mutex
	nextVersion int64
	inflight    atomic.Int64
}

// Open opens (and migrates) a snapshot database at dsn.
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS snapshot_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			root_id TEXT NOT NULL,
			root_path TEXT NOT NULL,
			manifest_id TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			sample_mark TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshot_rows (
			version INTEGER NOT NULL,
			manifest_id TEXT NOT NULL,
			path_index INTEGER NOT NULL,
			input TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (version, manifest_id, path_index)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_snapshot_rows_version ON snapshot_rows(version)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	var max sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM snapshots`).Scan(&max); err != nil {
		return err
	}
	if max.Valid {
		s.nextVersion = max.Int64 + 1
	} else {
		s.nextVersion = 1
	}
	return nil
}

// Inflight returns the observable in-flight resolution count.
func (s *Store) Inflight() int64 { return s.inflight.Load() }

// EnterInflight increments the observable in-flight resolution count.
func (s *Store) EnterInflight() { s.inflight.Add(1) }

// LeaveInflight decrements the observable in-flight resolution count.
func (s *Store) LeaveInflight() { s.inflight.Add(-1) }

// RootID derives a stable, workspace-independent identifier for a
// restricted root. It hashes the root's base name (e.g. "restricted-root")
// rather than its absolute location so identical inputs in different
// temporary directories produce the same identifier and report bytes.
func RootID(rootPath string) string {
	base := rootPath
	if idx := strings.LastIndexAny(rootPath, `/\`); idx >= 0 && idx+1 < len(rootPath) {
		base = rootPath[idx+1:]
	}
	sum := sha256.Sum256([]byte(base))
	return "root-" + hex.EncodeToString(sum[:8])
}

// stableRow is the location-independent projection used for content hashing.
type stableRow struct {
	ManifestID string         `json:"manifest_id"`
	PathIndex  int            `json:"path_index"`
	Input      string         `json:"input"`
	Code       bizcode.Code   `json:"code"`
	FailIndex  int            `json:"fail_index"`
	Hops       []stableHop    `json:"hops"`
	StepKinds  []reparse.Kind `json:"step_kinds"`
	StepComps  []string       `json:"step_components"`
}

type stableHop struct {
	Seq       int          `json:"seq"`
	Kind      reparse.Kind `json:"kind"`
	RawTarget string       `json:"raw_target"`
	Relative  bool         `json:"relative"`
}

// stableTarget strips host-specific absolute path prefixes from a link
// target so the content hash is independent of the temporary directory.
// Relative targets are returned unchanged; absolute targets keep only the
// workspace-anchored logical tail (restricted-root/... or outside-root/...).
func stableTarget(raw string) string {
	if raw == "" {
		return raw
	}
	bs := strings.ReplaceAll(raw, "/", `\`)
	anchors := []string{"restricted-root", "outside-root"}
	earliest := -1
	for _, anchor := range anchors {
		idx := indexComponent(bs, anchor)
		if idx >= 0 && (earliest < 0 || idx < earliest) {
			earliest = idx
		}
	}
	if earliest >= 0 {
		return bs[earliest:]
	}
	return raw
}

func hashRows(rows []Row) (string, error) {
	projection := make([]stableRow, 0, len(rows))
	for _, row := range rows {
		sr := stableRow{ManifestID: row.ManifestID, PathIndex: row.PathIndex, Input: row.Input}
		if row.Result != nil {
			sr.Code = row.Result.Code
			sr.FailIndex = row.Result.FailIndex
			for _, h := range row.Result.Hops {
				sr.Hops = append(sr.Hops, stableHop{Seq: h.Seq, Kind: h.Kind, RawTarget: stableTarget(h.RawTarget), Relative: h.Relative})
			}
			for _, st := range row.Result.Steps {
				sr.StepKinds = append(sr.StepKinds, st.Kind)
				sr.StepComps = append(sr.StepComps, st.Component)
			}
		}
		projection = append(projection, sr)
	}
	canonical, err := json.Marshal(projection)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// Commit persists one manifest batch as the next contiguous snapshot inside
// a single transaction.
func (s *Store) Commit(ctx context.Context, rootPath, manifestID string, rows []Row) (*Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(ctx, s.nextVersion, rootPath, manifestID, rows)
}

// CommitAtVersion forces an explicit version number. It exists solely for
// the snapshot-gap fault injection (external parameter driven) and returns
// an error when the number is not strictly greater than the current head.
func (s *Store) CommitAtVersion(ctx context.Context, version int64, rootPath, manifestID string, rows []Row) (*Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version < s.nextVersion {
		return nil, fmt.Errorf("forced version %d is not greater than head %d", version, s.nextVersion)
	}
	return s.commitLocked(ctx, version, rootPath, manifestID, rows)
}

func (s *Store) commitLocked(ctx context.Context, version int64, rootPath, manifestID string, rows []Row) (*Version, error) {
	hash, err := hashRows(rows)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(version, root_id, root_path, manifest_id, content_hash, sample_mark, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		version, RootID(rootPath), rootPath, manifestID, hash, FixedSampleMarker, FixedSampleMarker); err != nil {
		return nil, err
	}
	for _, row := range rows {
		payload, err := json.Marshal(row.Result)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO snapshot_rows(version, manifest_id, path_index, input, payload)
			 VALUES (?, ?, ?, ?, ?)`,
			version, row.ManifestID, row.PathIndex, row.Input, string(payload)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.nextVersion = version + 1
	return &Version{
		Version:     version,
		RootID:      RootID(rootPath),
		RootPath:    rootPath,
		ManifestID:  manifestID,
		ContentHash: hash,
		Rows:        rows,
		SampleMark:  FixedSampleMarker,
	}, nil
}

// LoadChain loads every snapshot from version 1 while the chain stays
// contiguous. A gap returns ErrVersionGap together with the contiguous
// prefix; callers keep the prefix (rollback to the most recent contiguous
// version).
func (s *Store) LoadChain(ctx context.Context) ([]Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rs, err := s.db.QueryContext(ctx,
		`SELECT version, root_id, root_path, manifest_id, content_hash, sample_mark
		 FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	type meta struct {
		v       Version
		gapHere bool
	}
	var metas []meta
	var expected int64 = 1
	var gapAt int64 = -1
	for rs.Next() {
		var v Version
		if err := rs.Scan(&v.Version, &v.RootID, &v.RootPath, &v.ManifestID, &v.ContentHash, &v.SampleMark); err != nil {
			rs.Close()
			return nil, err
		}
		gapHere := false
		if gapAt < 0 && v.Version != expected {
			gapAt = v.Version
			gapHere = true
		}
		metas = append(metas, meta{v: v, gapHere: gapHere})
		expected = v.Version + 1
	}
	if err := rs.Err(); err != nil {
		rs.Close()
		return nil, err
	}
	if err := rs.Close(); err != nil {
		return nil, err
	}

	var versions []Version
	for _, m := range metas {
		if m.gapHere {
			break
		}
		detail, err := s.loadRowsLocked(ctx, m.v.Version)
		if err != nil {
			return nil, err
		}
		m.v.Rows = detail
		versions = append(versions, m.v)
	}
	if gapAt > 0 {
		return versions, fmt.Errorf("%w: expected %d", ErrVersionGap, gapAt)
	}
	return versions, nil
}

func (s *Store) loadRowsLocked(ctx context.Context, version int64) ([]Row, error) {
	rs, err := s.db.QueryContext(ctx,
		`SELECT manifest_id, path_index, input, payload FROM snapshot_rows
		 WHERE version = ? ORDER BY manifest_id ASC, path_index ASC`, version)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []Row
	for rs.Next() {
		var row Row
		var payload string
		if err := rs.Scan(&row.ManifestID, &row.PathIndex, &row.Input, &payload); err != nil {
			return nil, err
		}
		var result resolver.Result
		if err := json.Unmarshal([]byte(payload), &result); err != nil {
			return nil, err
		}
		resultCopy := result
		row.Result = &resultCopy
		out = append(out, row)
	}
	return out, rs.Err()
}

// Compare classifies the change between two adjacent versions based on the
// row key sets and verdicts, never on the version number alone.
func Compare(from, to Version) Diff {
	fromMap := rowMap(from.Rows)
	toMap := rowMap(to.Rows)
	diff := Diff{From: from.Version, To: to.Version, FromKeys: len(fromMap), ToKeys: len(toMap)}
	for k, r := range toMap {
		if l, ok := fromMap[k]; ok {
			diff.SharedKeys++
			if l.Result.Code != r.Result.Code || l.Result.Normalized != r.Result.Normalized {
				diff.Changed++
			}
		}
	}
	switch {
	case from.ContentHash == to.ContentHash:
		diff.Kind = ChangeIdentical
	case diff.SharedKeys == 0:
		diff.Kind = ChangeWholeReplacement
	default:
		diff.Kind = ChangeIncremental
	}
	return diff
}

func rowMap(rows []Row) map[string]Row {
	out := make(map[string]Row, len(rows))
	for _, r := range rows {
		out[fmt.Sprintf("%s#%d", r.ManifestID, r.PathIndex)] = r
	}
	return out
}

// BudgetError marks a manifest that exceeded the per-session path budget.
type BudgetError struct {
	Limit int
	Count int
}

func (e *BudgetError) Error() string {
	return fmt.Sprintf("path budget exceeded: %d paths > limit %d (%s)", e.Count, e.Limit, bizcode.E_PATH_BUDGET)
}

// Code returns the business code carried by this error.
func (e *BudgetError) Code() bizcode.Code { return bizcode.E_PATH_BUDGET }

// indexComponent returns the start index of a full path component named name
// within the back-slash separated path p, or -1 when absent.
func indexComponent(p, name string) int {
	idx := 0
	for {
		j := strings.Index(p[idx:], name)
		if j < 0 {
			return -1
		}
		start := idx + j
		end := start + len(name)
		leftOK := start == 0 || p[start-1] == '\\'
		rightOK := end == len(p) || p[end] == '\\'
		if leftOK && rightOK {
			return start
		}
		idx = end
	}
}
