// Package store persists batch progress and versioned adjudication snapshots
// in SQLite using the pure-Go modernc.org/sqlite driver.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	_ "modernc.org/sqlite"

	"injectctl/internal/model"
	"injectctl/internal/session"
)

// BatchStatus values.
const (
	StatusSettled = "settled"
	StatusBudget  = "budget_terminated"
)

// Snapshot is one versioned adjudication snapshot.
type Snapshot struct {
	Version        int                  `json:"version"`
	BatchID        string               `json:"batch_id"`
	ContentHash    string               `json:"content_hash"`
	SummaryHash    string               `json:"summary_hash"`
	RuleSetVersion string               `json:"rule_set_version"`
	ContextVersion string               `json:"context_version"`
	Verdicts       []model.Verdict      `json:"verdicts"`
	Corrections    []session.Correction `json:"corrections,omitempty"`
}

// ChainReport describes the validated snapshot version chain.
type ChainReport struct {
	LastContinuousVersion int        `json:"last_continuous_version"`
	Snapshots             []Snapshot `json:"snapshots"`
	ErrorCode             string     `json:"error_code,omitempty"`
}

// Store wraps a SQLite database handle. The mutex serializes every
// read-modify-write sequence so concurrent batch workers cannot interleave
// transactions (SQLite permits only one writer at a time).
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// Open opens (and initializes) the database file.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) init() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS batches (
			batch_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			sample_count INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS verdicts (
			batch_id TEXT NOT NULL,
			sample_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			payload_json TEXT NOT NULL,
			PRIMARY KEY (batch_id, sample_id)
		)`,
		`CREATE TABLE IF NOT EXISTS corrections (
			batch_id TEXT NOT NULL,
			sample_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			batch_id TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			summary_hash TEXT NOT NULL,
			rule_set_version TEXT NOT NULL,
			context_version TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
	}
	for _, ddl := range stmts {
		if _, err := s.db.Exec(ddl); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return nil
}

// SaveSettlement commits batch status, verdicts and corrections in one
// transaction. If the process is killed before commit, SQLite discards the
// whole transaction on the next open.
func (s *Store) SaveSettlement(batchID, status string, verdicts []model.Verdict, corrections []session.Correction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`INSERT INTO batches(batch_id, status, sample_count) VALUES(?,?,?)
		 ON CONFLICT(batch_id) DO UPDATE SET status=excluded.status, sample_count=excluded.sample_count`,
		batchID, status, len(verdicts),
	); err != nil {
		return err
	}
	for _, v := range verdicts {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO verdicts(batch_id, sample_id, seq, payload_json) VALUES(?,?,?,?)
			 ON CONFLICT(batch_id, sample_id) DO UPDATE SET seq=excluded.seq, payload_json=excluded.payload_json`,
			batchID, v.SampleID, v.Seq, string(raw),
		); err != nil {
			return err
		}
	}
	for _, c := range corrections {
		raw, err := json.Marshal(c)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO corrections(batch_id, sample_id, seq, payload_json) VALUES(?,?,?,?)`,
			batchID, c.SampleID, c.Seq, string(raw),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// BatchExists reports whether a batch was durably committed.
func (s *Store) BatchExists(batchID string) (bool, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var status string
	err := s.db.QueryRow(`SELECT status FROM batches WHERE batch_id=?`, batchID).Scan(&status)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, status, nil
}

// NextSnapshotVersion returns version = max(version)+1, starting at 1.
func (s *Store) NextSnapshotVersion() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var maxv sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(version) FROM snapshots`).Scan(&maxv); err != nil {
		return 0, err
	}
	return int(maxv.Int64) + 1, nil
}

// SaveSnapshotAtomic computes the running hash, the next version and inserts
// the snapshot all inside one transaction. Concurrent workers therefore
// cannot observe a stale chain (the store mutex additionally serializes
// read-modify-write sequences).
func (s *Store) SaveSnapshotAtomic(snap Snapshot) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(
		`SELECT content_hash, summary_hash FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return 0, err
	}
	runningHash := ""
	for rows.Next() {
		var content, summary string
		if err := rows.Scan(&content, &summary); err != nil {
			rows.Close()
			return 0, err
		}
		runningHash = ChainSummaryHash(runningHash, content)
	}
	rows.Close()
	snap.SummaryHash = runningHash

	var next int
	if err := tx.QueryRow(`SELECT COALESCE(MAX(version),0)+1 FROM snapshots`).Scan(&next); err != nil {
		return 0, err
	}
	snap.Version = next
	raw, err := json.Marshal(snap)
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(
		`INSERT INTO snapshots(version, batch_id, content_hash, summary_hash,
			rule_set_version, context_version, payload_json) VALUES(?,?,?,?,?,?,?)`,
		snap.Version, snap.BatchID, snap.ContentHash, snap.SummaryHash,
		snap.RuleSetVersion, snap.ContextVersion, string(raw),
	); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// SaveSnapshot inserts a pre-numbered snapshot inside its own transaction.
func (s *Store) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO snapshots(version, batch_id, content_hash, summary_hash,
			rule_set_version, context_version, payload_json) VALUES(?,?,?,?,?,?,?)`,
		snap.Version, snap.BatchID, snap.ContentHash, snap.SummaryHash,
		snap.RuleSetVersion, snap.ContextVersion, string(raw),
	); err != nil {
		return err
	}
	return tx.Commit()
}

// LoadChain loads every snapshot and validates a continuous version chain
// starting at 1 and hash chaining. A gap yields E_SNAPSHOT_VERSION and the
// caller receives the prefix up to the last continuous version.
func (s *Store) LoadChain() (ChainReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, err := s.db.Query(`SELECT payload_json FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return ChainReport{}, err
	}
	defer rows.Close()

	report := ChainReport{LastContinuousVersion: 0}
	var runningHash string
	expected := 1
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return report, err
		}
		var snap Snapshot
		if err := json.Unmarshal([]byte(raw), &snap); err != nil {
			return report, err
		}

		if snap.Version != expected {
			report.ErrorCode = model.CodeSnapshotVersion
			return report, &VersionError{
				Code:           model.CodeSnapshotVersion,
				Msg:            fmt.Sprintf("snapshot version gap: expected %d, found %d", expected, snap.Version),
				LastContinuous: report.LastContinuousVersion,
			}
		}
		if expected > 1 && snap.SummaryHash != runningHash {
			report.ErrorCode = model.CodeSnapshotVersion
			return report, &VersionError{
				Code:           model.CodeSnapshotVersion,
				Msg:            fmt.Sprintf("snapshot %d hash chain broken (whole-replacement detected)", snap.Version),
				LastContinuous: report.LastContinuousVersion,
			}
		}
		report.Snapshots = append(report.Snapshots, snap)
		report.LastContinuousVersion = snap.Version
		runningHash = ChainSummaryHash(runningHash, snap.ContentHash)
		expected++
	}
	return report, rows.Err()
}

// ForceGap simulates an externally induced version gap by deleting the
// snapshot row at version and re-sequencing later rows. It backs the
// E_SNAPSHOT_VERSION recovery verification scenario.
func (s *Store) ForceGap(removeVersion int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM snapshots WHERE version=?`, removeVersion); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE snapshots SET version=version-1 WHERE version>?`, removeVersion); err != nil {
		return err
	}
	return tx.Commit()
}

// LoadVerdicts returns the persisted verdicts of one batch, sorted by seq.
func (s *Store) LoadVerdicts(batchID string) ([]model.Verdict, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(
		`SELECT payload_json FROM verdicts WHERE batch_id=? ORDER BY seq ASC`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Verdict
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var v model.Verdict
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, rows.Err()
}

// AppendSnapshotForBatch adds a snapshot for an already-settled batch only
// when none exists, preserving an already committed version.
func (s *Store) AppendSnapshotForBatch(snap Snapshot) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var existing sql.NullInt64
	err := s.db.QueryRow(
		`SELECT version FROM snapshots WHERE batch_id=? ORDER BY version DESC LIMIT 1`,
		snap.BatchID).Scan(&existing)
	if err != nil && err != sql.ErrNoRows {
		return 0, false, err
	}
	if err == nil && existing.Valid {
		return int(existing.Int64), false, nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(
		`SELECT content_hash FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return 0, false, err
	}
	runningHash := ""
	for rows.Next() {
		var content string
		if err := rows.Scan(&content); err != nil {
			rows.Close()
			return 0, false, err
		}
		runningHash = ChainSummaryHash(runningHash, content)
	}
	rows.Close()
	snap.SummaryHash = runningHash

	var next int
	if err := tx.QueryRow(`SELECT COALESCE(MAX(version),0)+1 FROM snapshots`).Scan(&next); err != nil {
		return 0, false, err
	}
	snap.Version = next
	raw, err := json.Marshal(snap)
	if err != nil {
		return 0, false, err
	}
	if _, err := tx.Exec(
		`INSERT INTO snapshots(version, batch_id, content_hash, summary_hash,
			rule_set_version, context_version, payload_json) VALUES(?,?,?,?,?,?,?)`,
		snap.Version, snap.BatchID, snap.ContentHash, snap.SummaryHash,
		snap.RuleSetVersion, snap.ContextVersion, string(raw),
	); err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return next, true, nil
}

// SnapshotForBatch returns the snapshot previously committed for a batch.
func (s *Store) SnapshotForBatch(batchID string) (Snapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var raw string
	err := s.db.QueryRow(
		`SELECT payload_json FROM snapshots WHERE batch_id=? ORDER BY version DESC LIMIT 1`,
		batchID).Scan(&raw)
	if err == sql.ErrNoRows {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
}

// LoadCorrections returns persisted corrections for one batch, by seq.
func (s *Store) LoadCorrections(batchID string) ([]session.Correction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(
		`SELECT payload_json FROM corrections WHERE batch_id=? ORDER BY seq ASC`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []session.Correction
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var c session.Correction
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// VersionError carries the E_SNAPSHOT_VERSION code and rollback position.
type VersionError struct {
	Code           string
	Msg            string
	LastContinuous int
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("%s: %s (last continuous version=%d)", e.Code, e.Msg, e.LastContinuous)
}
