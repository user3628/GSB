// Package store provides transactional SQLite persistence of per-layer
// calibration snapshots, pipeline versions and crash-recovery state. The driver
// is pure Go (modernc.org/sqlite) so no CGO toolchain is required.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"

	"qaudit/internal/model"
)

// Store is the snapshot database handle.
type Store struct {
	db       *sql.DB
	lockPath string
	mu       sync.Mutex
}

// Open opens (creating if needed) the snapshot database and applies the schema.
func Open(dbPath string) (*Store, error) {
	if dir := filepath.Dir(dbPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := "file:" + dbPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, lockPath: dbPath + ".lock"}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS snapshots(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			version INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			layer_name TEXT NOT NULL,
			input_fingerprint TEXT NOT NULL,
			policy_fingerprint TEXT NOT NULL,
			artifact_digest TEXT NOT NULL,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			UNIQUE(version, layer_name)
		)`,
		`CREATE TABLE IF NOT EXISTS versions(
			version INTEGER PRIMARY KEY,
			fingerprint TEXT NOT NULL,
			kind TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_state(
			id INTEGER PRIMARY KEY CHECK (id = 1),
			version INTEGER NOT NULL,
			fingerprint TEXT NOT NULL,
			stage INTEGER NOT NULL,
			updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_snap_version ON snapshots(version)`,
	}
	for _, st := range stmts {
		if _, err := s.db.Exec(st); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// SaveSnapshot inserts one layer snapshot inside a single transaction. The
// monotonic sequence is derived from existing rows and the unique constraint
// guarantees one valid record per layer per version.
func (s *Store) SaveSnapshot(version int64, layerName, inputFP, policyFP, digest string, artifact *model.LayerCalibration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, err := json.Marshal(artifact)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var maxSeq sql.NullInt64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(seq),0) FROM snapshots WHERE version=?`, version).Scan(&maxSeq); err != nil {
		return err
	}
	nextSeq := maxSeq.Int64 + 1
	if _, err := tx.Exec(
		`INSERT INTO snapshots(version, seq, layer_name, input_fingerprint, policy_fingerprint, artifact_digest, payload)
		 VALUES(?,?,?,?,?,?,?)`,
		version, nextSeq, layerName, inputFP, policyFP, digest, string(payload),
	); err != nil {
		return model.NewBizError(model.CodeStaleVersion, "snapshot insert rejected (duplicate layer/version)",
			map[string]string{"layer": layerName, "version": fmt.Sprintf("%d", version), "detail": err.Error()})
	}
	return tx.Commit()
}

// SnapshotRow is a stored snapshot without the full payload.
type SnapshotRow struct {
	Version           int64
	Seq               int64
	LayerName         string
	InputFingerprint  string
	PolicyFingerprint string
	ArtifactDigest    string
}

// ListSnapshots returns every committed snapshot of a version in sequence order.
func (s *Store) ListSnapshots(version int64) ([]SnapshotRow, error) {
	rows, err := s.db.Query(
		`SELECT version, seq, layer_name, input_fingerprint, policy_fingerprint, artifact_digest
		 FROM snapshots WHERE version=? ORDER BY seq`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotRow
	for rows.Next() {
		var r SnapshotRow
		if err := rows.Scan(&r.Version, &r.Seq, &r.LayerName, &r.InputFingerprint, &r.PolicyFingerprint, &r.ArtifactDigest); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LoadSnapshot returns the full calibration artifact of one layer/version.
func (s *Store) LoadSnapshot(version int64, layerName string) (*model.LayerCalibration, error) {
	var payload string
	err := s.db.QueryRow(`SELECT payload FROM snapshots WHERE version=? AND layer_name=?`, version, layerName).Scan(&payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var art model.LayerCalibration
	if err := json.Unmarshal([]byte(payload), &art); err != nil {
		return nil, err
	}
	return &art, nil
}

// SaveState upserts the pipeline progress atomically.
func (s *Store) SaveState(version int64, fingerprint string, stage int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO pipeline_state(id, version, fingerprint, stage) VALUES(1,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET version=excluded.version, fingerprint=excluded.fingerprint,
		 stage=excluded.stage, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')`,
		version, fingerprint, stage); err != nil {
		return err
	}
	return tx.Commit()
}

// StateRow is the recovered pipeline progress.
type StateRow struct {
	Version     int64
	Fingerprint string
	Stage       int
}

// LoadState returns the committed progress or nil when no run exists.
func (s *Store) LoadState() (*StateRow, error) {
	var r StateRow
	err := s.db.QueryRow(`SELECT version, fingerprint, stage FROM pipeline_state WHERE id=1`).Scan(&r.Version, &r.Fingerprint, &r.Stage)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// UpsertVersion records the classification of a pipeline version.
func (s *Store) UpsertVersion(version int64, fingerprint, kind string) error {
	_, err := s.db.Exec(
		`INSERT INTO versions(version, fingerprint, kind) VALUES(?,?,?)
		 ON CONFLICT(version) DO UPDATE SET fingerprint=excluded.fingerprint, kind=excluded.kind`,
		version, fingerprint, kind)
	return err
}

// VersionRow describes a recorded version.
type VersionRow struct {
	Version     int64
	Fingerprint string
	Kind        string
}

// ListVersions returns recorded versions ordered numerically.
func (s *Store) ListVersions() ([]VersionRow, error) {
	rows, err := s.db.Query(`SELECT version, fingerprint, kind FROM versions ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VersionRow
	for rows.Next() {
		var r VersionRow
		if err := rows.Scan(&r.Version, &r.Fingerprint, &r.Kind); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LatestVersion returns the highest recorded version number or zero.
func (s *Store) LatestVersion() (int64, error) {
	var v sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(version) FROM versions`).Scan(&v); err != nil {
		return 0, err
	}
	return v.Int64, nil
}

// lockPathForTest exposes the companion lock file path for tests.
func (s *Store) lockPathForTest() string { return s.lockPath }
