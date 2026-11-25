// Package store provides transactional SQLite stage checkpoints plus a real
// cross-process file lock for concurrent publishers.
package store

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"
)

// Checkpoint is one valid stage record for a material.
type Checkpoint struct {
	MaterialID string
	Stage      string
	Seq        int
	ConfigVer  int
	RecipeFP   string
	ArtifactFP string
	Payload    []byte
}

// Store wraps the SQLite database and the adjacent lock file.
type Store struct {
	db       *sql.DB
	lockFile *os.File
	mu       sync.Mutex
}

// Open opens (and migrates) the workspace database and acquires the file lock
// lazily through Lock.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "matbench.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	lf, err := os.OpenFile(filepath.Join(dir, "matbench.lock"),
		os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, lockFile: lf}, nil
}

// Lock acquires the real cross-process exclusive file lock.
func (s *Store) Lock() error   { return platformLock(s.lockFile.Fd()) }
func (s *Store) Unlock() error { return platformUnlock(s.lockFile.Fd()) }

// Close releases the lock file and database.
func (s *Store) Close() error {
	s.Unlock()
	s.lockFile.Close()
	return s.db.Close()
}

func migrate(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS checkpoints(
			material_id TEXT NOT NULL,
			stage       TEXT NOT NULL,
			seq         INTEGER NOT NULL,
			config_ver  INTEGER NOT NULL,
			recipe_fp   TEXT NOT NULL,
			artifact_fp TEXT NOT NULL,
			payload     BLOB NOT NULL,
			active      INTEGER NOT NULL DEFAULT 1,
			updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY(material_id, stage, seq)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active
			ON checkpoints(material_id, stage) WHERE active = 1`,
		`CREATE TABLE IF NOT EXISTS invalidations(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			material_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			reason TEXT NOT NULL,
			old_fp TEXT NOT NULL,
			new_fp TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		)`,
		`CREATE TABLE IF NOT EXISTS run_meta(
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, st := range stmts {
		if _, err := db.Exec(st); err != nil {
			return err
		}
	}
	return nil
}

// PublishOutcome classifies a conditional publish attempt.
type PublishOutcome string

const (
	PublishAccepted  PublishOutcome = "accepted"
	PublishStale     PublishOutcome = "stale"
	PushbackRollback PublishOutcome = "rollback"
	PushbackTampered PublishOutcome = "tampered"
)

// Publish conditionally inserts a new active checkpoint inside one transaction.
// Sequence numbers must advance by exactly one per material/stage; the prior
// active record is deactivated atomically. Whole-record replacement is
// classified as rollback (seq regression), tamper (fp/digest mismatch at the
// same seq) or normal advance.
func (s *Store) Publish(c Checkpoint) (PublishOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var curSeq int
	var curFP, curDigest string
	row := tx.QueryRow(`SELECT seq, recipe_fp, artifact_fp FROM checkpoints
		WHERE material_id=? AND stage=? AND active=1`, c.MaterialID, c.Stage)
	err = row.Scan(&curSeq, &curFP, &curDigest)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if c.Seq != 1 {
			return PublishStale, nil
		}
	case err != nil:
		return "", err
	default:
		switch {
		case c.Seq <= curSeq:
			if c.Seq == curSeq && (c.RecipeFP != curFP || c.ArtifactFP != curDigest) {
				return PushbackTampered, nil
			}
			return PushbackRollback, nil
		case c.Seq != curSeq+1:
			return PublishStale, nil
		}
	}

	if _, err := tx.Exec(`UPDATE checkpoints SET active=0 WHERE material_id=? AND stage=?`,
		c.MaterialID, c.Stage); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`INSERT INTO checkpoints
		(material_id, stage, seq, config_ver, recipe_fp, artifact_fp, payload, active)
		VALUES (?,?,?,?,?,?,?,1)`,
		c.MaterialID, c.Stage, c.Seq, c.ConfigVer, c.RecipeFP, c.ArtifactFP, c.Payload); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return PublishAccepted, nil
}

// LogInvalidation records an invalidation trail entry.
func (s *Store) LogInvalidation(materialID, stage, reason, oldFP, newFP string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT INTO invalidations
		(material_id, stage, reason, old_fp, new_fp) VALUES (?,?,?,?,?)`,
		materialID, stage, reason, oldFP, newFP)
	return err
}

// Active returns the active checkpoint for a material/stage, or nil.
func (s *Store) Active(materialID, stage string) (*Checkpoint, error) {
	row := s.db.QueryRow(`SELECT material_id, stage, seq, config_ver, recipe_fp,
		artifact_fp, payload FROM checkpoints WHERE material_id=? AND stage=? AND active=1`,
		materialID, stage)
	var c Checkpoint
	if err := row.Scan(&c.MaterialID, &c.Stage, &c.Seq, &c.ConfigVer, &c.RecipeFP,
		&c.ArtifactFP, &c.Payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

// AllActive returns all active checkpoints ordered deterministically.
func (s *Store) AllActive() ([]Checkpoint, error) {
	rows, err := s.db.Query(`SELECT material_id, stage, seq, config_ver, recipe_fp,
		artifact_fp, payload FROM checkpoints WHERE active=1
		ORDER BY material_id, stage`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var c Checkpoint
		if err := rows.Scan(&c.MaterialID, &c.Stage, &c.Seq, &c.ConfigVer,
			&c.RecipeFP, &c.ArtifactFP, &c.Payload); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Invalidations returns the recorded invalidation trail.
func (s *Store) Invalidations(materialID string) ([]map[string]any, error) {
	q := `SELECT stage, reason, old_fp, new_fp, created_at FROM invalidations
		WHERE material_id=? ORDER BY id`
	rows, err := s.db.Query(q, materialID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var stage, reason, oldFP, newFP, at string
		if err := rows.Scan(&stage, &reason, &oldFP, &newFP, &at); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"stage": stage, "reason": reason, "old_fp": oldFP,
			"new_fp": newFP, "at": at,
		})
	}
	return out, rows.Err()
}

// MetaGet / MetaSet maintain run metadata such as config version.
func (s *Store) MetaGet(key string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM run_meta WHERE key=?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) MetaSet(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT INTO run_meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

// ReplacePayloadForTest overwrites an active checkpoint payload in place to
// simulate whole-record tampering during tests.
func (s *Store) ReplacePayloadForTest(materialID, stage string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(`UPDATE checkpoints SET payload=?
		WHERE material_id=? AND stage=? AND active=1`, payload, materialID, stage)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNotFound
	}
	return nil
}

// ErrNotFound indicates a requested record does not exist.
var ErrNotFound = errNotFound{}

type errNotFound struct{}

func (errNotFound) Error() string { return "store: record not found" }
