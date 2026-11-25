package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	DB       *sql.DB
	path     string
	lockPath string
	mu       sync.Mutex
}

type FileLock struct {
	f *os.File
}

func AcquireFileLock(path string) (*FileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("acquire file lock %s: %w", path, err)
	}
	return &FileLock{f: f}, nil
}

func (l *FileLock) Unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}

func Open(path string) (*Store, error) {
	if path == "" {
		path = "tm-sync.db"
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := fmt.Sprintf("file:%s?_busy_timeout=10000&_foreign_keys=on", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{DB: db, path: path, lockPath: path + ".lock"}
	schemaLock, err := AcquireFileLock(s.lockPath)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = schemaLock.Unlock()
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=10000; PRAGMA foreign_keys=ON;`); err != nil {
		_ = schemaLock.Unlock()
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(Schema); err != nil {
		_ = schemaLock.Unlock()
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	_ = schemaLock.Unlock()
	return s, nil
}

func (s *Store) Close() error {
	return s.DB.Close()
}

func (s *Store) WithTx(fn func(tx *sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := AcquireFileLock(s.lockPath)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Unlock() }()

	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func GetMeta(tx *sql.Tx, key string) (string, error) {
	var value string
	err := tx.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func SetMeta(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func NextMonotonicSeq(tx *sql.Tx, key string) (int64, error) {
	if _, err := tx.Exec(`INSERT INTO meta(key,value) VALUES(?, '0') ON CONFLICT(key) DO NOTHING`, key); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key=?`, key); err != nil {
		return 0, err
	}
	var value string
	if err := tx.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&value); err != nil {
		return 0, err
	}
	var seq int64
	if _, err := fmt.Sscanf(value, "%d", &seq); err != nil {
		return 0, err
	}
	return seq, nil
}

const Schema = `
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(key,value) VALUES
 ('effective_version','0'),
 ('source_digest',''),
 ('version_fingerprint',''),
 ('checkpoint_seq','0');

CREATE TABLE IF NOT EXISTS version_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	stream TEXT NOT NULL,
	version INTEGER NOT NULL,
	content_digest TEXT NOT NULL,
	previous_digest TEXT NOT NULL DEFAULT '',
	classification TEXT NOT NULL,
	detail_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_version_events_version ON version_events(stream, version);

CREATE TABLE IF NOT EXISTS segments (
	segment_id TEXT PRIMARY KEY,
	source_lang TEXT NOT NULL,
	target_lang TEXT NOT NULL,
	source_text TEXT NOT NULL,
	target_text TEXT NOT NULL,
	update_seq INTEGER NOT NULL,
	quality_json TEXT NOT NULL,
	source_file TEXT NOT NULL DEFAULT '',
	content_fp TEXT NOT NULL,
	version INTEGER NOT NULL,
	raw_json TEXT NOT NULL,
	updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS segment_versions (
	segment_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	content_fp TEXT NOT NULL,
	raw_json TEXT NOT NULL,
	PRIMARY KEY(segment_id, version)
);

CREATE TABLE IF NOT EXISTS increments (
	increment_id TEXT PRIMARY KEY,
	base_version INTEGER NOT NULL,
	target_version INTEGER NOT NULL,
	digest TEXT NOT NULL,
	raw_json TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'applied',
	created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS checkpoints (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	seq_no INTEGER NOT NULL UNIQUE,
	stage TEXT NOT NULL,
	segment_id TEXT NOT NULL DEFAULT '',
	unit_id TEXT NOT NULL DEFAULT '',
	content_fp TEXT NOT NULL DEFAULT '',
	version INTEGER NOT NULL,
	artifact_digest TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'active',
	error_code TEXT NOT NULL DEFAULT '',
	detail_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoint_segment_active
	ON checkpoints(stage, segment_id)
	WHERE status='active' AND segment_id <> '';
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoint_unit_active
	ON checkpoints(stage, unit_id)
	WHERE status='active' AND unit_id <> '';
CREATE INDEX IF NOT EXISTS idx_checkpoints_segment ON checkpoints(segment_id, stage);

CREATE TABLE IF NOT EXISTS units (
	unit_id TEXT PRIMARY KEY,
	source_lang TEXT NOT NULL,
	target_lang TEXT NOT NULL,
	source_text TEXT NOT NULL,
	target_text TEXT NOT NULL,
	winning_segment_id TEXT NOT NULL DEFAULT '',
	decision_basis TEXT NOT NULL DEFAULT '',
	score REAL NOT NULL DEFAULT 0,
	occurrence_count INTEGER NOT NULL DEFAULT 0,
	distinct_segment_count INTEGER NOT NULL DEFAULT 0,
	digest TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'active',
	version INTEGER NOT NULL,
	previous_digest TEXT NOT NULL DEFAULT '',
	unit_json TEXT NOT NULL,
	updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_units_status ON units(status);

CREATE TABLE IF NOT EXISTS unit_occurrences (
	unit_id TEXT NOT NULL,
	segment_id TEXT NOT NULL,
	source_index INTEGER NOT NULL,
	target_index INTEGER NOT NULL,
	source_start INTEGER NOT NULL,
	source_end INTEGER NOT NULL,
	target_start INTEGER NOT NULL,
	target_end INTEGER NOT NULL,
	payload_json TEXT NOT NULL,
	PRIMARY KEY(unit_id, segment_id, source_start, target_start)
);
CREATE INDEX IF NOT EXISTS idx_occurrences_segment ON unit_occurrences(segment_id);

CREATE TABLE IF NOT EXISTS suggestions (
	suggestion_uid TEXT PRIMARY KEY,
	logical_id TEXT NOT NULL,
	unit_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	source_text TEXT NOT NULL,
	target_text TEXT NOT NULL,
	score REAL NOT NULL,
	occurrence_count INTEGER NOT NULL,
	digest TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'active',
	suggestion_json TEXT NOT NULL,
	created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_suggestion ON suggestions(logical_id) WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_suggestions_source ON suggestions(source_text);
CREATE INDEX IF NOT EXISTS idx_suggestions_logical ON suggestions(logical_id, status);

CREATE TABLE IF NOT EXISTS invalidations (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	version INTEGER NOT NULL,
	entity_type TEXT NOT NULL,
	entity_id TEXT NOT NULL,
	unit_id TEXT NOT NULL DEFAULT '',
	segment_id TEXT NOT NULL DEFAULT '',
	suggestion_uid TEXT NOT NULL DEFAULT '',
	code TEXT NOT NULL,
	message TEXT NOT NULL,
	chain_json TEXT NOT NULL,
	created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_invalidations_entity ON invalidations(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_invalidations_unit ON invalidations(unit_id);

CREATE TABLE IF NOT EXISTS sync_runs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	version INTEGER NOT NULL,
	mode TEXT NOT NULL,
	status TEXT NOT NULL,
	options_json TEXT NOT NULL DEFAULT '{}',
	report_json TEXT NOT NULL DEFAULT '{}',
	started_at TEXT NOT NULL DEFAULT (datetime('now')),
	finished_at TEXT
);

CREATE TABLE IF NOT EXISTS publish_cursors (
	segment_id TEXT PRIMARY KEY,
	last_seq INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS publications (
	segment_id TEXT NOT NULL,
	seq INTEGER NOT NULL,
	kind TEXT NOT NULL,
	entity_id TEXT NOT NULL DEFAULT '',
	checkpoint_seq INTEGER NOT NULL DEFAULT 0,
	payload_json TEXT NOT NULL DEFAULT '{}',
	status TEXT NOT NULL DEFAULT 'accepted',
	created_at TEXT NOT NULL DEFAULT (datetime('now')),
	PRIMARY KEY(segment_id, seq)
);
`
