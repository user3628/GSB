package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	Path string
}

func Open(runDir string) (*Store, error) {
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, fmt.Errorf("create run directory: %w", err)
	}
	path := filepath.Join(runDir, "mpckpt.sqlite")
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	store := &Store{db: db, Path: path}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS strategy_snapshots (
	version INTEGER PRIMARY KEY,
	generated_at TEXT NOT NULL,
	gpu_cap_bytes INTEGER NOT NULL,
	write_budget_bytes INTEGER NOT NULL,
	time_budget_ms INTEGER NOT NULL,
	loss_scale_initial_fixed INTEGER NOT NULL,
	loss_scale_growth REAL NOT NULL,
	max_abs_error_fixed INTEGER NOT NULL,
	max_rel_error_ppm INTEGER NOT NULL,
	checksum TEXT NOT NULL,
	data_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS data_versions (
	version INTEGER PRIMARY KEY,
	strategy_version INTEGER NOT NULL,
	commit_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shards (
	id TEXT PRIMARY KEY,
	round_no INTEGER NOT NULL,
	precision TEXT NOT NULL,
	size_bytes INTEGER NOT NULL,
	digest TEXT NOT NULL,
	pool TEXT NOT NULL,
	last_access INTEGER NOT NULL,
	source_digest TEXT NOT NULL DEFAULT '',
	momentum_digest TEXT NOT NULL DEFAULT '',
	priority INTEGER NOT NULL DEFAULT 0,
	status TEXT NOT NULL,
	data_version INTEGER NOT NULL,
	write_intent TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shard_versions (
	data_version INTEGER NOT NULL,
	shard_id TEXT NOT NULL,
	round_no INTEGER NOT NULL,
	precision TEXT NOT NULL,
	size_bytes INTEGER NOT NULL,
	digest TEXT NOT NULL,
	pool TEXT NOT NULL,
	last_access INTEGER NOT NULL,
	source_digest TEXT NOT NULL,
	momentum_digest TEXT NOT NULL,
	priority INTEGER NOT NULL,
	status TEXT NOT NULL,
	baseline_ref INTEGER NOT NULL,
	PRIMARY KEY (data_version, shard_id)
);
CREATE TABLE IF NOT EXISTS baseline_refs (
	data_version INTEGER NOT NULL,
	shard_id TEXT NOT NULL,
	PRIMARY KEY (data_version, shard_id)
);
CREATE TABLE IF NOT EXISTS derived_state (
	data_version INTEGER NOT NULL,
	shard_id TEXT NOT NULL,
	primary_digest TEXT NOT NULL,
	consistency_ok INTEGER NOT NULL,
	precision_deviation_fixed INTEGER NOT NULL,
	baseline_digest TEXT NOT NULL,
	rebuilt_at TEXT NOT NULL,
	PRIMARY KEY (data_version, shard_id)
);
CREATE TABLE IF NOT EXISTS derivation_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	data_version INTEGER NOT NULL,
	shard_id TEXT NOT NULL,
	reason TEXT NOT NULL,
	created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS precision_reports (
	data_version INTEGER PRIMARY KEY,
	report_json TEXT NOT NULL,
	within_policy INTEGER NOT NULL,
	created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS import_reports (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	data_version INTEGER NOT NULL,
	commit_id TEXT NOT NULL,
	format TEXT NOT NULL,
	format_version INTEGER NOT NULL,
	verdict TEXT NOT NULL,
	details_json TEXT NOT NULL,
	created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS compatibility_decisions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	commit_id TEXT NOT NULL,
	format TEXT NOT NULL,
	format_version INTEGER NOT NULL,
	verdict TEXT NOT NULL,
	details_json TEXT NOT NULL,
	created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commits (
	commit_id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	request_hash TEXT NOT NULL,
	status TEXT NOT NULL,
	data_version INTEGER NOT NULL DEFAULT 0,
	response_json TEXT NOT NULL DEFAULT '',
	error_json TEXT NOT NULL DEFAULT '',
	undo_json TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	commit_id TEXT NOT NULL,
	action TEXT NOT NULL,
	details_json TEXT NOT NULL,
	created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shard_versions_version ON shard_versions(data_version);
CREATE INDEX IF NOT EXISTS idx_baseline_version ON baseline_refs(data_version);
CREATE INDEX IF NOT EXISTS idx_commits_status ON commits(status);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("migrate sqlite schema: %w", err)
	}
	return nil
}
