package mfa

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type EngineOptions struct {
	ClockUnix    int64
	Limits       Limits
	BeforeCommit func(operation string)
	AfterCommit  func(operation string)
}

type Engine struct {
	db        *sql.DB
	limits    Limits
	clockUnix int64
	options   EngineOptions

	writeMu sync.Mutex
	sem     chan struct{}

	budgetMu sync.Mutex
	pending  map[string]int

	rebindLocks    sync.Map
	rebindInFlight sync.Map
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS engine_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    name TEXT NOT NULL,
    UNIQUE (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS policies (
    tenant_id TEXT PRIMARY KEY REFERENCES tenants(id),
    min_strength INTEGER NOT NULL,
    required_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS factors (
    factor_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    factor_type TEXT NOT NULL,
    sensitivity TEXT NOT NULL,
    strength INTEGER NOT NULL,
    status TEXT NOT NULL,
    rebindable INTEGER NOT NULL,
    last_activated INTEGER NOT NULL,
    expires_at_unix INTEGER NOT NULL,
    secret_seed TEXT NOT NULL,
    recovery_code_count INTEGER NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    degraded_reason TEXT NOT NULL DEFAULT '',
    created_seq INTEGER NOT NULL,
    FOREIGN KEY (tenant_id, user_id) REFERENCES users(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS factors_owner_idx ON factors(tenant_id, user_id, factor_type);

CREATE TABLE IF NOT EXISTS recovery_codes (
    factor_id TEXT NOT NULL REFERENCES factors(factor_id) ON DELETE CASCADE,
    code_index INTEGER NOT NULL,
    code_hash TEXT NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0,
    consumed_attempt TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (factor_id, code_index)
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    parent_version INTEGER NOT NULL,
    parent_hash TEXT NOT NULL,
    payload TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    chain_hash TEXT NOT NULL,
    reason TEXT NOT NULL,
    attempt_id TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS snapshot_seals (
    version INTEGER PRIMARY KEY,
    content_hash TEXT NOT NULL,
    chain_hash TEXT NOT NULL,
    reason TEXT NOT NULL,
    attempt_id TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS snapshot_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    head_version INTEGER NOT NULL,
    head_hash TEXT NOT NULL
);
INSERT OR IGNORE INTO snapshot_meta(singleton, head_version, head_hash) VALUES (1, 0, '');

CREATE TABLE IF NOT EXISTS usage_traces (
    trace_id TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    factor_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    factor_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    code TEXT NOT NULL,
    challenge_prompt TEXT NOT NULL,
    response_sha256 TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_traces (
    trace_id TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    factor_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    consecutive_failures INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rebind_traces (
    trace_id TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    factor_type TEXT NOT NULL,
    old_factor_id TEXT NOT NULL,
    new_factor_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    status TEXT NOT NULL,
    code TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT OR IGNORE INTO operation_meta(key, value) VALUES
    ('factor_seq', 0),
    ('usage_seq', 0),
    ('risk_seq', 0),
    ('rebind_seq', 0);
`

func NewEngine(ctx context.Context, path string, options EngineOptions) (*Engine, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if path != ":memory:" && !filepath.IsAbs(path) {
		if absolute, err := filepath.Abs(path); err == nil {
			path = absolute
		}
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, `
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = 5000;
`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	engine := &Engine{
		db:        db,
		limits:    options.Limits.withDefaults(),
		clockUnix: options.ClockUnix,
		options:   options,
		pending:   make(map[string]int),
	}
	if err := engine.loadEngineMeta(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if options.ClockUnix > 0 {
		engine.clockUnix = options.ClockUnix
	}
	engine.sem = make(chan struct{}, engine.limits.MaxConcurrency)
	return engine, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) Limits() Limits {
	e.budgetMu.Lock()
	defer e.budgetMu.Unlock()
	return e.limits
}

func (e *Engine) ClockUnix() int64 {
	if e.clockUnix > 0 {
		return e.clockUnix
	}
	return time.Now().Unix()
}

func (e *Engine) loadEngineMeta(ctx context.Context) error {
	rows, err := e.db.QueryContext(ctx, `SELECT key, value FROM engine_meta`)
	if err != nil {
		return fmt.Errorf("load engine metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		switch key {
		case "limits":
			var limits Limits
			if err := json.Unmarshal([]byte(value), &limits); err != nil {
				return fmt.Errorf("decode persisted limits: %w", err)
			}
			e.limits = limits
		case "clock_unix":
			if e.clockUnix == 0 {
				if _, err := fmt.Sscan(value, &e.clockUnix); err != nil {
					return fmt.Errorf("decode persisted clock: %w", err)
				}
			}
		}
	}
	return rows.Err()
}

func (e *Engine) saveEngineMeta(ctx context.Context, tx *sql.Tx, clockUnix int64, limits Limits) error {
	encoded, err := json.Marshal(limits)
	if err != nil {
		return err
	}
	values := map[string]string{
		"limits":     string(encoded),
		"clock_unix": fmt.Sprintf("%d", clockUnix),
	}
	for key, value := range values {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO engine_meta(key, value) VALUES(?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, key, value); err != nil {
			return err
		}
	}
	return nil
}

func nextSequence(ctx context.Context, tx *sql.Tx, key string) (int64, error) {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO operation_meta(key, value) VALUES(?, 0)
ON CONFLICT(key) DO NOTHING
`, key); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operation_meta SET value = value + 1 WHERE key = ?`, key); err != nil {
		return 0, err
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT value FROM operation_meta WHERE key = ?`, key).Scan(&sequence); err != nil {
		return 0, err
	}
	return sequence, nil
}

func commitWithHooks(tx *sql.Tx, operation string, options EngineOptions) error {
	if options.BeforeCommit != nil {
		options.BeforeCommit(operation)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if options.AfterCommit != nil {
		options.AfterCommit(operation)
	}
	return nil
}
