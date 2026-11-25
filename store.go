package grayrouter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Config struct {
	DBPath     string
	CrashPoint string
	ReadyFile  string
	Now        func() time.Time
	Hooks      Hooks
}

type Hooks struct {
	AfterDeltaGate func(baseVersion int64)
}

type Host struct {
	db              *sql.DB
	cfg             Config
	Hooks           Hooks
	writeMu         chan struct{}
	idemMu          sync.Mutex
	idemCalls       map[string]*idempotentCall
	deltaMu         sync.Mutex
	deltaGates      map[int64]*deltaGate
	recovery        *RecoveryReport
	InferExecutions int64
}

type idempotentCall struct {
	done     chan struct{}
	response any
	err      error
}

type deltaGate struct {
	base   int64
	done   chan struct{}
	report *CommitReport
	err    error
}

func Open(ctx context.Context, cfg Config) (*Host, error) {
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join("data", "grayrouter.db")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if dir := filepath.Dir(cfg.DBPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := cfg.DBPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	host := &Host{
		db: db, cfg: cfg, Hooks: cfg.Hooks, writeMu: make(chan struct{}, 1),
		idemCalls: map[string]*idempotentCall{}, deltaGates: map[int64]*deltaGate{},
	}
	host.writeMu <- struct{}{}
	if err := host.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := host.Recover(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return host, nil
}

func (h *Host) Close() error { return h.db.Close() }

func (h *Host) nowNano() int64 { return h.cfg.Now().UnixNano() }

func (h *Host) lockWriter(ctx context.Context) error {
	select {
	case <-h.writeMu:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Host) unlockWriter() { h.writeMu <- struct{}{} }

func (h *Host) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS head_state (
			id INTEGER PRIMARY KEY CHECK(id=1),
			snapshot_version INTEGER NOT NULL,
			updated_at_ns INTEGER NOT NULL
		)`,
		`INSERT OR IGNORE INTO head_state(id, snapshot_version, updated_at_ns) VALUES(1, 0, 0)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			base_version INTEGER NOT NULL DEFAULT 0,
			target_version INTEGER NOT NULL DEFAULT 0,
			request_id TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL,
			status TEXT NOT NULL,
			recovery_note TEXT NOT NULL DEFAULT '',
			created_at_ns INTEGER NOT NULL,
			updated_at_ns INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_delta_base_committed
		 ON intents(base_version)
		 WHERE kind='delta_commit' AND status IN ('prepared','running','completed')`,
		`CREATE TABLE IF NOT EXISTS idempotent_requests (
			request_key TEXT PRIMARY KEY,
			request_hash TEXT NOT NULL,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			status_code INTEGER NOT NULL DEFAULT 200,
			response_json TEXT NOT NULL DEFAULT '',
			error_json TEXT NOT NULL DEFAULT '',
			created_at_ns INTEGER NOT NULL,
			completed_at_ns INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			kind TEXT NOT NULL,
			base_version INTEGER NOT NULL DEFAULT 0,
			request_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			report_json TEXT NOT NULL DEFAULT '',
			model_count INTEGER NOT NULL DEFAULT 0,
			created_at_ns INTEGER NOT NULL,
			committed_at_ns INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS model_versions (
			model_key TEXT PRIMARY KEY,
			model_name TEXT NOT NULL,
			model_version TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			graph_json TEXT NOT NULL,
			route_json TEXT NOT NULL,
			gray_json TEXT NOT NULL,
			signature TEXT NOT NULL,
			structural_signature TEXT NOT NULL,
			created_at_ns INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_model_versions_name ON model_versions(model_name)`,
		`CREATE TABLE IF NOT EXISTS current_models (
			model_name TEXT PRIMARY KEY,
			model_key TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			updated_at_ns INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS migration_plans (
			snapshot_version INTEGER NOT NULL,
			model_key TEXT NOT NULL,
			diff_json TEXT NOT NULL,
			plan_json TEXT NOT NULL,
			PRIMARY KEY(snapshot_version, model_key)
		)`,
		`CREATE TABLE IF NOT EXISTS derived_artifacts (
			model_key TEXT NOT NULL,
			artifact TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			action TEXT NOT NULL,
			status TEXT NOT NULL,
			revision TEXT NOT NULL,
			checksum TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			updated_at_ns INTEGER NOT NULL,
			PRIMARY KEY(model_key, artifact)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_derived_snapshot_status ON derived_artifacts(snapshot_version, status)`,
		`CREATE TABLE IF NOT EXISTS routing_entries (
			model_key TEXT PRIMARY KEY,
			model_name TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			path TEXT NOT NULL,
			config_json TEXT NOT NULL,
			checksum TEXT NOT NULL,
			status TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS prepost_cache (
			cache_key TEXT PRIMARY KEY,
			model_key TEXT NOT NULL,
			step_id TEXT NOT NULL,
			phase TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			payload_json TEXT NOT NULL,
			checksum TEXT NOT NULL,
			status TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_prepost_model ON prepost_cache(model_key)`,
		`CREATE TABLE IF NOT EXISTS inflight_bindings (
			request_id TEXT PRIMARY KEY,
			idempotency_key TEXT NOT NULL,
			model_key TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			gray_digest TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			result_json TEXT NOT NULL DEFAULT '',
			error_json TEXT NOT NULL DEFAULT '',
			bound_at_ns INTEGER NOT NULL,
			updated_at_ns INTEGER NOT NULL,
			completed_at_ns INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_inflight_status ON inflight_bindings(status)`,
		`CREATE TABLE IF NOT EXISTS gray_plans (
			route_key TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			baseline_model_key TEXT NOT NULL,
			canary_model_key TEXT NOT NULL,
			canary_rate REAL NOT NULL,
			scale INTEGER NOT NULL,
			canary_fixed INTEGER NOT NULL,
			normalization_json TEXT NOT NULL,
			plan_json TEXT NOT NULL,
			status TEXT NOT NULL,
			updated_at_ns INTEGER NOT NULL,
			PRIMARY KEY(route_key, snapshot_version)
		)`,
	}
	for _, statement := range statements {
		if _, err := h.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite: %w\nSQL: %s", err, statement)
		}
	}
	return nil
}

func (h *Host) headVersion(ctx context.Context) (int64, error) {
	var version int64
	err := h.db.QueryRowContext(ctx, `SELECT snapshot_version FROM head_state WHERE id=1`).Scan(&version)
	return version, err
}

func (h *Host) fault(point string) {
	if h.cfg.CrashPoint != point {
		return
	}
	if h.cfg.ReadyFile != "" {
		_ = os.WriteFile(h.cfg.ReadyFile, []byte(point+"\n"), 0o644)
	}
	select {}
}
