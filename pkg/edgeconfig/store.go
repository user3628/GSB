package edgeconfig

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	if path != ":memory:" {
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(8)
	}
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA synchronous=NORMAL",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("pragma %s: %w", pragma, err)
		}
	}
	store := &Store{db: db}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS config_keys (
			path TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			value_json TEXT NOT NULL,
			order_no INTEGER NOT NULL,
			updated_version INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS nodes (
			id TEXT PRIMARY KEY,
			region TEXT NOT NULL,
			endpoint TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS subscriptions (
			node_id TEXT NOT NULL,
			key_path TEXT NOT NULL,
			position INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(node_id, key_path)
		)`,
		`CREATE TABLE IF NOT EXISTS node_dependencies (
			node_id TEXT NOT NULL,
			depends_on TEXT NOT NULL,
			PRIMARY KEY(node_id, depends_on)
		)`,
		`CREATE TABLE IF NOT EXISTS idempotent_requests (
			idempotency_key TEXT PRIMARY KEY,
			request_hash TEXT NOT NULL,
			kind TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			http_status INTEGER NOT NULL DEFAULT 200,
			response_json TEXT NOT NULL DEFAULT '{}',
			error_code TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			error_details_json TEXT,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			idempotency_key TEXT NOT NULL DEFAULT '',
			request_hash TEXT NOT NULL DEFAULT '',
			target_version INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			payload_json TEXT NOT NULL DEFAULT '{}',
			recovery_action TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			commit_id TEXT NOT NULL UNIQUE,
			generated_at_ms INTEGER NOT NULL,
			digest TEXT NOT NULL,
			canonical_json TEXT NOT NULL,
			complete INTEGER NOT NULL DEFAULT 1,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS increments (
			version INTEGER PRIMARY KEY,
			base_version INTEGER NOT NULL,
			commit_id TEXT NOT NULL UNIQUE,
			payload_json TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS affected_reports (
			version INTEGER PRIMARY KEY,
			report_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS expected_views (
			commit_version INTEGER NOT NULL,
			node_id TEXT NOT NULL,
			region TEXT NOT NULL,
			expected_version INTEGER NOT NULL,
			expected_digest TEXT NOT NULL,
			key_count INTEGER NOT NULL,
			digest_json TEXT NOT NULL,
			PRIMARY KEY(commit_version, node_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_expected_views_node ON expected_views(node_id, expected_version)`,
		`CREATE TABLE IF NOT EXISTS node_convergence (
			node_id TEXT PRIMARY KEY,
			region TEXT NOT NULL,
			expected_version INTEGER NOT NULL DEFAULT 0,
			expected_digest TEXT NOT NULL DEFAULT '',
			observed_version INTEGER NOT NULL DEFAULT 0,
			applied_digest TEXT NOT NULL DEFAULT '',
			effective_at_ms INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'PENDING',
			reason_code TEXT NOT NULL DEFAULT '',
			last_report_id TEXT NOT NULL DEFAULT '',
			last_round_id TEXT NOT NULL DEFAULT '',
			weight INTEGER NOT NULL DEFAULT 1,
			updated_version INTEGER NOT NULL DEFAULT 0,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS node_reports (
			report_id TEXT PRIMARY KEY,
			node_id TEXT NOT NULL,
			observed_version INTEGER NOT NULL,
			applied_digest TEXT NOT NULL,
			effective_at_ms INTEGER NOT NULL,
			received_at_ms INTEGER NOT NULL,
			decision TEXT NOT NULL,
			code TEXT NOT NULL DEFAULT '',
			expected_version INTEGER NOT NULL,
			current_view_version INTEGER NOT NULL,
			response_json TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_node_reports_node ON node_reports(node_id, received_at_ms)`,
		`CREATE TABLE IF NOT EXISTS probe_rounds (
			round_id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL DEFAULT '',
			target_version INTEGER NOT NULL,
			status TEXT NOT NULL,
			timeout_ms INTEGER NOT NULL,
			concurrency INTEGER NOT NULL,
			started_at_ms INTEGER NOT NULL,
			completed_at_ms INTEGER NOT NULL,
			total_count INTEGER NOT NULL,
			converged_count INTEGER NOT NULL,
			timeout_count INTEGER NOT NULL,
			rejected_count INTEGER NOT NULL,
			failed_count INTEGER NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS probe_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			round_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			endpoint TEXT NOT NULL,
			status TEXT NOT NULL,
			http_status INTEGER NOT NULL DEFAULT 0,
			applied_digest TEXT NOT NULL DEFAULT '',
			observed_version INTEGER NOT NULL DEFAULT 0,
			effective_at_ms INTEGER NOT NULL DEFAULT 0,
			error_code TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			latency_ms INTEGER NOT NULL DEFAULT 0,
			timed_out INTEGER NOT NULL DEFAULT 0,
			converged INTEGER NOT NULL DEFAULT 0,
			expected_version INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_probe_records_round ON probe_records(round_id)`,
		`CREATE TABLE IF NOT EXISTS consistency_views (
			scope TEXT PRIMARY KEY,
			commit_version INTEGER NOT NULL,
			round_id TEXT NOT NULL DEFAULT '',
			score_units INTEGER NOT NULL,
			threshold_units INTEGER NOT NULL,
			decision TEXT NOT NULL,
			consistency_json TEXT NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_runs (
			recovery_id INTEGER PRIMARY KEY AUTOINCREMENT,
			report_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate: %w\nSQL: %s", err, statement)
		}
	}
	return nil
}

func (s *Store) metaInt(ctx context.Context, key string) (int64, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var parsed int64
	_, err = fmt.Sscan(value, &parsed)
	return parsed, err
}

func (s *Store) setMetaTx(ctx context.Context, tx *sql.Tx, key string, value any) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO meta(key, value) VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, fmt.Sprint(value))
	return err
}
