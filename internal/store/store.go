package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	DB *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	pragmas := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA foreign_keys=ON`,
		`PRAGMA busy_timeout=5000`,
		`PRAGMA synchronous=FULL`,
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma %s: %w", p, err)
		}
	}
	s := &Store{DB: db}
	if err := s.Migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			commit_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			base_version INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			request_json TEXT NOT NULL,
			response_json TEXT,
			error_code TEXT,
			error_json TEXT,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			generated_at_ms INTEGER NOT NULL,
			digest TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS increments (
			commit_id TEXT PRIMARY KEY,
			base_version INTEGER NOT NULL,
			version INTEGER NOT NULL,
			payload_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS services (
			service_id TEXT PRIMARY KEY,
			ordinal INTEGER NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS service_dependencies (
			service_id TEXT NOT NULL,
			depends_on TEXT NOT NULL,
			ordinal INTEGER NOT NULL,
			PRIMARY KEY(service_id, depends_on)
		)`,
		`CREATE TABLE IF NOT EXISTS service_subscriptions (
			service_id TEXT NOT NULL,
			metric TEXT NOT NULL,
			weight INTEGER NOT NULL,
			ordinal INTEGER NOT NULL,
			PRIMARY KEY(service_id, metric)
		)`,
		`CREATE TABLE IF NOT EXISTS samples_current (
			service_id TEXT NOT NULL,
			instance_id TEXT NOT NULL,
			metric TEXT NOT NULL,
			sampled_at_ms INTEGER NOT NULL,
			value INTEGER NOT NULL,
			collection_weight INTEGER NOT NULL,
			PRIMARY KEY(service_id, instance_id, metric, sampled_at_ms)
		) WITHOUT ROWID`,
		`CREATE INDEX IF NOT EXISTS idx_samples_service ON samples_current(service_id)`,
		`CREATE TABLE IF NOT EXISTS derived_views (
			service_id TEXT PRIMARY KEY,
			version INTEGER NOT NULL,
			metrics_json TEXT NOT NULL,
			metrics_checksum TEXT NOT NULL,
			impact_json TEXT NOT NULL,
			impact_checksum TEXT NOT NULL,
			decision_available INTEGER NOT NULL,
			decision_json TEXT,
			decision_checksum TEXT NOT NULL,
			decision_error_code TEXT,
			decision_error_json TEXT,
			refs_json TEXT NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS view_refs (
			consumer_service TEXT NOT NULL,
			ref_kind TEXT NOT NULL,
			target_service TEXT NOT NULL,
			target_version INTEGER NOT NULL,
			target_checksum TEXT NOT NULL,
			PRIMARY KEY(consumer_service, ref_kind, target_service)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_view_refs_target ON view_refs(target_service, ref_kind)`,
		`CREATE TABLE IF NOT EXISTS rebuild_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			version INTEGER NOT NULL,
			requested_mode TEXT NOT NULL,
			selected_path TEXT NOT NULL,
			threshold_micro INTEGER NOT NULL,
			propagation_threshold_micro INTEGER NOT NULL,
			touched_subscription_items INTEGER NOT NULL,
			total_subscription_items INTEGER NOT NULL,
			touched_sample_records INTEGER NOT NULL,
			total_sample_records INTEGER NOT NULL,
			touched_ratio_micro INTEGER NOT NULL,
			propagation_cardinality INTEGER NOT NULL,
			service_count INTEGER NOT NULL,
			recompute_scope_json TEXT NOT NULL,
			reason TEXT NOT NULL,
			metrics_checksum TEXT NOT NULL,
			decisions_checksum TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			started_at_ms INTEGER NOT NULL,
			finished_at_ms INTEGER NOT NULL,
			report_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w\nSQL: %s", err, stmt)
		}
	}
	return nil
}

func NowMS() int64 { return time.Now().UnixMilli() }
