package registry

import (
	"context"
	"database/sql"
	"fmt"
)

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS snapshots (
		version INTEGER PRIMARY KEY,
		generated_at TEXT NOT NULL,
		digest TEXT NOT NULL,
		instance_count INTEGER NOT NULL,
		status TEXT NOT NULL CHECK(status IN ('STAGING','COMPLETE')),
		request_id TEXT NOT NULL DEFAULT '',
		base_version INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS snapshot_instances (
		snapshot_version INTEGER NOT NULL,
		service_name TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		endpoint TEXT NOT NULL,
		weight INTEGER NOT NULL,
		lease_ttl_ms INTEGER NOT NULL,
		probe_kind TEXT NOT NULL,
		probe_path TEXT NOT NULL DEFAULT '',
		expected_status INTEGER NOT NULL DEFAULT 0,
		expected_marker TEXT NOT NULL DEFAULT '',
		probe_timeout_ms INTEGER NOT NULL,
		priority INTEGER NOT NULL,
		success_threshold INTEGER NOT NULL,
		failure_threshold INTEGER NOT NULL,
		state TEXT NOT NULL,
		config_fingerprint TEXT NOT NULL,
		PRIMARY KEY(snapshot_version, service_name, instance_id)
	)`,
	`CREATE TABLE IF NOT EXISTS instances (
		service_name TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		endpoint TEXT NOT NULL,
		weight INTEGER NOT NULL,
		lease_ttl_ms INTEGER NOT NULL,
		probe_kind TEXT NOT NULL,
		probe_path TEXT NOT NULL DEFAULT '',
		expected_status INTEGER NOT NULL DEFAULT 0,
		expected_marker TEXT NOT NULL DEFAULT '',
		probe_timeout_ms INTEGER NOT NULL,
		priority INTEGER NOT NULL,
		success_threshold INTEGER NOT NULL,
		failure_threshold INTEGER NOT NULL,
		config_fingerprint TEXT NOT NULL,
		state TEXT NOT NULL,
		consecutive_success INTEGER NOT NULL DEFAULT 0,
		consecutive_failure INTEGER NOT NULL DEFAULT 0,
		last_heartbeat_ms INTEGER NOT NULL DEFAULT 0,
		last_probe_at TEXT NOT NULL DEFAULT '',
		last_probe_ok INTEGER NOT NULL DEFAULT -1,
		last_probe_code TEXT NOT NULL DEFAULT '',
		updated_revision INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(service_name, instance_id)
	)`,
	`CREATE TABLE IF NOT EXISTS leases (
		service_name TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		last_heartbeat_ms INTEGER NOT NULL,
		ttl_ms INTEGER NOT NULL,
		expires_at_ms INTEGER NOT NULL,
		config_version INTEGER NOT NULL,
		PRIMARY KEY(service_name, instance_id)
	)`,
	`CREATE TABLE IF NOT EXISTS derived_revisions (
		revision INTEGER PRIMARY KEY,
		based_on_version INTEGER NOT NULL,
		status TEXT NOT NULL CHECK(status IN ('STAGING','COMPLETE')),
		reason TEXT NOT NULL,
		created_at TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS health_summary (
		revision INTEGER NOT NULL,
		service_name TEXT NOT NULL,
		total INTEGER NOT NULL,
		healthy INTEGER NOT NULL,
		suspect INTEGER NOT NULL,
		unhealthy INTEGER NOT NULL,
		unknown_count INTEGER NOT NULL,
		draining INTEGER NOT NULL,
		available_count INTEGER NOT NULL,
		available_weight INTEGER NOT NULL,
		total_weight INTEGER NOT NULL,
		PRIMARY KEY(revision, service_name)
	)`,
	`CREATE TABLE IF NOT EXISTS weight_allocations (
		revision INTEGER NOT NULL,
		service_name TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		weight INTEGER NOT NULL,
		available INTEGER NOT NULL,
		share_ppm INTEGER NOT NULL,
		PRIMARY KEY(revision, service_name, instance_id)
	)`,
	`CREATE TABLE IF NOT EXISTS probe_plans (
		revision INTEGER NOT NULL,
		service_name TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		endpoint TEXT NOT NULL,
		probe_kind TEXT NOT NULL,
		probe_path TEXT NOT NULL,
		expected_status INTEGER NOT NULL,
		expected_marker TEXT NOT NULL,
		probe_timeout_ms INTEGER NOT NULL,
		priority INTEGER NOT NULL,
		lease_ttl_ms INTEGER NOT NULL,
		lease_expires_at_ms INTEGER NOT NULL,
		lease_remaining_ms INTEGER NOT NULL,
		config_fingerprint TEXT NOT NULL,
		health_state TEXT NOT NULL,
		PRIMARY KEY(revision, service_name, instance_id)
	)`,
	`CREATE TABLE IF NOT EXISTS probe_rounds (
		round_id TEXT PRIMARY KEY,
		snapshot_version INTEGER NOT NULL,
		base_derived_revision INTEGER NOT NULL,
		status TEXT NOT NULL,
		budget_ms INTEGER NOT NULL,
		global_limit INTEGER NOT NULL,
		service_limit INTEGER NOT NULL,
		queue_capacity INTEGER NOT NULL,
		created_at TEXT NOT NULL,
		completed_at TEXT NOT NULL DEFAULT '',
		error_code TEXT NOT NULL DEFAULT '',
		result_json TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS probe_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		round_id TEXT NOT NULL,
		service_name TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		status TEXT NOT NULL,
		ok INTEGER NOT NULL DEFAULT 0,
		code TEXT NOT NULL DEFAULT '',
		latency_ms INTEGER NOT NULL DEFAULT 0,
		observed_at TEXT NOT NULL DEFAULT '',
		detail TEXT NOT NULL DEFAULT '',
		committed INTEGER NOT NULL DEFAULT 0
	)`,
	`CREATE TABLE IF NOT EXISTS intents (
		id TEXT PRIMARY KEY,
		kind TEXT NOT NULL,
		request_id TEXT NOT NULL DEFAULT '',
		payload_hash TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		target_version INTEGER NOT NULL DEFAULT 0,
		derived_revision INTEGER NOT NULL DEFAULT 0,
		round_id TEXT NOT NULL DEFAULT '',
		detail_json TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS requests (
		request_id TEXT PRIMARY KEY,
		operation TEXT NOT NULL,
		payload_hash TEXT NOT NULL,
		status TEXT NOT NULL,
		response_json TEXT NOT NULL DEFAULT '',
		http_status INTEGER NOT NULL DEFAULT 0,
		snapshot_version INTEGER NOT NULL DEFAULT 0,
		error_code TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS recovery_reports (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		recovered_at TEXT NOT NULL,
		report_json TEXT NOT NULL,
		created_at TEXT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_probe_results_round ON probe_results(round_id, status)`,
	`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
	`CREATE INDEX IF NOT EXISTS idx_derived_status ON derived_revisions(status)`,
	`CREATE INDEX IF NOT EXISTS idx_snapshots_status ON snapshots(status)`,
}

func migrate(ctx context.Context, db *sql.DB) error {
	for _, stmt := range schemaStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate schema: %w", err)
		}
	}
	for key, value := range map[string]string{
		"current_version":          "0",
		"current_derived_revision": "0",
		"latest_round_id":          "",
	} {
		if _, err := db.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING`, key, value); err != nil {
			return fmt.Errorf("initialize meta %s: %w", key, err)
		}
	}
	return nil
}
