package store

import (
	"context"
	"database/sql"
	"fmt"
)

func migrate(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS releases (
			release_id TEXT PRIMARY KEY,
			definition_json TEXT NOT NULL,
			state TEXT NOT NULL,
			current_stage INTEGER NOT NULL,
			traffic_per_mille INTEGER NOT NULL,
			current_stats_version INTEGER NOT NULL,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS stats_versions (
			release_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			status TEXT NOT NULL,
			snapshot_json TEXT NOT NULL,
			committed_at_ms INTEGER NOT NULL,
			PRIMARY KEY (release_id, version)
		)`,
		`CREATE TABLE IF NOT EXISTS observation_batches (
			release_id TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			stage INTEGER NOT NULL,
			status TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			observation_count INTEGER NOT NULL,
			stats_version INTEGER NOT NULL,
			received_at_ms INTEGER NOT NULL,
			committed_at_ms INTEGER,
			abort_reason TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (release_id, batch_id)
		)`,
		`CREATE TABLE IF NOT EXISTS observations (
			release_id TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			stage INTEGER NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			instance_id TEXT NOT NULL,
			group_name TEXT NOT NULL,
			success INTEGER NOT NULL,
			latency_us INTEGER NOT NULL,
			PRIMARY KEY (release_id, batch_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS decisions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			release_id TEXT NOT NULL,
			stats_version INTEGER NOT NULL,
			stage INTEGER NOT NULL,
			scope TEXT NOT NULL,
			outcome TEXT NOT NULL,
			code TEXT NOT NULL,
			triggered_threshold TEXT NOT NULL DEFAULT '',
			measured_json TEXT NOT NULL,
			from_state TEXT NOT NULL,
			to_state TEXT NOT NULL,
			from_stage INTEGER NOT NULL,
			to_stage INTEGER NOT NULL,
			from_traffic_per_mille INTEGER NOT NULL,
			to_traffic_per_mille INTEGER NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			release_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			from_state TEXT NOT NULL,
			from_stage INTEGER NOT NULL,
			from_traffic_per_mille INTEGER NOT NULL,
			from_version INTEGER NOT NULL,
			created_at_ms INTEGER NOT NULL,
			completed_at_ms INTEGER,
			recovery_detail TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS intents_pending ON intents(status, created_at_ms)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			report_id INTEGER PRIMARY KEY AUTOINCREMENT,
			started_at_ms INTEGER NOT NULL,
			finished_at_ms INTEGER NOT NULL,
			release_ids_json TEXT NOT NULL,
			pending_intents_found INTEGER NOT NULL,
			intents_recovered INTEGER NOT NULL,
			orphan_batches_aborted INTEGER NOT NULL,
			versions_restored INTEGER NOT NULL,
			facts_json TEXT NOT NULL,
			claim_boundary TEXT NOT NULL
		)`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate SQLite: %w", err)
		}
	}
	return nil
}
