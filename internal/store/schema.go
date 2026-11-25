package store

import (
	"context"
	"database/sql"
	"fmt"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS snapshots (
	version INTEGER PRIMARY KEY,
	generated_at TEXT NOT NULL,
	checksum TEXT NOT NULL,
	snapshot_json TEXT NOT NULL,
	created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS logical_tables (
	version INTEGER NOT NULL,
	table_id TEXT NOT NULL,
	logical_db TEXT NOT NULL,
	shard_key TEXT NOT NULL,
	shard_key_type TEXT NOT NULL,
	algorithm_json TEXT NOT NULL,
	primary_key TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (version, table_id)
);

CREATE TABLE IF NOT EXISTS shard_ranges (
	version INTEGER NOT NULL,
	table_id TEXT NOT NULL,
	range_id TEXT NOT NULL,
	lower_bound INTEGER NOT NULL,
	upper_bound INTEGER NOT NULL,
	lower_inclusive INTEGER NOT NULL,
	upper_inclusive INTEGER NOT NULL,
	physical_table TEXT NOT NULL,
	weight INTEGER NOT NULL,
	PRIMARY KEY (version, table_id, range_id)
);

CREATE TABLE IF NOT EXISTS route_cache (
	table_id TEXT PRIMARY KEY,
	cache_version INTEGER NOT NULL,
	expected_version INTEGER NOT NULL,
	physical_tables_json TEXT NOT NULL,
	boundary_index_json TEXT NOT NULL,
	checksum TEXT NOT NULL,
	built_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route_cache_refs (
	table_id TEXT NOT NULL,
	ref_kind TEXT NOT NULL,
	ref_value TEXT NOT NULL,
	PRIMARY KEY (table_id, ref_kind, ref_value)
);

CREATE TABLE IF NOT EXISTS aggregate_cache (
	view_id TEXT PRIMARY KEY,
	cache_version INTEGER NOT NULL,
	expected_version INTEGER NOT NULL,
	depends_on_json TEXT NOT NULL,
	physical_tables_json TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	built_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aggregate_cache_refs (
	view_id TEXT NOT NULL,
	ref_kind TEXT NOT NULL,
	ref_value TEXT NOT NULL,
	PRIMARY KEY (view_id, ref_kind, ref_value)
);

CREATE TABLE IF NOT EXISTS commits (
	commit_id TEXT PRIMARY KEY,
	content_hash TEXT NOT NULL,
	kind TEXT NOT NULL,
	status TEXT NOT NULL,
	version INTEGER NOT NULL,
	response_json TEXT NOT NULL,
	created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
	id TEXT PRIMARY KEY,
	commit_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	base_version INTEGER NOT NULL,
	target_version INTEGER NOT NULL,
	phase TEXT NOT NULL,
	status TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	created_at TEXT NOT NULL,
	finished_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS recovery_reports (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at TEXT NOT NULL,
	report_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_snapshots_version ON snapshots(version);
`

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("migrate sqlite schema: %w", err)
	}
	return nil
}
