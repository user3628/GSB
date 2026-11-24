package blockpool

import (
	"context"
	"database/sql"
	"fmt"
)

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS namespaces (
		name TEXT PRIMARY KEY
	)`,
	`CREATE TABLE IF NOT EXISTS nodes (
		namespace TEXT NOT NULL,
		path TEXT NOT NULL,
		created_at_ms INTEGER NOT NULL,
		PRIMARY KEY (namespace, path),
		FOREIGN KEY (namespace) REFERENCES namespaces(name)
	)`,
	`CREATE TABLE IF NOT EXISTS blocks (
		digest TEXT PRIMARY KEY,
		size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
		stored_at_ms INTEGER NOT NULL,
		retention_ms INTEGER NOT NULL CHECK (retention_ms >= 0),
		state TEXT NOT NULL CHECK (state IN ('live','pending_reclaim','reclaimed')),
		ref_count INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
		content BLOB,
		CHECK ((state = 'reclaimed' AND content IS NULL) OR (state <> 'reclaimed' AND content IS NOT NULL))
	)`,
	`CREATE INDEX IF NOT EXISTS idx_blocks_state_size_digest ON blocks(state, size_bytes DESC, digest ASC)`,
	`CREATE TABLE IF NOT EXISTS lease_counters (
		namespace TEXT NOT NULL,
		path TEXT NOT NULL,
		digest TEXT NOT NULL,
		next_version INTEGER NOT NULL,
		PRIMARY KEY (namespace, path, digest)
	)`,
	`CREATE TABLE IF NOT EXISTS leases (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		namespace TEXT NOT NULL,
		path TEXT NOT NULL,
		digest TEXT NOT NULL,
		version INTEGER NOT NULL,
		mounted_at_ms INTEGER NOT NULL,
		expires_at_ms INTEGER NOT NULL,
		last_renewed_at_ms INTEGER NOT NULL,
		state TEXT NOT NULL CHECK (state IN ('active','expired','unmounted')),
		UNIQUE (namespace, path, digest, version),
		FOREIGN KEY (namespace, path) REFERENCES nodes(namespace, path),
		FOREIGN KEY (digest) REFERENCES blocks(digest)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_leases_digest_state_expiry ON leases(digest, state, expires_at_ms)`,
	`CREATE TABLE IF NOT EXISTS refs (
		namespace TEXT NOT NULL,
		path TEXT NOT NULL,
		digest TEXT NOT NULL,
		lease_id INTEGER NOT NULL UNIQUE,
		version INTEGER NOT NULL,
		mounted_at_ms INTEGER NOT NULL,
		expires_at_ms INTEGER NOT NULL,
		PRIMARY KEY (namespace, path, digest),
		FOREIGN KEY (namespace, path) REFERENCES nodes(namespace, path),
		FOREIGN KEY (digest) REFERENCES blocks(digest),
		FOREIGN KEY (lease_id) REFERENCES leases(id)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_refs_digest_expiry ON refs(digest, expires_at_ms)`,
	`CREATE INDEX IF NOT EXISTS idx_refs_node_expiry ON refs(namespace, path, expires_at_ms)`,
	`CREATE TABLE IF NOT EXISTS reclaim_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		digest TEXT NOT NULL,
		size_bytes INTEGER NOT NULL,
		reclaimed_at_ms INTEGER NOT NULL,
		reason TEXT NOT NULL,
		request_id TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX IF NOT EXISTS idx_reclaim_records_digest ON reclaim_records(digest, reclaimed_at_ms)`,
	`CREATE TABLE IF NOT EXISTS intents (
		id TEXT PRIMARY KEY,
		request_id TEXT NOT NULL,
		operation TEXT NOT NULL,
		aggregate_key TEXT NOT NULL,
		payload_json TEXT NOT NULL,
		state TEXT NOT NULL CHECK (state IN ('pending','committed','failed','aborted')),
		created_at_ms INTEGER NOT NULL,
		finished_at_ms INTEGER,
		detail TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX IF NOT EXISTS idx_intents_state_created ON intents(state, created_at_ms)`,
	`CREATE TABLE IF NOT EXISTS requests (
		request_id TEXT PRIMARY KEY,
		operation TEXT NOT NULL,
		request_hash TEXT NOT NULL,
		status TEXT NOT NULL CHECK (status IN ('in_progress','completed','aborted')),
		owner TEXT NOT NULL,
		status_code INTEGER NOT NULL DEFAULT 0,
		response_json TEXT,
		error_json TEXT,
		created_at_ms INTEGER NOT NULL,
		completed_at_ms INTEGER
	)`,
	`CREATE TABLE IF NOT EXISTS batch_locks (
		node_key TEXT PRIMARY KEY,
		request_id TEXT NOT NULL,
		acquired_at_ms INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS recovery_reports (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at_ms INTEGER NOT NULL,
		pending_intents INTEGER NOT NULL,
		aborted_intents INTEGER NOT NULL,
		recovered_requests INTEGER NOT NULL,
		diff_count INTEGER NOT NULL,
		note TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS recovery_diffs (
		report_id INTEGER NOT NULL,
		digest TEXT NOT NULL,
		persisted_ref_count INTEGER NOT NULL,
		rebuilt_ref_count INTEGER NOT NULL,
		effective_lease_count INTEGER NOT NULL,
		detail TEXT NOT NULL,
		PRIMARY KEY (report_id, digest),
		FOREIGN KEY (report_id) REFERENCES recovery_reports(id)
	)`,
}

func migrate(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version > 1 {
		return fmt.Errorf("database schema version %d is newer than supported version 1", version)
	}
	for _, stmt := range schemaStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	if version < 1 {
		if _, err := db.ExecContext(ctx, `PRAGMA user_version = 1`); err != nil {
			return fmt.Errorf("set schema version: %w", err)
		}
	}
	return nil
}
