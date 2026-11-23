package cache

import "context"

const schemaSQL = `
CREATE TABLE IF NOT EXISTS entries (
    entry_id TEXT PRIMARY KEY,
    cache_key TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    toolchain_version TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    artifact_size INTEGER NOT NULL CHECK (artifact_size >= 0),
    hits INTEGER NOT NULL DEFAULT 0 CHECK (hits >= 0),
    retain_until_ns INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    last_hit_ns INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING','WRITING','AVAILABLE','STALE','EVICTED')),
    version INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_entry_per_key
ON entries(cache_key) WHERE state <> 'EVICTED';

CREATE INDEX IF NOT EXISTS entries_key_order
ON entries(cache_key, created_at_ns DESC);

CREATE TABLE IF NOT EXISTS artifacts (
    digest TEXT PRIMARY KEY,
    size INTEGER NOT NULL CHECK (size >= 0),
    relative_path TEXT NOT NULL UNIQUE,
    ref_count INTEGER NOT NULL CHECK (ref_count >= 0)
);

CREATE TABLE IF NOT EXISTS evictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    artifact_size INTEGER NOT NULL,
    hits INTEGER NOT NULL,
    reason TEXT NOT NULL,
    evicted_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    target TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING','DONE','ABORTED')),
    payload TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    completed_at_ns INTEGER
);

CREATE INDEX IF NOT EXISTS intents_running
ON intents(status, created_at_ns, intent_id);

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generated_at_ns INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

INSERT INTO metadata(key, value) VALUES ('snapshot_sequence', '0')
ON CONFLICT(key) DO NOTHING;
`

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return wrapError(CodeIO, "cannot initialize SQLite schema", nil, err)
	}
	return nil
}
