package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS policy_snapshots (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    committed_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    norm_path TEXT NOT NULL,
    norm_query TEXT NOT NULL DEFAULT '',
    vary_json TEXT NOT NULL DEFAULT '[]',
    etag TEXT NOT NULL DEFAULT '',
    body BLOB,
    body_length INTEGER NOT NULL DEFAULT 0,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    tags_json TEXT NOT NULL DEFAULT '[]',
    state TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    written_at_ms INTEGER NOT NULL DEFAULT 0,
    fresh_until_ms INTEGER NOT NULL DEFAULT 0,
    stale_until_ms INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL,
    last_request_id TEXT NOT NULL DEFAULT '',
    last_intent_id INTEGER NOT NULL DEFAULT 0,
    UNIQUE(node_id, cache_key)
);
CREATE INDEX IF NOT EXISTS idx_entries_path ON cache_entries(node_id, norm_path);
CREATE INDEX IF NOT EXISTS idx_entries_state ON cache_entries(state);

CREATE TABLE IF NOT EXISTS route_map (
    entry_id INTEGER PRIMARY KEY REFERENCES cache_entries(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    norm_path TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    purge_id TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_route_path ON route_map(node_id, norm_path, active);

CREATE TABLE IF NOT EXISTS entry_tags (
    entry_id INTEGER NOT NULL REFERENCES cache_entries(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    node_id TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    purge_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(entry_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_tags_lookup ON entry_tags(tag, node_id, active);

CREATE TABLE IF NOT EXISTS key_epochs (
    cache_key TEXT PRIMARY KEY,
    epoch INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS entry_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    request_id TEXT NOT NULL DEFAULT '',
    snapshot_version INTEGER NOT NULL,
    at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transitions_entry ON entry_transitions(entry_id, id);

CREATE TABLE IF NOT EXISTS origin_objects (
    cache_key TEXT PRIMARY KEY,
    etag TEXT NOT NULL,
    body BLOB NOT NULL,
    body_length INTEGER NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    tags_json TEXT NOT NULL,
    version_seq INTEGER NOT NULL DEFAULT 1,
    updated_at_ms INTEGER NOT NULL,
    fetch_count INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS origin_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    status TEXT NOT NULL,
    code TEXT NOT NULL DEFAULT '',
    etag TEXT NOT NULL DEFAULT '',
    duration_ms INTEGER NOT NULL DEFAULT 0,
    at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_origin_attempts_at ON origin_attempts(at_ms);

CREATE TABLE IF NOT EXISTS request_records (
    request_id TEXT PRIMARY KEY,
    method TEXT NOT NULL,
    node_id TEXT NOT NULL DEFAULT '',
    snapshot_version INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    response_status INTEGER NOT NULL DEFAULT 0,
    response_body BLOB,
    response_headers_json TEXT NOT NULL DEFAULT '{}',
    business_code TEXT NOT NULL DEFAULT '',
    trace_json TEXT NOT NULL DEFAULT '',
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_requests_status ON request_records(status);
CREATE INDEX IF NOT EXISTS idx_requests_code ON request_records(business_code);

CREATE TABLE IF NOT EXISTS purge_records (
    purge_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    target TEXT NOT NULL,
    request_id TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    deadline_ms INTEGER NOT NULL,
    purge_window_ms INTEGER NOT NULL,
    incomplete_nodes_json TEXT NOT NULL DEFAULT '[]',
    error_code TEXT NOT NULL DEFAULT '',
    affected_entries INTEGER NOT NULL DEFAULT 0,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_purge_status ON purge_records(status);

CREATE TABLE IF NOT EXISTS purge_deliveries (
    purge_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    status TEXT NOT NULL,
    entry_count INTEGER NOT NULL DEFAULT 0,
    error_code TEXT NOT NULL DEFAULT '',
    attempted_at_ms INTEGER NOT NULL DEFAULT 0,
    completed_at_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(purge_id, node_id)
);

CREATE TABLE IF NOT EXISTS intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    request_id TEXT NOT NULL DEFAULT '',
    snapshot_version INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    business_code TEXT NOT NULL DEFAULT '',
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL DEFAULT '',
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_code ON audit_events(code);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generated_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`
