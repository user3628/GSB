package store

const schemaSQL = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(key, value) VALUES ('current_version', '0');

CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    aggregate_key TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at);

CREATE TABLE IF NOT EXISTS recovery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pending_count INTEGER NOT NULL,
    aborted_json TEXT NOT NULL,
    restored_version INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    checksum TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS environments (
    name TEXT PRIMARY KEY,
    priority INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS units (
    environment TEXT NOT NULL,
    service TEXT NOT NULL,
    version INTEGER NOT NULL,
    image_digest TEXT NOT NULL,
    desired_version TEXT NOT NULL,
    probe_json TEXT NOT NULL DEFAULT '{}',
    probe_status TEXT NOT NULL DEFAULT 'pending',
    last_run_id TEXT NOT NULL DEFAULT '',
    last_code TEXT NOT NULL DEFAULT '',
    probed_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(environment, service)
);

CREATE TABLE IF NOT EXISTS config_kv (
    environment TEXT NOT NULL,
    service TEXT NOT NULL,
    config_key TEXT NOT NULL,
    config_value TEXT NOT NULL,
    PRIMARY KEY(environment, service, config_key)
);

CREATE TABLE IF NOT EXISTS dependencies (
    environment TEXT NOT NULL,
    service TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(environment, service, depends_on)
);

CREATE TABLE IF NOT EXISTS plans (
    version INTEGER NOT NULL,
    environment TEXT NOT NULL,
    service TEXT NOT NULL,
    layer INTEGER NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(version, environment, service)
);

CREATE TABLE IF NOT EXISTS topologies (
    version INTEGER NOT NULL,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(version, environment)
);

CREATE TABLE IF NOT EXISTS scores (
    version INTEGER NOT NULL,
    service TEXT NOT NULL,
    score REAL NOT NULL,
    consistent INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(version, service)
);

CREATE TABLE IF NOT EXISTS artifact_refs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_key TEXT NOT NULL,
    environment TEXT NOT NULL DEFAULT '',
    service TEXT NOT NULL DEFAULT '',
    unit_env TEXT NOT NULL DEFAULT '',
    unit_service TEXT NOT NULL DEFAULT '',
    config_key TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_refs_unit ON artifact_refs(unit_env, unit_service);
CREATE INDEX IF NOT EXISTS idx_refs_config ON artifact_refs(unit_env, unit_service, config_key);
CREATE INDEX IF NOT EXISTS idx_refs_artifact ON artifact_refs(artifact_type, artifact_key);

CREATE TABLE IF NOT EXISTS cascade_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS probe_runs (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    global_concurrency INTEGER NOT NULL,
    env_concurrency INTEGER NOT NULL,
    queue_capacity INTEGER NOT NULL,
    budget_ms INTEGER NOT NULL,
    probe_timeout_ms INTEGER NOT NULL DEFAULT 0,
    elapsed_ms INTEGER NOT NULL DEFAULT 0,
    consumed_budget_ms INTEGER NOT NULL DEFAULT 0,
    total_tasks INTEGER NOT NULL DEFAULT 0,
    started_count INTEGER NOT NULL DEFAULT 0,
    succeeded_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    timeout_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    report_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS probe_results (
    run_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    service TEXT NOT NULL,
    status TEXT NOT NULL,
    code TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    latency_ms INTEGER NOT NULL DEFAULT 0,
    detail TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL,
    PRIMARY KEY(run_id, environment, service)
);
`
