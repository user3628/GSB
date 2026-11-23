PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    checksum TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    duplicates_json TEXT NOT NULL DEFAULT '[]',
    derived_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS components (
    version INTEGER NOT NULL,
    component_id TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    cpu_millicores INTEGER NOT NULL,
    memory_bytes INTEGER NOT NULL,
    capacity_cpu_millicores INTEGER NOT NULL,
    capacity_memory_bytes INTEGER NOT NULL,
    PRIMARY KEY (version, component_id),
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS edges (
    version INTEGER NOT NULL,
    upstream TEXT NOT NULL,
    downstream TEXT NOT NULL,
    PRIMARY KEY (version, upstream, downstream),
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_components (
    version INTEGER NOT NULL,
    component_id TEXT NOT NULL,
    resource_summary_json TEXT NOT NULL,
    local_score_micro INTEGER NOT NULL,
    refs_json TEXT NOT NULL,
    PRIMARY KEY (version, component_id),
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_impacts (
    version INTEGER NOT NULL,
    origin TEXT NOT NULL,
    component_id TEXT NOT NULL,
    hop INTEGER NOT NULL,
    topology_level INTEGER,
    recovery_batch INTEGER,
    blocked INTEGER NOT NULL,
    blocked_reason TEXT NOT NULL DEFAULT '',
    blocked_by_cycle TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (version, origin, component_id),
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_batches (
    version INTEGER NOT NULL,
    batch_no INTEGER,
    component_id TEXT NOT NULL,
    blocked INTEGER NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (version, component_id),
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_consistency (
    version INTEGER NOT NULL,
    component_id TEXT NOT NULL,
    score_micro INTEGER NOT NULL,
    decision TEXT NOT NULL,
    trace_json TEXT NOT NULL,
    PRIMARY KEY (version, component_id),
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_refs (
    version INTEGER NOT NULL,
    derived_kind TEXT NOT NULL,
    derived_key TEXT NOT NULL,
    ref_kind TEXT NOT NULL,
    ref_key TEXT NOT NULL,
    field TEXT NOT NULL,
    PRIMARY KEY (version, derived_kind, derived_key, ref_kind, ref_key, field),
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_rebuilds (
    version INTEGER PRIMARY KEY,
    trigger_json TEXT NOT NULL,
    changed_refs_json TEXT NOT NULL,
    invalidated_json TEXT NOT NULL,
    rebuilt_json TEXT NOT NULL,
    strategy TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (version) REFERENCES snapshots(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS submissions (
    idempotency_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER,
    http_status INTEGER NOT NULL DEFAULT 0,
    response_json TEXT,
    error_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    submission_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    target_version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    phase TEXT NOT NULL,
    state TEXT NOT NULL,
    error_json TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state);
CREATE INDEX IF NOT EXISTS idx_intents_submission ON intents(submission_key);

CREATE TABLE IF NOT EXISTS drills (
    id TEXT PRIMARY KEY,
    submission_key TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_drills_state ON drills(state);

CREATE TABLE IF NOT EXISTS reservation_audit (
    drill_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    cpu_millicores INTEGER NOT NULL,
    memory_bytes INTEGER NOT NULL,
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (drill_id, component_id)
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    last_committed_version INTEGER NOT NULL,
    scanned_intents INTEGER NOT NULL,
    aborted_json TEXT NOT NULL,
    facts_json TEXT NOT NULL
);
