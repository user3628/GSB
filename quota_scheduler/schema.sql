PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quota_groups (
    group_id TEXT PRIMARY KEY,
    memory_limit_bytes INTEGER NOT NULL CHECK (memory_limit_bytes >= 0),
    compute_limit_millis INTEGER NOT NULL CHECK (compute_limit_millis >= 0)
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    total_memory_bytes INTEGER NOT NULL CHECK (total_memory_bytes >= 0),
    available_compute_millis INTEGER NOT NULL CHECK (available_compute_millis >= 0),
    occupied_memory_bytes INTEGER NOT NULL DEFAULT 0 CHECK (occupied_memory_bytes >= 0),
    occupied_compute_millis INTEGER NOT NULL DEFAULT 0 CHECK (occupied_compute_millis >= 0),
    quota_group TEXT NOT NULL REFERENCES quota_groups(group_id),
    healthy INTEGER NOT NULL CHECK (healthy IN (0, 1)),
    CHECK (occupied_memory_bytes <= total_memory_bytes),
    CHECK (occupied_compute_millis <= available_compute_millis)
);

CREATE TABLE IF NOT EXISTS models (
    model_id TEXT PRIMARY KEY,
    max_batch_samples INTEGER NOT NULL CHECK (max_batch_samples > 0),
    unit_sample_cost_millis INTEGER NOT NULL CHECK (unit_sample_cost_millis > 0)
);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    model_id TEXT NOT NULL REFERENCES models(model_id),
    node_id TEXT REFERENCES nodes(node_id),
    memory_bytes INTEGER NOT NULL CHECK (memory_bytes >= 0),
    samples_per_batch INTEGER NOT NULL CHECK (samples_per_batch > 0),
    expected_latency_millis INTEGER NOT NULL CHECK (expected_latency_millis > 0),
    priority INTEGER NOT NULL CHECK (priority >= 0),
    quota_group TEXT NOT NULL REFERENCES quota_groups(group_id),
    compute_millis INTEGER NOT NULL CHECK (compute_millis >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'PENDING', 'ADMITTED', 'BATCHED', 'RUNNING',
        'COMPLETED', 'REJECTED', 'PREEMPTED'
    )),
    submission_seq INTEGER NOT NULL UNIQUE,
    last_reason TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reservations (
    request_id TEXT PRIMARY KEY REFERENCES requests(request_id),
    quota_group TEXT NOT NULL REFERENCES quota_groups(group_id),
    memory_bytes INTEGER NOT NULL CHECK (memory_bytes >= 0),
    compute_millis INTEGER NOT NULL CHECK (compute_millis >= 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_op_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_op_id TEXT,
    released_at TEXT,
    release_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_reservations_active
ON reservations(active, quota_group);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL REFERENCES models(model_id),
    node_id TEXT NOT NULL REFERENCES nodes(node_id),
    quota_group TEXT NOT NULL REFERENCES quota_groups(group_id),
    strategy TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'PLANNED', 'QUEUED', 'RUNNING', 'COMPLETED', 'PREEMPTED', 'ABORTED'
    )),
    total_samples INTEGER NOT NULL CHECK (total_samples > 0),
    memory_bytes INTEGER NOT NULL CHECK (memory_bytes >= 0),
    compute_millis INTEGER NOT NULL CHECK (compute_millis >= 0),
    estimated_latency_millis INTEGER NOT NULL CHECK (estimated_latency_millis >= 0),
    priority INTEGER NOT NULL,
    first_submission_seq INTEGER NOT NULL,
    created_round INTEGER,
    created_op_id TEXT NOT NULL,
    reason_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_batches_status
ON batches(status, priority DESC, estimated_latency_millis, first_submission_seq);

CREATE TABLE IF NOT EXISTS batch_members (
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    request_id TEXT NOT NULL REFERENCES requests(request_id),
    piece_index INTEGER NOT NULL,
    samples INTEGER NOT NULL CHECK (samples > 0),
    memory_bytes INTEGER NOT NULL CHECK (memory_bytes >= 0),
    compute_millis INTEGER NOT NULL CHECK (compute_millis >= 0),
    PRIMARY KEY (batch_id, request_id, piece_index)
);

CREATE INDEX IF NOT EXISTS idx_batch_members_request
ON batch_members(request_id);

CREATE TABLE IF NOT EXISTS node_allocations (
    allocation_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES requests(request_id),
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    node_id TEXT NOT NULL REFERENCES nodes(node_id),
    memory_bytes INTEGER NOT NULL CHECK (memory_bytes >= 0),
    compute_millis INTEGER NOT NULL CHECK (compute_millis >= 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_op_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_op_id TEXT,
    released_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_node_allocations_active
ON node_allocations(active, node_id);

CREATE INDEX IF NOT EXISTS idx_node_allocations_request
ON node_allocations(active, request_id);

CREATE TABLE IF NOT EXISTS state_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL REFERENCES requests(request_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    op_seq INTEGER NOT NULL,
    op_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_state_transitions_request
ON state_transitions(request_id, transition_id);

CREATE TABLE IF NOT EXISTS intents (
    op_id TEXT PRIMARY KEY,
    op_seq INTEGER NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    request_id TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PREPARED', 'COMMITTED', 'ABORTED')),
    error_json TEXT,
    created_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_status
ON intents(status, op_seq);

CREATE TABLE IF NOT EXISTS rounds (
    round_no INTEGER PRIMARY KEY,
    time_budget_millis INTEGER NOT NULL,
    max_parallel_batches INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'COMPLETED', 'ABORTED')),
    selected_json TEXT NOT NULL,
    queued_json TEXT NOT NULL,
    skipped_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS round_dispatches (
    round_no INTEGER NOT NULL REFERENCES rounds(round_no),
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    dispatched_at TEXT NOT NULL,
    PRIMARY KEY (round_no, batch_id)
);

CREATE TABLE IF NOT EXISTS batch_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    op_seq INTEGER NOT NULL,
    op_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    recovery_id INTEGER PRIMARY KEY AUTOINCREMENT,
    op_id TEXT NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

