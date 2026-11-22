"""SQLite schema. Kept as SQL so a fresh Windows environment needs no extras."""

SCHEMA_VERSION = 1

SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS schema_meta (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS revision (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS queues (
    queue_name TEXT PRIMARY KEY,
    priority INTEGER NOT NULL,
    next_seq INTEGER NOT NULL DEFAULT 1,
    rotation INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    queue_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    body_digest TEXT NOT NULL,
    body_json TEXT,
    visibility_timeout_ms INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    redelivery_count INTEGER NOT NULL DEFAULT 0,
    available_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    last_error TEXT,
    active_delivery_id INTEGER,
    PRIMARY KEY (queue_name, message_id),
    UNIQUE (queue_name, seq),
    FOREIGN KEY (queue_name) REFERENCES queues(queue_name)
);

CREATE INDEX IF NOT EXISTS idx_messages_eligible
    ON messages(queue_name, status, available_at_ms, seq);

CREATE TABLE IF NOT EXISTS consumers (
    consumer_id TEXT PRIMARY KEY,
    group_name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_queues (
    consumer_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    PRIMARY KEY (consumer_id, queue_name),
    FOREIGN KEY (consumer_id) REFERENCES consumers(consumer_id) ON DELETE CASCADE,
    FOREIGN KEY (queue_name) REFERENCES queues(queue_name)
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    delivery_mode TEXT NOT NULL,
    round_id TEXT,
    lease_token TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    dispatched_at_ms INTEGER NOT NULL,
    deadline_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    dispatch_outcome TEXT,
    error_code TEXT,
    http_status INTEGER,
    elapsed_ms INTEGER,
    FOREIGN KEY (queue_name, message_id) REFERENCES messages(queue_name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_message
    ON deliveries(queue_name, message_id, attempt);

CREATE INDEX IF NOT EXISTS idx_deliveries_expiry
    ON deliveries(state, deadline_ms);

CREATE UNIQUE INDEX IF NOT EXISTS uq_deliveries_active
    ON deliveries(queue_name, message_id)
    WHERE state = 'DISPATCHED';

CREATE TABLE IF NOT EXISTS acks (
    ack_id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    delivery_id INTEGER NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL,
    request_id TEXT,
    state TEXT NOT NULL,
    acked_at_ms INTEGER NOT NULL,
    UNIQUE (queue_name, message_id, consumer_id),
    UNIQUE (idempotency_key),
    FOREIGN KEY (delivery_id) REFERENCES deliveries(delivery_id)
);

CREATE TABLE IF NOT EXISTS group_offsets (
    group_name TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    next_seq INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (group_name, queue_name)
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    request_id TEXT,
    kind TEXT NOT NULL,
    queue_name TEXT,
    message_id TEXT,
    delivery_id INTEGER,
    consumer_id TEXT,
    state TEXT NOT NULL,
    payload_json TEXT,
    result_json TEXT,
    created_at_ms INTEGER NOT NULL,
    applied_at_ms INTEGER,
    finished_at_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_intents_unfinished
    ON intents(state, created_at_ms);

CREATE TABLE IF NOT EXISTS request_results (
    request_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    status TEXT NOT NULL,
    owner_token TEXT NOT NULL,
    result_json TEXT,
    error_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    request_id TEXT,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    config_json TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    result_json TEXT
);

CREATE TABLE IF NOT EXISTS round_traces (
    trace_id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    event TEXT NOT NULL,
    queue_name TEXT,
    message_id TEXT,
    seq INTEGER,
    delivery_id INTEGER,
    consumer_id TEXT,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_round_traces_round
    ON round_traces(round_id, trace_id);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    created_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


def initialize_schema(conn) -> None:
    conn.executescript(SCHEMA_SQL)
    row = conn.execute("SELECT COUNT(*) AS n FROM schema_meta").fetchone()
    if row["n"] == 0:
        conn.execute("INSERT INTO schema_meta(version) VALUES (?)", (SCHEMA_VERSION,))
    row = conn.execute("SELECT COUNT(*) AS n FROM revision WHERE id = 1").fetchone()
    if row["n"] == 0:
        conn.execute("INSERT INTO revision(id, value) VALUES (1, 0)")
