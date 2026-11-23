package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS key_state (
    object_key TEXT PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 0,
    active_session_id TEXT NOT NULL DEFAULT '',
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    lower_bound INTEGER NOT NULL,
    ttl_ms INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','completed','cancelled','expired')),
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    cancelled_at_ms INTEGER,
    cancel_reason TEXT NOT NULL DEFAULT '',
    object_sha256 TEXT NOT NULL DEFAULT '',
    object_total_size INTEGER NOT NULL DEFAULT 0,
    object_part_count INTEGER NOT NULL DEFAULT 0,
    creation_request_id TEXT NOT NULL DEFAULT '',
    UNIQUE(object_key, generation)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_sessions_one_active_per_key
    ON sessions(object_key) WHERE status = 'active';
CREATE UNIQUE INDEX IF NOT EXISTS ux_sessions_creation_request
    ON sessions(creation_request_id) WHERE creation_request_id <> '';
CREATE INDEX IF NOT EXISTS ix_sessions_object_key ON sessions(object_key, generation DESC);
CREATE INDEX IF NOT EXISTS ix_sessions_expiry ON sessions(status, expires_at_ms);

CREATE TABLE IF NOT EXISTS parts (
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    arrived_at_ms INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('active','void')),
    created_at_ms INTEGER NOT NULL,
    content BLOB NOT NULL,
    PRIMARY KEY(session_id, part_number)
);
CREATE INDEX IF NOT EXISTS ix_parts_session_status ON parts(session_id, status, part_number);

CREATE TABLE IF NOT EXISTS objects (
    object_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    total_size INTEGER NOT NULL,
    part_count INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_objects_generation ON objects(generation);

CREATE TABLE IF NOT EXISTS intents (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    session_id TEXT NOT NULL DEFAULT '',
    object_key TEXT NOT NULL DEFAULT '',
    part_number INTEGER,
    sha256 TEXT NOT NULL DEFAULT '',
    size INTEGER,
    payload_json TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('pending','committed','rejected','abandoned')),
    created_at_ms INTEGER NOT NULL,
    committed_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS ix_intents_status_seq ON intents(status, seq);
CREATE INDEX IF NOT EXISTS ix_intents_session_seq ON intents(session_id, seq);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('processing','completed','interrupted')),
    http_status INTEGER NOT NULL DEFAULT 0,
    response_body BLOB,
    code TEXT NOT NULL DEFAULT '',
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS ix_requests_status ON requests(status, created_at_ms);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    generated_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
`
