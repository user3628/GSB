"""SQLite schema. All component state lives in these six tables."""

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS upload_sessions (
    session_id        TEXT PRIMARY KEY,
    object_key        TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('open','merging','completed','cancelled','aborted')),
    min_part_size     INTEGER NOT NULL,
    max_part_size     INTEGER NOT NULL,
    expected_total_size INTEGER,
    merge_strategy    TEXT NOT NULL DEFAULT 'fast' CHECK (merge_strategy IN ('fast','full')),
    conflict_policy   TEXT NOT NULL CHECK (conflict_policy IN ('reject','allow')),
    part_count        INTEGER NOT NULL DEFAULT 0,
    total_size        INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    last_intent_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_object_key ON upload_sessions(object_key);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON upload_sessions(status);

-- Raw bytes are stored separately so the manifest row stays small and the
-- digest column is the persisted evidence the fast merge path relies on.
CREATE TABLE IF NOT EXISTS parts (
    session_id   TEXT NOT NULL REFERENCES upload_sessions(session_id),
    part_number  INTEGER NOT NULL,
    size_bytes   INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    uploaded_at  TEXT NOT NULL,
    PRIMARY KEY (session_id, part_number)
);

CREATE TABLE IF NOT EXISTS part_bodies (
    session_id   TEXT NOT NULL,
    part_number  INTEGER NOT NULL,
    content      BLOB NOT NULL,
    PRIMARY KEY (session_id, part_number),
    FOREIGN KEY (session_id, part_number) REFERENCES parts(session_id, part_number)
);

CREATE TABLE IF NOT EXISTS object_versions (
    object_key     TEXT NOT NULL,
    version        INTEGER NOT NULL,
    total_size     INTEGER NOT NULL,
    part_count     INTEGER NOT NULL,
    object_sha256  TEXT NOT NULL,
    etag           TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    session_id     TEXT NOT NULL,
    PRIMARY KEY (object_key, version)
);

CREATE TABLE IF NOT EXISTS objects_current (
    object_key      TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL,
    updated_at      TEXT NOT NULL,
    FOREIGN KEY (object_key, current_version)
        REFERENCES object_versions(object_key, version)
);

-- Write-ahead style intent log: every merge is recorded before any mutation
-- and marked terminal only after the object metadata transaction commits.
CREATE TABLE IF NOT EXISTS merge_intents (
    intent_id       TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    object_key      TEXT NOT NULL,
    strategy        TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('pending','committed','aborted')),
    created_at      TEXT NOT NULL,
    committed_at    TEXT,
    detail          TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_state ON merge_intents(state);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id     TEXT PRIMARY KEY,
    created_at    TEXT NOT NULL,
    scanned       INTEGER NOT NULL,
    recovered     INTEGER NOT NULL,
    aborted       INTEGER NOT NULL,
    detail_json   TEXT NOT NULL
);
"""
