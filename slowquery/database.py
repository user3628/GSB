from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = r"""
CREATE TABLE IF NOT EXISTS operation_intents (
    intent_id TEXT PRIMARY KEY,
    session_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('SESSION_CREATE', 'SAMPLE_BATCH', 'SNAPSHOT_LOAD', 'ANALYZE', 'VERIFY')),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED', 'RECOVERED', 'ABANDONED')),
    checkpoint TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_operation_intents_pending
    ON operation_intents(status, created_at);
CREATE INDEX IF NOT EXISTS idx_operation_intents_session
    ON operation_intents(session_id, kind, status);

CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    sql TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    scanned_rows INTEGER NOT NULL CHECK (scanned_rows >= 0),
    lock_wait_ms INTEGER NOT NULL CHECK (lock_wait_ms >= 0),
    occurred_at TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    normalized_json TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    generated_at TEXT NOT NULL,
    digest TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    loaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_pointer (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    active_version INTEGER NOT NULL REFERENCES snapshots(version),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    sample_id TEXT NOT NULL REFERENCES samples(sample_id),
    status TEXT NOT NULL CHECK (status IN ('CREATED', 'ANALYZING', 'SUGGESTED', 'VERIFIED', 'FAILED')),
    snapshot_version INTEGER REFERENCES snapshots(version),
    is_slow INTEGER CHECK (is_slow IS NULL OR is_slow IN (0, 1)),
    analysis_json TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    revision INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status, updated_at);

CREATE TABLE IF NOT EXISTS session_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    intent_id TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_events_session
    ON session_events(session_id, created_at);

CREATE TABLE IF NOT EXISTS suggestions (
    suggestion_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    rule_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('ADD_INDEX', 'REORDER_PREDICATES', 'SPLIT_OR', 'ADD_LIMIT')),
    title TEXT NOT NULL,
    original_sql TEXT NOT NULL,
    rewritten_sql TEXT NOT NULL,
    setup_sql_json TEXT NOT NULL,
    expected_benefit TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL REFERENCES snapshots(version),
    cause_codes_json TEXT NOT NULL,
    verification_status TEXT NOT NULL DEFAULT 'PROPOSED'
        CHECK (verification_status IN ('PROPOSED', 'VERIFIED', 'FAILED')),
    created_at TEXT NOT NULL,
    UNIQUE (session_id, suggestion_id)
);
CREATE INDEX IF NOT EXISTS idx_suggestions_session ON suggestions(session_id, created_at);

CREATE TABLE IF NOT EXISTS verification_records (
    verification_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    suggestion_id TEXT NOT NULL UNIQUE REFERENCES suggestions(suggestion_id) ON DELETE CASCADE,
    outcome TEXT NOT NULL CHECK (outcome IN ('VERIFIED', 'FAILED')),
    original_cost_json TEXT NOT NULL,
    rewritten_cost_json TEXT NOT NULL,
    original_result_digest TEXT,
    rewritten_result_digest TEXT,
    original_row_count INTEGER,
    rewritten_row_count INTEGER,
    error_code TEXT,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_verification_session ON verification_records(session_id, created_at);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_sessions_legal_transition
BEFORE UPDATE OF status ON sessions
WHEN OLD.status <> NEW.status AND NOT (
    (OLD.status = 'CREATED' AND NEW.status = 'ANALYZING') OR
    (OLD.status = 'ANALYZING' AND NEW.status IN ('SUGGESTED', 'VERIFIED', 'FAILED', 'CREATED')) OR
    (OLD.status = 'SUGGESTED' AND NEW.status IN ('ANALYZING', 'FAILED'))
)
BEGIN
    SELECT RAISE(ABORT, 'E_INVALID_TRANSITION');
END;

CREATE TRIGGER IF NOT EXISTS trg_sessions_suggested_atomic
BEFORE UPDATE OF status ON sessions
WHEN NEW.status = 'SUGGESTED' AND NEW.is_slow = 1
     AND NOT EXISTS (SELECT 1 FROM suggestions WHERE session_id = NEW.session_id)
BEGIN
    SELECT RAISE(ABORT, 'E_INVALID_TRANSITION');
END;

CREATE TRIGGER IF NOT EXISTS trg_sessions_verified_evidence
BEFORE UPDATE OF status ON sessions
WHEN NEW.status = 'VERIFIED'
     AND NOT EXISTS (
         SELECT 1 FROM verification_records
         WHERE session_id = NEW.session_id AND outcome = 'VERIFIED'
     )
BEGIN
    SELECT RAISE(ABORT, 'E_INVALID_TRANSITION');
END;

CREATE TRIGGER IF NOT EXISTS trg_suggestions_require_analyzing
BEFORE INSERT ON suggestions
WHEN (SELECT status FROM sessions WHERE session_id = NEW.session_id) <> 'ANALYZING'
BEGIN
    SELECT RAISE(ABORT, 'E_INVALID_TRANSITION');
END;

CREATE TRIGGER IF NOT EXISTS trg_verification_requires_analyzing
BEFORE INSERT ON verification_records
WHEN (SELECT status FROM sessions WHERE session_id = NEW.session_id) <> 'ANALYZING'
BEGIN
    SELECT RAISE(ABORT, 'E_INVALID_TRANSITION');
END;
"""


class Database:
    def __init__(self, path: Path, busy_timeout_ms: int = 5_000) -> None:
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=self.busy_timeout_ms / 1000.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def connection(self, *, readonly: bool = False) -> Iterator[sqlite3.Connection]:
        if readonly:
            uri = f"{self.path.as_uri()}?mode=ro"
            conn = sqlite3.connect(
                uri,
                uri=True,
                isolation_level=None,
                timeout=self.busy_timeout_ms / 1000.0,
                check_same_thread=False,
            )
        else:
            conn = sqlite3.connect(
                str(self.path),
                isolation_level=None,
                timeout=self.busy_timeout_ms / 1000.0,
                check_same_thread=False,
            )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            conn.execute("PRAGMA foreign_keys=ON")
            if readonly:
                conn.execute("PRAGMA query_only=ON")
            else:
                conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection(readonly=False) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection(readonly=True) as conn:
            conn.execute("BEGIN")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
