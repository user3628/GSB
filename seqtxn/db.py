"""SQLite schema and transaction helpers.

All calls use their own short-lived connection.  WAL permits concurrent
readers while BEGIN IMMEDIATE serializes writers at the database boundary.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA_VERSION = 1


SCHEMA = r"""
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 10000;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
    name TEXT PRIMARY KEY,
    partition_count INTEGER NOT NULL CHECK (partition_count > 0),
    routing_mode TEXT NOT NULL CHECK (routing_mode IN ('hash', 'explicit')),
    routing_salt TEXT NOT NULL,
    config_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
    topic TEXT NOT NULL,
    partition_no INTEGER NOT NULL CHECK (partition_no >= 0),
    last_sequence INTEGER NOT NULL DEFAULT 0,
    visibility_sequence INTEGER NOT NULL DEFAULT 0,
    head_sequence INTEGER,
    delivery_sequence INTEGER,
    delivery_ready INTEGER NOT NULL DEFAULT 0,
    acked_through INTEGER NOT NULL DEFAULT 0,
    lease_epoch INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, partition_no),
    FOREIGN KEY (topic) REFERENCES topics(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'COMMITTED', 'ROLLED_BACK')),
    idempotency_key TEXT NOT NULL,
    decision_source TEXT,
    created_at INTEGER NOT NULL,
    decided_at INTEGER,
    query_count INTEGER NOT NULL DEFAULT 0,
    last_outcome TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    partition_no INTEGER NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    producer_id TEXT NOT NULL,
    client_sequence TEXT NOT NULL,
    txn_id TEXT,
    body TEXT NOT NULL,
    body_digest TEXT NOT NULL,
    produced_at INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'COMMITTED', 'ROLLED_BACK')),
    visible_at INTEGER,
    decision_source TEXT,
    acked_at INTEGER,
    ack_lease_id TEXT,
    UNIQUE (producer_id, client_sequence),
    UNIQUE (topic, partition_no, sequence),
    FOREIGN KEY (topic, partition_no) REFERENCES partitions(topic, partition_no),
    FOREIGN KEY (txn_id) REFERENCES transactions(txn_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_head
    ON messages(topic, partition_no, sequence, state, acked_at);
CREATE INDEX IF NOT EXISTS idx_messages_txn
    ON messages(txn_id, topic, partition_no, sequence);

-- This table is the literal pending table.  A txn message is inserted here
-- before it becomes part of any deliverable sequence, and the row is removed
-- atomically with the final decision.
CREATE TABLE IF NOT EXISTS pending_messages (
    message_id INTEGER PRIMARY KEY,
    txn_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_no INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (message_id) REFERENCES messages(message_id) ON DELETE CASCADE,
    FOREIGN KEY (txn_id) REFERENCES transactions(txn_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_txn ON pending_messages(txn_id);

CREATE TABLE IF NOT EXISTS coordinator_cache (
    txn_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('COMMIT', 'ROLLBACK')),
    source TEXT NOT NULL,
    response_json TEXT NOT NULL,
    stored_at INTEGER NOT NULL,
    PRIMARY KEY (txn_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS txn_queries (
    query_id TEXT PRIMARY KEY,
    txn_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    requested_at INTEGER NOT NULL,
    finished_at INTEGER,
    outcome TEXT,
    result_code TEXT NOT NULL,
    source TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    UNIQUE (txn_id, idempotency_key, attempt)
);

CREATE INDEX IF NOT EXISTS idx_txn_queries_txn ON txn_queries(txn_id, attempt);

CREATE TABLE IF NOT EXISTS txn_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id TEXT NOT NULL,
    existing_outcome TEXT NOT NULL,
    incoming_outcome TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    source TEXT NOT NULL,
    detected_at INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    FOREIGN KEY (txn_id) REFERENCES transactions(txn_id)
);

CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_no INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    consumer_id TEXT NOT NULL,
    lease_token TEXT NOT NULL UNIQUE,
    attempt INTEGER NOT NULL,
    acquired_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'ACKED', 'EXPIRED', 'RELEASED', 'RECOVERED')),
    ended_at INTEGER,
    end_reason TEXT,
    FOREIGN KEY (topic, partition_no) REFERENCES partitions(topic, partition_no),
    FOREIGN KEY (topic, partition_no, sequence)
        REFERENCES messages(topic, partition_no, sequence)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_one_active_lease
    ON leases(topic, partition_no) WHERE state = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_leases_message
    ON leases(topic, partition_no, sequence, attempt);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('STARTED', 'COMMITTED', 'ABORTED')),
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER,
    note TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_unfinished ON intents(state, kind);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    topic TEXT,
    partition_no INTEGER,
    sequence INTEGER,
    txn_id TEXT,
    consumer_id TEXT,
    code TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_topic_partition
    ON audit_events(topic, partition_no, event_id);
CREATE INDEX IF NOT EXISTS idx_audit_txn ON audit_events(txn_id, event_id);

CREATE TABLE IF NOT EXISTS fault_events (
    fault_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id TEXT,
    injection TEXT NOT NULL,
    conditions_json TEXT NOT NULL,
    observed_code TEXT NOT NULL,
    observed_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    run_id TEXT PRIMARY KEY,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    starting_process TEXT,
    facts_json TEXT NOT NULL,
    note TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    intent_id TEXT,
    kind TEXT NOT NULL,
    action TEXT NOT NULL,
    before_json TEXT NOT NULL,
    after_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (run_id) REFERENCES recovery_runs(run_id)
);
"""


def json_text(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def connect(db_path: str | Path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(db_path), timeout=10.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def initialize(db_path: str | Path) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES('version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )
    finally:
        conn.close()


@contextmanager
def immediate(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


@contextmanager
def read_conn(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()

