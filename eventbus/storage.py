from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    topic_id TEXT PRIMARY KEY,
    partitions INTEGER NOT NULL CHECK (partitions > 0),
    group_id TEXT NOT NULL,
    min_retained_seq INTEGER NOT NULL CHECK (min_retained_seq > 0),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
    topic_id TEXT NOT NULL REFERENCES topics(topic_id),
    partition INTEGER NOT NULL,
    next_seq INTEGER NOT NULL,
    min_retained_seq INTEGER NOT NULL,
    PRIMARY KEY (topic_id, partition)
);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic_id TEXT NOT NULL REFERENCES topics(topic_id),
    group_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    next_seq INTEGER NOT NULL,
    replaying INTEGER NOT NULL DEFAULT 0,
    replay_target INTEGER,
    replay_end INTEGER,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (topic_id, group_id, partition)
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    topic_id TEXT NOT NULL REFERENCES topics(topic_id),
    partition INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    production_seq INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    body_digest TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    event_key TEXT NOT NULL,
    measure REAL,
    payload_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (topic_id, partition, production_seq)
);
CREATE INDEX IF NOT EXISTS idx_events_topic_partition_seq
    ON events(topic_id, partition, production_seq);
CREATE INDEX IF NOT EXISTS idx_events_topic_state ON events(topic_id, state);

CREATE TABLE IF NOT EXISTS event_state_trace (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    delivery_id INTEGER,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trace_event ON event_state_trace(event_id, id);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    consumer_id TEXT NOT NULL,
    lease_token TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    dispatched_at TEXT NOT NULL,
    lease_expires_epoch REAL NOT NULL,
    lease_expires_at TEXT NOT NULL,
    ended_at TEXT,
    end_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_delivery_partition
    ON deliveries(topic_id, group_id, partition)
    WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_delivery_event ON deliveries(event_id, id);

CREATE TABLE IF NOT EXISTS confirmations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    delivery_id INTEGER NOT NULL REFERENCES deliveries(id),
    topic_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    production_seq INTEGER NOT NULL,
    consumer_id TEXT NOT NULL,
    status TEXT NOT NULL,
    acked_at TEXT NOT NULL,
    rolled_back_at TEXT,
    intent_id TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_confirmation_event
    ON confirmations(event_id)
    WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_confirmation_scope
    ON confirmations(topic_id, group_id, partition, status, production_seq);

CREATE TABLE IF NOT EXISTS aggregate_state (
    topic_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    event_key TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    measure_sum REAL NOT NULL,
    compensation REAL NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (topic_id, group_id, event_key)
);

CREATE TABLE IF NOT EXISTS offset_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    old_seq INTEGER NOT NULL,
    new_seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_offset_history_scope
    ON offset_history(topic_id, group_id, partition, id);

CREATE TABLE IF NOT EXISTS ingest_batches (
    internal_id TEXT PRIMARY KEY,
    topic_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    accepted_count INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    topic_id TEXT,
    group_id TEXT,
    partition INTEGER,
    payload_json TEXT NOT NULL,
    precondition_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_pending ON intents(status, created_at);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(SCHEMA)
            connection.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES ('schema_version', '1')"
            )

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
