from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_snapshots (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    checksum TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shards (
    shard_id TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    heat INTEGER NOT NULL CHECK (heat BETWEEN 0 AND 255),
    layer_id TEXT NOT NULL,
    cooldown_until TEXT NOT NULL,
    last_batch_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shards_layer ON shards(layer_id);

CREATE TABLE IF NOT EXISTS layer_state (
    layer_id TEXT PRIMARY KEY,
    rank INTEGER NOT NULL,
    capacity_bytes INTEGER NOT NULL,
    used_bytes INTEGER NOT NULL,
    shard_count INTEGER NOT NULL,
    hot_shards INTEGER NOT NULL,
    hot_bytes INTEGER NOT NULL,
    cold_shards INTEGER NOT NULL,
    cold_bytes INTEGER NOT NULL,
    eligible_shards INTEGER NOT NULL,
    eligible_bytes INTEGER NOT NULL,
    heat_histogram_json TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_runtime (
    snapshot_version INTEGER PRIMARY KEY,
    cascade_generation INTEGER NOT NULL,
    eligible_shards INTEGER NOT NULL,
    eligible_bytes INTEGER NOT NULL,
    candidate_rule TEXT NOT NULL,
    computed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shard_policy_state (
    shard_id TEXT PRIMARY KEY,
    snapshot_version INTEGER NOT NULL,
    cascade_generation INTEGER NOT NULL,
    candidate_eligible INTEGER NOT NULL,
    target_layer_id TEXT,
    cooldown_eligible INTEGER NOT NULL,
    heat_class TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    FOREIGN KEY (shard_id) REFERENCES shards(shard_id)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    cascade_generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    phase TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    plan_json TEXT NOT NULL,
    result_json TEXT,
    error_json TEXT,
    committed_epoch INTEGER,
    recovery_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);

CREATE TABLE IF NOT EXISTS batch_items (
    batch_id TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    source_layer_id TEXT NOT NULL,
    target_layer_id TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    heat INTEGER NOT NULL,
    state TEXT NOT NULL,
    last_changed_at TEXT NOT NULL,
    reason TEXT,
    source_release_bytes INTEGER NOT NULL DEFAULT 0,
    target_alloc_bytes INTEGER NOT NULL DEFAULT 0,
    sequence INTEGER NOT NULL,
    PRIMARY KEY (batch_id, shard_id),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE,
    FOREIGN KEY (shard_id) REFERENCES shards(shard_id)
);

CREATE INDEX IF NOT EXISTS idx_batch_items_batch ON batch_items(batch_id);

CREATE TABLE IF NOT EXISTS transition_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    at TEXT NOT NULL,
    reason TEXT NOT NULL,
    persisted_phase TEXT NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_transition_batch ON transition_events(batch_id, event_id);

CREATE TABLE IF NOT EXISTS migration_intents (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    cascade_generation INTEGER NOT NULL,
    source_layer_id TEXT NOT NULL,
    target_layer_id TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    UNIQUE (batch_id, shard_id),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_intents_state ON migration_intents(state);

CREATE TABLE IF NOT EXISTS lifecycle_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pid INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,
    safe_summary_json TEXT,
    recovery_report_id TEXT
);

CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    report_type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    forced_termination_detected INTEGER NOT NULL,
    run_id INTEGER,
    report_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reports_type ON reports(report_type, created_at);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.writer_lock = threading.RLock()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path),
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(SCHEMA_SQL)
            connection.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', '2') "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
            )
        finally:
            connection.close()

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        with self.writer_lock:
            connection = self.connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.execute("COMMIT")
            except BaseException:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                connection.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    def close(self) -> None:
        """No persistent connection is kept; retained for lifecycle symmetry."""


def remove_database(path: str | Path) -> None:
    db_path = Path(path)
    for candidate in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass
