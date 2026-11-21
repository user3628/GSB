from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    current_version INTEGER NOT NULL,
    last_snapshot_seq INTEGER NOT NULL,
    last_completed_intent_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoint_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    latest_commit_id TEXT,
    latest_seq INTEGER NOT NULL,
    last_completed_intent_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
    policy_version INTEGER PRIMARY KEY,
    format_version INTEGER NOT NULL,
    origin TEXT NOT NULL,
    base_version INTEGER,
    loss_scale REAL NOT NULL,
    loss_scale_initial REAL NOT NULL,
    growth_factor REAL NOT NULL,
    commit_id TEXT UNIQUE,
    request_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_precisions (
    policy_version INTEGER NOT NULL REFERENCES policies(policy_version) ON DELETE CASCADE,
    shard_id TEXT NOT NULL,
    precision TEXT NOT NULL CHECK (precision IN ('FP16', 'FP32')),
    PRIMARY KEY (policy_version, shard_id)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    commit_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL UNIQUE,
    round_number INTEGER NOT NULL,
    scheduler_position_json TEXT NOT NULL,
    data_cursor_json TEXT NOT NULL,
    loss_scale_at_commit REAL NOT NULL,
    policy_version_at_commit INTEGER NOT NULL REFERENCES policies(policy_version),
    manifest_digest TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoint_shards (
    commit_id TEXT NOT NULL REFERENCES checkpoints(commit_id) ON DELETE CASCADE,
    shard_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    shape_json TEXT NOT NULL,
    numel INTEGER NOT NULL,
    fp16_blob BLOB NOT NULL,
    master_blob BLOB NOT NULL,
    momentum_blob BLOB NOT NULL,
    fp16_digest TEXT NOT NULL,
    master_digest TEXT NOT NULL,
    momentum_digest TEXT NOT NULL,
    precision_at_commit TEXT NOT NULL CHECK (precision_at_commit IN ('FP16', 'FP32')),
    source_file TEXT NOT NULL,
    PRIMARY KEY (commit_id, shard_id)
);

CREATE TABLE IF NOT EXISTS derived_shards (
    commit_id TEXT NOT NULL REFERENCES checkpoints(commit_id) ON DELETE CASCADE,
    policy_version INTEGER NOT NULL REFERENCES policies(policy_version),
    shard_id TEXT NOT NULL,
    precision TEXT NOT NULL CHECK (precision IN ('FP16', 'FP32')),
    loss_scale REAL NOT NULL,
    restore_digest TEXT NOT NULL,
    summary_digest TEXT NOT NULL,
    dependency_fingerprint TEXT NOT NULL,
    consistency_ok INTEGER NOT NULL,
    consistency_reason TEXT NOT NULL,
    max_abs REAL NOT NULL,
    max_rel REAL NOT NULL,
    mean_abs REAL NOT NULL,
    cumulative_rounding_error REAL NOT NULL,
    memory_bytes INTEGER NOT NULL,
    recovery_baseline_digest TEXT NOT NULL,
    details_json TEXT NOT NULL,
    invalidated INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (commit_id, policy_version, shard_id)
);

CREATE TABLE IF NOT EXISTS derived_aggregates (
    commit_id TEXT NOT NULL REFERENCES checkpoints(commit_id) ON DELETE CASCADE,
    policy_version INTEGER NOT NULL REFERENCES policies(policy_version),
    shard_count INTEGER NOT NULL,
    restore_digest TEXT NOT NULL,
    summary_digest TEXT NOT NULL,
    consistency_ok INTEGER NOT NULL,
    max_abs REAL NOT NULL,
    max_rel REAL NOT NULL,
    cumulative_rounding_error REAL NOT NULL,
    memory_bytes INTEGER NOT NULL,
    recovery_baseline_digest TEXT NOT NULL,
    dependency_fingerprint TEXT NOT NULL,
    invalidated INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (commit_id, policy_version)
);

CREATE TABLE IF NOT EXISTS dependency_edges (
    commit_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    shard_id TEXT NOT NULL,
    derived_kind TEXT NOT NULL,
    dependency_key TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    PRIMARY KEY (commit_id, policy_version, shard_id, derived_kind, dependency_key),
    FOREIGN KEY (commit_id, policy_version) REFERENCES derived_aggregates(commit_id, policy_version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_invalidations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT,
    commit_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    shard_id TEXT,
    target_kind TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    old_policy_version INTEGER,
    new_policy_version INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'aborted')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    error_json TEXT,
    resolution_json TEXT
);

CREATE TABLE IF NOT EXISTS commit_operations (
    commit_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_snapshot_files (
    snapshot_seq INTEGER PRIMARY KEY,
    policy_version INTEGER NOT NULL REFERENCES policies(policy_version),
    format_version INTEGER NOT NULL,
    generated_at TEXT NOT NULL,
    path TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    loaded_at TEXT NOT NULL,
    resource_released_at TEXT
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    scanned_pending INTEGER NOT NULL,
    completed_count INTEGER NOT NULL,
    aborted_count INTEGER NOT NULL,
    last_complete_checkpoint_version INTEGER NOT NULL,
    current_policy_version INTEGER NOT NULL,
    scope TEXT NOT NULL,
    guarantees_complete_exit_report INTEGER NOT NULL,
    facts_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_derived_shards_policy ON derived_shards(policy_version, commit_id);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_invalidations_intent ON derived_invalidations(intent_id);
"""


class Database:
    """SQLite access with one in-process writer and arbitrary concurrent readers."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._initialize()

    def _connect(self, *, query_only: bool = False) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        if query_only:
            connection.execute("PRAGMA query_only = ON")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(SCHEMA)
            from .util import utc_now

            now = utc_now()
            connection.execute(
                "INSERT OR IGNORE INTO policy_state(singleton, current_version, last_snapshot_seq, updated_at) VALUES (1, 0, 0, ?)",
                (now,),
            )
            connection.execute(
                "INSERT OR IGNORE INTO checkpoint_state(singleton, latest_seq, updated_at) VALUES (1, 0, ?)",
                (now,),
            )
        finally:
            connection.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            connection = self._connect()
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
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect(query_only=True)
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

    def fetch_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self.read_transaction() as connection:
            return connection.execute(sql, params).fetchone()

    def fetch_all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self.read_transaction() as connection:
            return list(connection.execute(sql, params).fetchall())
