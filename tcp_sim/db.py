from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
    config_json TEXT NOT NULL,
    config_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    base_checkpoint_id TEXT NOT NULL,
    last_checkpoint_id TEXT NOT NULL,
    last_event_index INTEGER NOT NULL,
    last_ts_ms INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    expected_base_checkpoint_id TEXT,
    final_batch INTEGER NOT NULL,
    events_json TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    state TEXT NOT NULL,
    processed_count INTEGER NOT NULL DEFAULT 0,
    base_checkpoint_id TEXT,
    result_json TEXT,
    error_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_batches_run ON batches(run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);

CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_index INTEGER NOT NULL,
    sim_time_ms INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    state_sha256 TEXT NOT NULL,
    batch_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_run_event
    ON checkpoints(run_id, event_index);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    checkpoint_id TEXT NOT NULL,
    event_index INTEGER NOT NULL,
    sim_time_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    batch_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, event_index)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_run_time
    ON snapshots(run_id, sim_time_ms, event_index);

CREATE TABLE IF NOT EXISTS intent_log (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    batch_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_batch ON intent_log(batch_id, status);
CREATE INDEX IF NOT EXISTS idx_intents_run ON intent_log(run_id, intent_id);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        conn = sqlite3.connect(str(self.path), timeout=15.0, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=15000")
            conn.execute("PRAGMA journal_mode=WAL")
            # NORMAL is sufficient for process-crash recovery (the required
            # failure mode): committed WAL frames remain in the OS page cache.
            # It is not a promise of power-loss durability, which is outside this
            # local adjudication workbench's contract.
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(SCHEMA_SQL)
        finally:
            conn.close()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.path),
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
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
    def transaction_on(
        self,
        conn: sqlite3.Connection,
        *,
        immediate: bool = True,
    ) -> Iterator[sqlite3.Connection]:
        try:
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
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

    def fetch_one(self, sql: str, params: tuple[object, ...] = ()) -> sqlite3.Row | None:
        conn = self.connect()
        try:
            return conn.execute(sql, params).fetchone()
        finally:
            conn.close()

    def fetch_all(self, sql: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        conn = self.connect()
        try:
            return list(conn.execute(sql, params).fetchall())
        finally:
            conn.close()
