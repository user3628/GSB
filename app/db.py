from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS policies (
    version INTEGER PRIMARY KEY,
    generated_at_ms INTEGER NOT NULL,
    digest TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    service TEXT NOT NULL,
    change_type TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    baseline_json TEXT NOT NULL,
    baseline_sample_count INTEGER NOT NULL,
    baseline_success_count INTEGER NOT NULL,
    baseline_p90_micros INTEGER NOT NULL,
    started_at_ms INTEGER NOT NULL,
    policy_version INTEGER NOT NULL REFERENCES policies(version),
    window_ms INTEGER NOT NULL,
    late_tolerance_ms INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('OBSERVING','PASSED','ROLLING_BACK','ROLLED_BACK','ESCALATED')),
    window_epoch INTEGER NOT NULL DEFAULT 0,
    watermark_ms INTEGER,
    row_version INTEGER NOT NULL DEFAULT 1,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_service ON batches(service);
CREATE INDEX IF NOT EXISTS idx_batches_policy ON batches(policy_version);

CREATE TABLE IF NOT EXISTS windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    window_index INTEGER NOT NULL,
    policy_version INTEGER NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL CHECK(state IN ('OPEN','CLOSED')),
    sample_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    latency_sum_micros INTEGER NOT NULL DEFAULT 0,
    watermark_ms INTEGER,
    closed_at_ms INTEGER,
    UNIQUE(batch_id, window_index)
);
CREATE INDEX IF NOT EXISTS idx_windows_batch ON windows(batch_id, window_index);

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    sequence INTEGER NOT NULL,
    metric_type TEXT NOT NULL CHECK(metric_type IN ('CANDIDATE')),
    timestamp_ms INTEGER NOT NULL,
    window_index INTEGER NOT NULL,
    latency_micros INTEGER NOT NULL,
    success INTEGER NOT NULL CHECK(success IN (0,1)),
    received_at_ms INTEGER NOT NULL,
    UNIQUE(batch_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_observations_window
    ON observations(batch_id, window_index, sequence);

CREATE TABLE IF NOT EXISTS cooldowns (
    service TEXT PRIMARY KEY,
    cooldown_until_ms INTEGER NOT NULL,
    source_batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    policy_version INTEGER NOT NULL REFERENCES policies(version),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    mode TEXT NOT NULL CHECK(mode IN ('FAST','FULL')),
    policy_version INTEGER NOT NULL REFERENCES policies(version),
    window_index INTEGER,
    window_version INTEGER,
    window_epoch INTEGER NOT NULL,
    decision_time_ms INTEGER NOT NULL,
    risk_score_micros INTEGER NOT NULL,
    risk_score TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    action TEXT NOT NULL,
    applied INTEGER NOT NULL CHECK(applied IN (0,1)),
    details_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_batch ON decisions(batch_id, created_at_ms DESC);

CREATE TABLE IF NOT EXISTS batch_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    from_status TEXT,
    to_status TEXT NOT NULL,
    action TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    window_index INTEGER,
    window_version INTEGER,
    window_epoch INTEGER NOT NULL,
    risk_score TEXT NOT NULL,
    decision_id TEXT,
    at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transitions_batch ON batch_transitions(batch_id, id);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING','COMMITTED','ABORTED')),
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    error_json TEXT,
    recovery_run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at_ms);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    started_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER NOT NULL,
    pending_count INTEGER NOT NULL,
    aborted_count INTEGER NOT NULL,
    reconciled_count INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path),
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    def initialize(self) -> None:
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(SCHEMA)
        finally:
            connection.close()

    @contextmanager
    def transaction(
        self, *, immediate: bool = True
    ) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
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
    def read_snapshot(self) -> Iterator[sqlite3.Connection]:
        with self.transaction(immediate=False) as connection:
            yield connection
