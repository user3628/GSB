from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS connections (
    connection_id TEXT PRIMARY KEY,
    initial_connection_window INTEGER NOT NULL,
    connection_window INTEGER NOT NULL,
    initial_stream_window INTEGER NOT NULL,
    closed INTEGER NOT NULL DEFAULT 0,
    config_json TEXT NOT NULL,
    protocol_violations INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS streams (
    connection_id TEXT NOT NULL,
    stream_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    initial_window INTEGER NOT NULL,
    window INTEGER NOT NULL,
    sent_unacked INTEGER NOT NULL DEFAULT 0,
    pending_bytes INTEGER NOT NULL DEFAULT 0,
    weight INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    sent_total INTEGER NOT NULL DEFAULT 0,
    pending_end_stream INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (connection_id, stream_id),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    frame_count INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    status TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_samples_connection ON samples(connection_id, created_at);

CREATE TABLE IF NOT EXISTS data_jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    sample_id TEXT NOT NULL,
    frame_index INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    original_bytes INTEGER NOT NULL,
    remaining_bytes INTEGER NOT NULL,
    end_stream INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(sample_id, frame_index),
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_jobs_pending
    ON data_jobs(connection_id, status, stream_id, job_id);

CREATE TABLE IF NOT EXISTS schedule_runs (
    run_id TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL,
    sample_id TEXT,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT,
    error_json TEXT,
    round_count INTEGER NOT NULL DEFAULT 0,
    selected_bytes INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    FOREIGN KEY (connection_id) REFERENCES connections(connection_id) ON DELETE CASCADE,
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_sample_schedule
    ON schedule_runs(sample_id) WHERE sample_id IS NOT NULL AND mode = 'sample';
CREATE INDEX IF NOT EXISTS ix_runs_connection ON schedule_runs(connection_id, started_at);

CREATE TABLE IF NOT EXISTS schedule_rounds (
    run_id TEXT NOT NULL,
    round_index INTEGER NOT NULL,
    algorithm TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    selected_candidate_count INTEGER NOT NULL,
    truncated_count INTEGER NOT NULL,
    approximation_json TEXT NOT NULL,
    selected_bytes INTEGER NOT NULL,
    connection_window_before INTEGER NOT NULL,
    connection_window_after INTEGER NOT NULL,
    selected_json TEXT NOT NULL,
    blocked_json TEXT NOT NULL,
    final_status TEXT,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (run_id, round_index),
    FOREIGN KEY (run_id) REFERENCES schedule_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trace_entries (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    round_index INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    connection_id TEXT NOT NULL,
    sample_id TEXT,
    stream_id INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    weight INTEGER NOT NULL,
    reason TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    state_before TEXT NOT NULL,
    state_after TEXT NOT NULL,
    job_consumption_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES schedule_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_trace_connection ON trace_entries(connection_id, run_id, round_index, seq);
CREATE INDEX IF NOT EXISTS ix_trace_stream ON trace_entries(connection_id, stream_id, run_id, round_index, seq);

CREATE TABLE IF NOT EXISTS blocked_entries (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    round_index INTEGER NOT NULL,
    connection_id TEXT NOT NULL,
    stream_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    required_bytes INTEGER NOT NULL,
    pending_bytes INTEGER NOT NULL,
    connection_window INTEGER NOT NULL,
    stream_window INTEGER NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES schedule_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_blocked_run ON blocked_entries(run_id, round_index, stream_id);

CREATE TABLE IF NOT EXISTS state_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    stream_id INTEGER NOT NULL,
    sample_id TEXT,
    run_id TEXT,
    frame_index INTEGER,
    round_index INTEGER,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    trigger TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_transitions_connection ON state_transitions(connection_id, transition_id);

CREATE TABLE IF NOT EXISTS protocol_violations (
    violation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    stream_id INTEGER NOT NULL,
    sample_id TEXT,
    frame_index INTEGER,
    code TEXT NOT NULL,
    requested_increment INTEGER NOT NULL,
    applied_increment INTEGER NOT NULL,
    current_before INTEGER NOT NULL,
    initial_limit INTEGER NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_violations_connection ON protocol_violations(connection_id, violation_id);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    connection_id TEXT,
    sample_id TEXT,
    run_id TEXT,
    round_index INTEGER,
    status TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    committed_at TEXT,
    recovered_at TEXT,
    failure_reason TEXT
);

CREATE INDEX IF NOT EXISTS ix_intents_status ON intents(status, created_at);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT,
    run_id TEXT,
    intent_id TEXT,
    action TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_recovery_connection ON recovery_reports(connection_id, report_id);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def json_loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


class Database:
    def __init__(self, path: str | Path):
        self.path = str(Path(path).resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    def initialize(self) -> None:
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
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
    def snapshot(self) -> Iterator[sqlite3.Connection]:
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

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        conn = self.connect()
        try:
            return conn.execute(sql, params).fetchone()
        finally:
            conn.close()

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        conn = self.connect()
        try:
            return list(conn.execute(sql, params).fetchall())
        finally:
            conn.close()
