from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = 2

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO runtime_meta(key, value) VALUES ('commit_seq', '0');

CREATE TABLE IF NOT EXISTS spec_batches (
    batch_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    seed INTEGER,
    function_count INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS function_specs (
    function_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    runtime TEXT NOT NULL,
    init_us INTEGER NOT NULL CHECK(init_us >= 0),
    memory_bytes INTEGER NOT NULL CHECK(memory_bytes >= 0),
    per_instance_concurrency INTEGER NOT NULL CHECK(per_instance_concurrency > 0),
    function_concurrency_limit INTEGER NOT NULL CHECK(function_concurrency_limit > 0),
    reserved_quota INTEGER NOT NULL CHECK(reserved_quota >= 0),
    init_concurrency INTEGER NOT NULL CHECK(init_concurrency > 0),
    prewarm_instances INTEGER NOT NULL CHECK(prewarm_instances >= 0),
    queue_capacity INTEGER NOT NULL CHECK(queue_capacity >= 0),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(function_id, version),
    FOREIGN KEY(batch_id) REFERENCES spec_batches(batch_id)
);

CREATE INDEX IF NOT EXISTS idx_function_specs_active
    ON function_specs(function_id, active);

CREATE TABLE IF NOT EXISTS function_runtime (
    function_id TEXT PRIMARY KEY,
    spec_version INTEGER NOT NULL,
    active_instances INTEGER NOT NULL DEFAULT 0,
    available_instances INTEGER NOT NULL DEFAULT 0,
    cold_starts INTEGER NOT NULL DEFAULT 0,
    warm_starts INTEGER NOT NULL DEFAULT 0,
    start_timeouts INTEGER NOT NULL DEFAULT 0,
    exec_timeouts INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS quota_usage (
    function_id TEXT PRIMARY KEY,
    spec_version INTEGER NOT NULL,
    on_demand_used INTEGER NOT NULL DEFAULT 0,
    reserved_used INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS instances (
    instance_id TEXT PRIMARY KEY,
    function_id TEXT NOT NULL,
    spec_version INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(
        state IN ('CREATING', 'READY', 'BUSY', 'DRAINING', 'TERMINATED')
    ),
    capacity INTEGER NOT NULL CHECK(capacity > 0),
    occupied_slots INTEGER NOT NULL DEFAULT 0 CHECK(occupied_slots >= 0),
    last_active_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    drain_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_instances_function_state
    ON instances(function_id, state, occupied_slots);

CREATE TABLE IF NOT EXISTS calls (
    call_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    function_id TEXT NOT NULL,
    spec_version INTEGER NOT NULL,
    arrival_ms INTEGER NOT NULL,
    execution_us INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    retry INTEGER NOT NULL CHECK(retry IN (0, 1)),
    quota_class TEXT NOT NULL,
    quota_bucket TEXT,
    status TEXT NOT NULL,
    is_cold_start INTEGER,
    instance_id TEXT,
    created_seq INTEGER NOT NULL,
    injection_json TEXT NOT NULL DEFAULT '{}',
    request_json TEXT NOT NULL,
    business_code TEXT,
    message TEXT,
    detail_json TEXT,
    result_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    admitted_at_ms INTEGER,
    started_at_ms INTEGER,
    completed_at_ms INTEGER,
    FOREIGN KEY(instance_id) REFERENCES instances(instance_id)
);

CREATE INDEX IF NOT EXISTS idx_calls_queue
    ON calls(function_id, status, priority DESC, retry DESC, arrival_ms ASC, created_seq ASC);
CREATE INDEX IF NOT EXISTS idx_calls_function_status
    ON calls(function_id, status);
CREATE INDEX IF NOT EXISTS idx_calls_instance
    ON calls(instance_id, status);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'RESCUED')),
    function_id TEXT,
    call_id TEXT,
    instance_id TEXT,
    payload_json TEXT NOT NULL,
    error_code TEXT,
    error_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_pending
    ON intents(status, created_at_ms);

CREATE TABLE IF NOT EXISTS trace_batches (
    batch_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    seed INTEGER,
    strict_order INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS trace_records (
    batch_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    call_id TEXT NOT NULL,
    function_id TEXT NOT NULL,
    arrival_ms INTEGER NOT NULL,
    execution_us INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    retry INTEGER NOT NULL CHECK(retry IN (0, 1)),
    PRIMARY KEY(batch_id, sequence),
    UNIQUE(batch_id, call_id),
    FOREIGN KEY(batch_id) REFERENCES trace_batches(batch_id)
);

CREATE TABLE IF NOT EXISTS injection_events (
    injection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    params_json TEXT NOT NULL,
    status TEXT NOT NULL,
    result_code TEXT,
    observation_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL,
    persisted_facts_only INTEGER NOT NULL CHECK(persisted_facts_only IN (0, 1)),
    report_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    level TEXT NOT NULL,
    event TEXT NOT NULL,
    fields_json TEXT NOT NULL
);
"""


def _configure(connection: sqlite3.Connection, busy_timeout_s: float) -> None:
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_s * 1000)}")


def open_read_connection(path: Path, busy_timeout_s: float = 10.0) -> sqlite3.Connection:
    connection = sqlite3.connect(
        path,
        timeout=busy_timeout_s,
        isolation_level=None,
        check_same_thread=False,
    )
    _configure(connection, busy_timeout_s)
    return connection


def initialize_database(path: Path, busy_timeout_s: float = 10.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        path,
        timeout=busy_timeout_s,
        isolation_level=None,
        check_same_thread=False,
    )
    try:
        _configure(connection, busy_timeout_s)
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.executescript(SCHEMA_SQL)
        call_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(calls)").fetchall()
        }
        if "updated_at_ms" not in call_columns:
            connection.execute(
                "ALTER TABLE calls ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0"
            )
            connection.execute("UPDATE calls SET updated_at_ms = created_at_ms")
        connection.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
    finally:
        connection.close()


@contextmanager
def write_transaction(
    path: Path, busy_timeout_s: float = 10.0
) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(
        path,
        timeout=busy_timeout_s,
        isolation_level=None,
        check_same_thread=False,
    )
    _configure(connection, busy_timeout_s)
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


@contextmanager
def read_transaction(
    path: Path, busy_timeout_s: float = 10.0
) -> Iterator[sqlite3.Connection]:
    connection = open_read_connection(path, busy_timeout_s)
    try:
        connection.execute("BEGIN")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def bump_commit_seq(connection: sqlite3.Connection) -> int:
    connection.execute(
        "UPDATE runtime_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
        "WHERE key = 'commit_seq'"
    )
    row = connection.execute(
        "SELECT CAST(value AS INTEGER) AS seq FROM runtime_meta WHERE key = 'commit_seq'"
    ).fetchone()
    return int(row["seq"])


def current_commit_seq(path: Path, busy_timeout_s: float = 10.0) -> int:
    connection = open_read_connection(path, busy_timeout_s)
    try:
        row = connection.execute(
            "SELECT CAST(value AS INTEGER) AS seq FROM runtime_meta WHERE key = 'commit_seq'"
        ).fetchone()
        return int(row["seq"]) if row else 0
    finally:
        connection.close()


def log_event(
    path: Path,
    event: str,
    fields_json: str,
    *,
    level: str = "INFO",
    at_ms: int,
    busy_timeout_s: float = 10.0,
) -> None:
    connection = sqlite3.connect(
        path,
        timeout=busy_timeout_s,
        isolation_level=None,
        check_same_thread=False,
    )
    _configure(connection, busy_timeout_s)
    try:
        connection.execute(
            "INSERT INTO operation_log(at_ms, level, event, fields_json) VALUES (?, ?, ?, ?)",
            (at_ms, level, event, fields_json),
        )
    finally:
        connection.close()

