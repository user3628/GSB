"""SQLite schema and low-level connection helpers."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .config import Settings

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS versions (
    version INTEGER PRIMARY KEY,
    committed_at TEXT NOT NULL,
    batch_id TEXT,
    accepted_events INTEGER NOT NULL,
    source TEXT
);

CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id TEXT PRIMARY KEY,
    payload_sha256 TEXT NOT NULL,
    version INTEGER NOT NULL,
    accepted_events INTEGER NOT NULL,
    response_json TEXT NOT NULL,
    committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT UNIQUE,
    service TEXT NOT NULL,
    sli_type TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    good INTEGER NOT NULL CHECK (good IN (0, 1)),
    bucket_start_ms INTEGER NOT NULL,
    batch_id TEXT,
    version INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_window
ON events(service, sli_type, timestamp_ms, version);
CREATE INDEX IF NOT EXISTS idx_events_version ON events(version);

CREATE TABLE IF NOT EXISTS bucket_versions (
    version INTEGER NOT NULL,
    service TEXT NOT NULL,
    sli_type TEXT NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    good_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('OPEN', 'CLOSED')),
    slo_target REAL NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (service, sli_type, bucket_start_ms, version)
);

CREATE INDEX IF NOT EXISTS idx_bucket_window
ON bucket_versions(service, sli_type, version, bucket_start_ms);
CREATE INDEX IF NOT EXISTS idx_bucket_scan
ON bucket_versions(service, version, bucket_start_ms);

CREATE TABLE IF NOT EXISTS closed_budget_snapshots (
    service TEXT NOT NULL,
    sli_type TEXT NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    closed_version INTEGER NOT NULL,
    good_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL,
    slo_target REAL NOT NULL,
    allowed_bad_count REAL NOT NULL,
    bad_count INTEGER NOT NULL,
    consumption_ratio REAL NOT NULL,
    closed_at TEXT NOT NULL,
    PRIMARY KEY (service, sli_type, bucket_start_ms)
);

CREATE TABLE IF NOT EXISTS service_state (
    service TEXT PRIMARY KEY,
    watermark_ms INTEGER NOT NULL,
    last_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS service_state_versions (
    version INTEGER NOT NULL,
    service TEXT NOT NULL,
    watermark_ms INTEGER NOT NULL,
    PRIMARY KEY (version, service)
);

CREATE INDEX IF NOT EXISTS idx_service_state_versions
ON service_state_versions(service, version DESC);

CREATE TABLE IF NOT EXISTS alerts_history (
    version INTEGER NOT NULL,
    service TEXT NOT NULL,
    sli_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OK', 'WARNING', 'CRITICAL')),
    evaluation_status TEXT NOT NULL,
    as_of_ms INTEGER NOT NULL,
    short_window_seconds INTEGER NOT NULL,
    long_window_seconds INTEGER NOT NULL,
    short_sli REAL,
    long_sli REAL,
    short_burn_rate REAL,
    long_burn_rate REAL,
    short_sample_count INTEGER NOT NULL,
    long_sample_count INTEGER NOT NULL,
    details_json TEXT NOT NULL,
    PRIMARY KEY (version, service, sli_type)
);

CREATE INDEX IF NOT EXISTS idx_alerts_service
ON alerts_history(service, version DESC);

CREATE TABLE IF NOT EXISTS write_intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    service TEXT,
    batch_id TEXT,
    payload_sha256 TEXT,
    target_version INTEGER,
    accepted_events INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL CHECK (state IN ('RUNNING', 'COMPLETE', 'ABANDONED', 'FAILED')),
    created_at TEXT NOT NULL,
    finished_at TEXT,
    details_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_write_intents_state
ON write_intents(state, created_at);

CREATE TABLE IF NOT EXISTS rejected_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    service TEXT NOT NULL,
    sli_type TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    reason TEXT NOT NULL,
    watermark_ms INTEGER,
    batch_id TEXT,
    rejected_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rejected_batch ON rejected_events(batch_id);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    db_path TEXT NOT NULL,
    last_complete_version INTEGER NOT NULL,
    intents_scanned INTEGER NOT NULL,
    intents_abandoned INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


def connect(settings: Settings, *, readonly: bool = False) -> sqlite3.Connection:
    """Open a connection configured for WAL reads/writes."""

    path = Path(settings.db_path)
    connection = sqlite3.connect(
        str(path),
        timeout=settings.sqlite_busy_timeout_ms / 1000,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout={settings.sqlite_busy_timeout_ms}")
    connection.execute("PRAGMA foreign_keys=ON")
    if readonly:
        connection.execute("PRAGMA query_only=ON")
    else:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
    return connection


def initialize_database(settings: Settings) -> None:
    Path(settings.data_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(settings)
    try:
        connection.executescript(SCHEMA_SQL)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )
        connection.commit()
    finally:
        connection.close()


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def loads(value: str | bytes | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def begin_read(connection: sqlite3.Connection) -> None:
    connection.execute("BEGIN")


def latest_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COALESCE(MAX(version), 0) AS v FROM versions").fetchone()
    return int(row["v"])
