"""SQLite connection management and the full relational schema.

WAL mode gives the single-writer / multi-reader concurrency model: readers
open short-lived read transactions against one committed snapshot while the
writer holds an exclusive write transaction.  Every table is append/version
oriented so recovery can reason about durable facts only.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topology_versions (
    version          INTEGER PRIMARY KEY,
    kind             TEXT NOT NULL CHECK (kind IN ('snapshot', 'increment')),
    checksum         TEXT NOT NULL,
    declared_checksum TEXT,
    based_on_version INTEGER,
    commit_id        TEXT,
    created_at_ms    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS services (
    name              TEXT PRIMARY KEY,
    first_version     INTEGER NOT NULL,
    latest_version    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS edges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    src             TEXT NOT NULL,
    dst             TEXT NOT NULL,
    added_version   INTEGER NOT NULL,
    removed_version INTEGER,
    UNIQUE (src, dst)
);
CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src);
CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges(dst);

CREATE TABLE IF NOT EXISTS sample_batches (
    commit_id       TEXT PRIMARY KEY,
    base_version    INTEGER NOT NULL,
    new_version     INTEGER NOT NULL,
    payload_hash    TEXT NOT NULL,
    record_count    INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    created_at_ms   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_version INTEGER NOT NULL,
    batch_commit  TEXT NOT NULL,
    ts_ms         INTEGER NOT NULL,
    service       TEXT NOT NULL,
    metric        TEXT NOT NULL,
    is_good       INTEGER NOT NULL CHECK (is_good IN (0, 1)),
    latency_us    INTEGER NOT NULL CHECK (latency_us >= 0),
    UNIQUE (service, metric, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_samples_lookup
    ON samples(service, metric, sample_version, ts_ms);

CREATE TABLE IF NOT EXISTS commits (
    commit_id     TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    payload_hash  TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('inflight', 'committed', 'aborted')),
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id       TEXT NOT NULL UNIQUE,
    kind            TEXT NOT NULL,
    commit_id       TEXT,
    detail_json     TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'done', 'aborted')),
    created_at_ms   INTEGER NOT NULL,
    finished_at_ms  INTEGER
);

CREATE TABLE IF NOT EXISTS quantile_index (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    service          TEXT NOT NULL,
    metric           TEXT NOT NULL,
    sample_lo        INTEGER NOT NULL,
    sample_hi        INTEGER NOT NULL,
    window_size      INTEGER NOT NULL,
    sample_count     INTEGER NOT NULL,
    good_rate        INTEGER NOT NULL,
    quantiles_json   TEXT NOT NULL,
    built_at_ms      INTEGER NOT NULL,
    UNIQUE (service, metric)
);

CREATE TABLE IF NOT EXISTS alerts (
    id            TEXT PRIMARY KEY,
    fingerprint   TEXT NOT NULL,
    service       TEXT NOT NULL,
    metric        TEXT NOT NULL,
    level         TEXT NOT NULL,
    rule          TEXT NOT NULL,
    status        TEXT NOT NULL,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms  INTEGER NOT NULL,
    sample_hi     INTEGER NOT NULL,
    UNIQUE (fingerprint, service, level)
);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);

CREATE TABLE IF NOT EXISTS alert_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id    TEXT NOT NULL,
    transition  TEXT NOT NULL,
    from_status TEXT,
    to_status   TEXT NOT NULL,
    at_ms       INTEGER NOT NULL,
    detail_json TEXT
);

CREATE TABLE IF NOT EXISTS suppression_index (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint     TEXT NOT NULL,
    service         TEXT NOT NULL,
    level           TEXT NOT NULL,
    alert_id        TEXT NOT NULL,
    suppressed_count INTEGER NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms   INTEGER NOT NULL,
    built_at_ms     INTEGER NOT NULL,
    UNIQUE (fingerprint, service, level)
);

CREATE TABLE IF NOT EXISTS rca_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id        TEXT NOT NULL,
    candidate       TEXT NOT NULL,
    hops            INTEGER NOT NULL,
    time_delta_ms   INTEGER NOT NULL,
    hop_score       INTEGER NOT NULL,
    time_score      INTEGER NOT NULL,
    score           INTEGER NOT NULL,
    rank            INTEGER NOT NULL,
    built_at_ms     INTEGER NOT NULL,
    UNIQUE (alert_id, candidate)
);
CREATE INDEX IF NOT EXISTS idx_rca_alert ON rca_scores(alert_id, rank);

CREATE TABLE IF NOT EXISTS service_refs (
    service       TEXT NOT NULL,
    artifact      TEXT NOT NULL CHECK (artifact IN ('quantile', 'suppression', 'rca')),
    artifact_id   TEXT NOT NULL,
    depends_on    TEXT,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (service, artifact, artifact_id)
);

CREATE TABLE IF NOT EXISTS recovery_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms         INTEGER NOT NULL,
    intent_id     TEXT,
    action        TEXT NOT NULL,
    detail_json   TEXT NOT NULL
);
"""


def connect(db_path: str | Path, *, readonly: bool = False) -> sqlite3.Connection:
    """Open a connection with the determinism/concurrency pragmas required."""
    path = Path(db_path)
    if not readonly:
        path.parent.mkdir(parents=True, exist_ok=True)
    uri = f"file:{path.resolve()}"
    if readonly:
        uri += "?mode=ro"
    conn = sqlite3.connect(
        uri,
        uri=True,
        timeout=30.0,
        isolation_level=None,  # explicit BEGIN/COMMIT, no implicit transactions
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def initialize(db_path: str | Path) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '1')"
        )
    finally:
        conn.close()


def get_meta(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
