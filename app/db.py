from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    component_count INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('committed', 'aborted')),
    committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS current_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_version INTEGER,
    updated_at TEXT NOT NULL
);

INSERT OR IGNORE INTO current_state (id, current_version, updated_at)
VALUES (1, NULL, '1970-01-01T00:00:00Z');

CREATE TABLE IF NOT EXISTS derived_metrics (
    version INTEGER NOT NULL REFERENCES snapshots(version),
    component_id TEXT NOT NULL,
    quota_share_micros INTEGER NOT NULL,
    headroom_ratio_micros INTEGER NOT NULL,
    normalized_weight_micros INTEGER NOT NULL,
    quota_share_error_micros INTEGER NOT NULL,
    headroom_error_micros INTEGER NOT NULL,
    metric_json TEXT NOT NULL,
    PRIMARY KEY (version, component_id)
);

CREATE TABLE IF NOT EXISTS consistency_results (
    version INTEGER PRIMARY KEY REFERENCES snapshots(version),
    status TEXT NOT NULL,
    weighted_consistency_micros INTEGER NOT NULL,
    weighted_error_micros INTEGER NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS error_explanations (
    version INTEGER PRIMARY KEY REFERENCES snapshots(version),
    explanation_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commit_responses (
    version INTEGER PRIMARY KEY REFERENCES snapshots(version),
    response_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    operation TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('started', 'committed', 'aborted', 'recovered_aborted')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, id);
CREATE INDEX IF NOT EXISTS idx_intents_version ON intents(version, id);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def connect(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def initialize_database(path: Path | str) -> None:
    connection = connect(path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.executescript(SCHEMA)
    finally:
        connection.close()


def get_current_version(connection: sqlite3.Connection) -> int | None:
    row = connection.execute("SELECT current_version FROM current_state WHERE id = 1").fetchone()
    return None if row is None else row["current_version"]


def get_snapshot_row(connection: sqlite3.Connection, version: int) -> sqlite3.Row | None:
    return connection.execute("SELECT * FROM snapshots WHERE version = ?", (version,)).fetchone()


def snapshot_is_complete(connection: sqlite3.Connection, version: int) -> bool:
    row = connection.execute(
        """
        SELECT s.status, s.component_count,
               (SELECT COUNT(*) FROM derived_metrics m WHERE m.version = s.version) AS metric_count,
               EXISTS(SELECT 1 FROM consistency_results c WHERE c.version = s.version) AS has_consistency,
               EXISTS(SELECT 1 FROM error_explanations e WHERE e.version = s.version) AS has_errors,
               EXISTS(SELECT 1 FROM commit_responses r WHERE r.version = s.version) AS has_response
        FROM snapshots s
        WHERE s.version = ?
        """,
        (version,),
    ).fetchone()
    return bool(
        row
        and row["status"] == "committed"
        and row["metric_count"] == row["component_count"]
        and row["has_consistency"]
        and row["has_errors"]
        and row["has_response"]
    )


def latest_complete_version(connection: sqlite3.Connection, upper_bound: int | None = None) -> int | None:
    query = "SELECT version FROM snapshots WHERE status = 'committed'"
    parameters: tuple[object, ...] = ()
    if upper_bound is not None:
        query += " AND version <= ?"
        parameters = (upper_bound,)
    query += " ORDER BY version DESC"
    for row in connection.execute(query, parameters):
        if snapshot_is_complete(connection, row["version"]):
            return int(row["version"])
    return None


def create_intent(connection: sqlite3.Connection, version: int, checksum: str, operation: str = "commit") -> int:
    connection.execute("BEGIN IMMEDIATE")
    try:
        cursor = connection.execute(
            "INSERT INTO intents(version, checksum, operation, status, started_at) VALUES (?, ?, ?, 'started', ?)",
            (version, checksum, operation, utc_now()),
        )
        intent_id = int(cursor.lastrowid)
        connection.commit()
        return intent_id
    except Exception:
        connection.rollback()
        raise


def finish_intent(
    connection: sqlite3.Connection,
    intent_id: int,
    status: str,
    detail_json: str = "{}",
) -> None:
    connection.execute(
        "UPDATE intents SET status = ?, finished_at = ?, detail_json = ? WHERE id = ?",
        (status, utc_now(), detail_json, intent_id),
    )


def mark_intent_aborted(path: Path | str, intent_id: int, reason: str) -> None:
    import json

    connection = connect(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        finish_intent(connection, intent_id, "aborted", json.dumps({"reason": reason}, sort_keys=True))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


class InterProcessFileLock:
    """A crash-released single-writer lock on POSIX and Windows."""

    def __init__(self, path: Path | str, timeout_seconds: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout_seconds = timeout_seconds
        self._handle = None

    def __enter__(self) -> "InterProcessFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+b")
        self._handle.seek(0, os.SEEK_END)
        if self._handle.tell() == 0:
            self._handle.write(b"0")
            self._handle.flush()
        deadline = time.monotonic() + self.timeout_seconds
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    self._handle.seek(0)
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        self._handle.close()
                        self._handle = None
                        raise TimeoutError("timed out waiting for the SQLite writer lock")
                    time.sleep(0.05)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        self._handle.close()
                        self._handle = None
                        raise TimeoutError("timed out waiting for the SQLite writer lock")
                    time.sleep(0.05)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._handle is None:
            return
        if os.name == "nt":
            import msvcrt

            try:
                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                self._handle.close()
        else:
            import fcntl

            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._handle.close()
        self._handle = None


@contextmanager
def immediate_transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
