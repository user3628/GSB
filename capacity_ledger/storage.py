"""SQLite storage and a cross-platform process-wide writer lock."""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterator

from .checksum import canonical_json


SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_versions (
    version INTEGER PRIMARY KEY,
    commit_id TEXT NOT NULL UNIQUE,
    base_version INTEGER NOT NULL,
    mode TEXT NOT NULL,
    mode_reason TEXT NOT NULL,
    snapshot_checksum TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    metrics_checksum TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    result_checksum TEXT NOT NULL,
    result_json TEXT NOT NULL,
    previous_ledger_checksum TEXT NOT NULL,
    ledger_checksum TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commits (
    commit_id TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    kind TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    target_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('COMMITTED', 'REJECTED')),
    http_status INTEGER NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    commit_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    target_version INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PREPARED', 'COMMITTED', 'ABORTED')),
    payload_json TEXT NOT NULL,
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS intents_state_idx ON intents(state, target_version);

CREATE TABLE IF NOT EXISTS artifact_catalog (
    artifact_id TEXT PRIMARY KEY,
    artifact_kind TEXT NOT NULL,
    service_name TEXT,
    dependencies_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_current (
    artifact_id TEXT PRIMARY KEY REFERENCES artifact_catalog(artifact_id),
    origin_version INTEGER NOT NULL,
    value_json TEXT NOT NULL,
    value_checksum TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_versions (
    version INTEGER NOT NULL,
    artifact_id TEXT NOT NULL,
    value_json TEXT NOT NULL,
    value_checksum TEXT NOT NULL,
    event TEXT NOT NULL,
    PRIMARY KEY (version, artifact_id, event)
);

CREATE TABLE IF NOT EXISTS invalidations (
    version INTEGER NOT NULL,
    artifact_id TEXT NOT NULL,
    wave INTEGER NOT NULL,
    reason TEXT NOT NULL,
    cause TEXT NOT NULL,
    PRIMARY KEY (version, artifact_id)
);

CREATE INDEX IF NOT EXISTS ledger_commit_idx ON ledger_versions(commit_id);
"""


class FileLock:
    """Blocking exclusive lock shared by threads and local processes."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._thread_lock = threading.RLock()
        self._file: Any = None

    def __enter__(self) -> "FileLock":
        self._thread_lock.acquire()
        handle: Any = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle = open(self.path, "a+b")
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
                os.fsync(handle.fileno())
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            self._file = handle
            return self
        except BaseException:
            if handle is not None:
                handle.close()
            self._thread_lock.release()
            raise

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if self._file is not None:
                self._file.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
                self._file.close()
                self._file = None
        finally:
            self._thread_lock.release()


class SQLiteStore:
    def __init__(self, db_path: Path, intent_dir: Path) -> None:
        self.db_path = Path(db_path).resolve()
        self.intent_dir = Path(intent_dir).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.intent_dir.mkdir(parents=True, exist_ok=True)
        self.writer_lock = FileLock(self.db_path.parent / "capacity-ledger.writer.lock")
        self.intent_log_path = self.intent_dir / "intents.jsonl"
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
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
        with self.writer_lock:
            connection = self.connect()
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.executescript(SCHEMA)
            finally:
                connection.close()

    @contextlib.contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextlib.contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def append_intent_log(self, event: dict[str, Any]) -> None:
        self.intent_dir.mkdir(parents=True, exist_ok=True)
        with open(self.intent_log_path, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(canonical_json(event))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

    def read_intent_log(self, limit: int = 100) -> list[dict[str, Any]]:
        if not self.intent_log_path.exists():
            return []
        events: list[dict[str, Any]] = []
        with open(self.intent_log_path, "r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    events.append(json.loads(line))
        return events[-limit:]

