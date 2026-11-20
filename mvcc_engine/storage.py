"""SQLite schema, seed data, durable intent append, and atomic files."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable

from .config import Settings
from .errors import fail


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id TEXT PRIMARY KEY,
    txn_seq INTEGER NOT NULL UNIQUE,
    isolation TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at REAL NOT NULL,
    committed_at REAL,
    snapshot_seq INTEGER,
    commit_seq INTEGER,
    commit_request_hash TEXT,
    commit_result_json TEXT,
    abort_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);

CREATE TABLE IF NOT EXISTS committed_transactions (
    txn_id TEXT PRIMARY KEY,
    commit_seq INTEGER NOT NULL UNIQUE,
    committed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS row_versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    row_version INTEGER NOT NULL,
    row_data_json TEXT NOT NULL,
    created_txn TEXT NOT NULL,
    deleted_txn TEXT,
    commit_seq INTEGER,
    created_at REAL NOT NULL,
    UNIQUE(table_name, row_id, row_version)
);
CREATE INDEX IF NOT EXISTS idx_row_versions_row ON row_versions(table_name, row_id, row_version DESC);
CREATE INDEX IF NOT EXISTS idx_row_versions_created_txn ON row_versions(created_txn);
CREATE INDEX IF NOT EXISTS idx_row_versions_deleted_txn ON row_versions(deleted_txn);

CREATE TABLE IF NOT EXISTS edit_sessions (
    session_id TEXT PRIMARY KEY,
    txn_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    version_id INTEGER NOT NULL,
    row_version INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL,
    snapshot_seq INTEGER NOT NULL,
    status TEXT NOT NULL,
    opened_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    budget_ms INTEGER NOT NULL,
    last_saved_at REAL,
    saved_version_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_edit_sessions_txn ON edit_sessions(txn_id);

CREATE TABLE IF NOT EXISTS savepoints (
    savepoint_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id TEXT NOT NULL,
    name TEXT NOT NULL,
    snapshot_seq INTEGER NOT NULL,
    op_seq INTEGER NOT NULL,
    intent_seq INTEGER NOT NULL,
    sessions_json TEXT NOT NULL,
    locks_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(txn_id, name)
);

CREATE TABLE IF NOT EXISTS txn_write_ops (
    op_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    old_version_id INTEGER,
    old_deleted_before TEXT,
    new_version_id INTEGER,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_txn_write_ops_txn ON txn_write_ops(txn_id, op_seq);

CREATE TABLE IF NOT EXISTS intents (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_txn ON intents(txn_id, status);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at REAL NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Storage:
    """Small SQLite gateway.

    Writes are serialized by the engine's single-writer critical section.  Each
    operation uses its own short-lived connection so read requests can proceed
    concurrently under WAL.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.ensure_directories()
        self._intent_file_lock = threading.Lock()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.settings.db_path),
            timeout=self.settings.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.settings.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def initialize(self) -> None:
        conn = self.connect()
        try:
            conn.executescript(SCHEMA_SQL)
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT value FROM meta WHERE key='initialized'").fetchone()
                if row is None:
                    self._bootstrap(conn)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    def _bootstrap(self, conn: sqlite3.Connection) -> None:
        seed_path = self.settings.seed_path
        if not seed_path.exists():
            raise RuntimeError(f"seed file not found: {seed_path}")
        seed = json.loads(seed_path.read_text(encoding="utf-8"))
        now = time.time()
        txn_id = "sys-bootstrap"
        conn.execute(
            "INSERT INTO transactions(txn_id,txn_seq,isolation,status,started_at,committed_at,snapshot_seq,commit_seq) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (txn_id, 0, "SYSTEM", "committed", now, now, 1, 1),
        )
        conn.execute(
            "INSERT INTO committed_transactions(txn_id,commit_seq,committed_at) VALUES(?,?,?)",
            (txn_id, 1, now),
        )
        for table_name, rows in seed.items():
            if not isinstance(rows, list):
                raise RuntimeError(f"seed table {table_name!r} must be a list")
            for item in rows:
                row_id = str(item["row_id"])
                data = item.get("data", {})
                conn.execute(
                    "INSERT INTO row_versions(table_name,row_id,row_version,row_data_json,created_txn,deleted_txn,commit_seq,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (table_name, row_id, 1, _json(data), txn_id, None, 1, now),
                )
        meta = {
            "initialized": "1",
            "schema_version": "1",
            "next_txn_seq": "1",
            "next_commit_seq": "2",
        }
        conn.executemany("INSERT INTO meta(key,value) VALUES(?,?)", meta.items())

    def append_intent_log(self, record: dict[str, Any]) -> None:
        line = _json(record) + "\n"
        with self._intent_file_lock:
            with self.settings.intent_log_path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())

    def record_intent(
        self,
        txn_id: str | None,
        kind: str,
        payload: dict[str, Any] | None = None,
    ) -> int:
        """Persist an intent before the corresponding data mutation.

        The SQLite intent row is committed first.  The append-only JSONL mirror is
        fsynced immediately afterwards; recovery treats SQLite as authoritative.
        """

        now = time.time()
        payload_json = _json(payload or {})
        conn = self.connect()
        try:
            cur = conn.execute(
                "INSERT INTO intents(txn_id,kind,status,payload_json,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?)",
                (txn_id, kind, "open", payload_json, now, now),
            )
            intent_id = int(cur.lastrowid)
        finally:
            conn.close()
        self.append_intent_log(
            {
                "intent_id": intent_id,
                "txn_id": txn_id,
                "kind": kind,
                "status": "open",
                "payload": payload or {},
                "created_at": now,
            }
        )
        return intent_id

    def current_commit_seq(self, conn: sqlite3.Connection | None = None) -> int:
        own = conn is None
        c = conn or self.connect()
        try:
            row = c.execute("SELECT value FROM meta WHERE key='next_commit_seq'").fetchone()
            return max(0, int(row["value"]) - 1)
        finally:
            if own:
                c.close()

    def write_json_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)

    def close(self) -> None:
        # Connections are operation-scoped; this method documents lifecycle and
        # gives callers a single shutdown point.
        return None


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_json(text: str | None, default: Any = None) -> Any:
    if text is None:
        return default
    return json.loads(text)


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]

