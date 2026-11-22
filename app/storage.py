"""SQLite persistence primitives.

SQLite runs in WAL mode. All business writes are serialized by the service's
writer lock and use BEGIN IMMEDIATE. Readers open independent connections and
pin a complete revision with BEGIN DEFERRED.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

SCHEMA_SQL = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_snapshots (
    version INTEGER PRIMARY KEY,
    base_version INTEGER,
    schema_version TEXT NOT NULL,
    source_schema TEXT NOT NULL,
    generated_at_ms INTEGER NOT NULL,
    source_checksum TEXT NOT NULL,
    checksum TEXT NOT NULL,
    document_json TEXT NOT NULL,
    created_revision INTEGER NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tenant_runtime (
    tenant_id TEXT PRIMARY KEY,
    snapshot_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    usage_cpu_millicores INTEGER NOT NULL,
    usage_memory_bytes INTEGER NOT NULL,
    direct_cpu_millicores INTEGER NOT NULL,
    direct_memory_bytes INTEGER NOT NULL,
    tokens_scaled INTEGER NOT NULL,
    last_refill_ms INTEGER NOT NULL,
    state_version INTEGER NOT NULL,
    updated_revision INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_snapshots (
    revision INTEGER PRIMARY KEY,
    snapshot_version INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    state_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    request_checksum TEXT NOT NULL,
    batch_id TEXT,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    cpu_millicores INTEGER NOT NULL,
    memory_bytes INTEGER NOT NULL,
    arrival_ms INTEGER NOT NULL,
    token_cost INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    decision TEXT NOT NULL,
    code TEXT NOT NULL,
    http_status INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_revision_id ON decisions(revision, id);
CREATE INDEX IF NOT EXISTS idx_decisions_batch ON decisions(batch_id, id);

CREATE TABLE IF NOT EXISTS state_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT,
    revision INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    base_revision INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_checksum TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    revision INTEGER,
    error_json TEXT,
    recovery_run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at_ms);

CREATE TABLE IF NOT EXISTS migration_records (
    id TEXT PRIMARY KEY,
    base_version INTEGER,
    status TEXT NOT NULL,
    code TEXT NOT NULL,
    input_checksum TEXT NOT NULL,
    input_json TEXT NOT NULL,
    output_version INTEGER,
    report_json TEXT NOT NULL,
    report_checksum TEXT NOT NULL,
    revision INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    id TEXT PRIMARY KEY,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER NOT NULL,
    pending_found INTEGER NOT NULL,
    aborted_count INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    snapshot_version INTEGER,
    report_json TEXT NOT NULL,
    report_checksum TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
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
        conn = self._connect()
        try:
            if self.path != ":memory:":
                conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA_SQL)
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('current_revision', '0')"
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version', '1')"
            )
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def read_tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN DEFERRED")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    @contextmanager
    def write_tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    @contextmanager
    def autocommit(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def loads(value: str) -> Any:
    return json.loads(value)


def get_meta(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return default if row is None else str(row["value"])


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def current_revision(conn: sqlite3.Connection) -> int:
    return int(get_meta(conn, "current_revision", "0") or "0")


def current_snapshot_version(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT MAX(version) AS version FROM policy_snapshots").fetchone()
    value = row["version"] if row is not None else None
    return None if value is None else int(value)


def snapshot_at_revision(conn: sqlite3.Connection, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM policy_snapshots "
        "WHERE created_revision <= ? ORDER BY created_revision DESC LIMIT 1",
        (revision,),
    ).fetchone()


def runtime_snapshot_at_revision(conn: sqlite3.Connection, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM runtime_snapshots WHERE revision = ?",
        (revision,),
    ).fetchone()


def get_runtime_row(conn: sqlite3.Connection, tenant_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM tenant_runtime WHERE tenant_id = ?", (tenant_id,)
    ).fetchone()


def list_runtime_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM tenant_runtime ORDER BY tenant_id ASC"))


def next_revision(conn: sqlite3.Connection) -> int:
    revision = current_revision(conn) + 1
    set_meta(conn, "current_revision", str(revision))
    return revision
