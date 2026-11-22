"""SQLite persistence, transactional boundaries, and lease-row operations."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

from .metrics import HolderFact, LeaseFact

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS holders (
    holder_id TEXT PRIMARY KEY,
    declared_weight TEXT NOT NULL,
    declared_weight_q INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS lock_registry (
    lock_key TEXT PRIMARY KEY,
    registered_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS lock_state (
    lock_key TEXT PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (lock_key) REFERENCES lock_registry(lock_key)
);

CREATE TABLE IF NOT EXISTS leases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lock_key TEXT NOT NULL,
    holder_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    granted_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    ttl_ms INTEGER NOT NULL,
    renew_count INTEGER NOT NULL,
    weight_text TEXT NOT NULL,
    weight_q INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','released','expired')),
    last_renewed_at_ms INTEGER,
    ended_at_ms INTEGER,
    acquire_request_id TEXT NOT NULL,
    UNIQUE (lock_key, version),
    FOREIGN KEY (lock_key) REFERENCES lock_registry(lock_key),
    FOREIGN KEY (holder_id) REFERENCES holders(holder_id)
);

CREATE INDEX IF NOT EXISTS idx_leases_active_expiry
    ON leases(status, expires_at_ms, lock_key);

CREATE TABLE IF NOT EXISTS expiry_index (
    lock_key TEXT PRIMARY KEY,
    lease_id INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    FOREIGN KEY (lock_key) REFERENCES lock_registry(lock_key),
    FOREIGN KEY (lease_id) REFERENCES leases(id)
);

CREATE INDEX IF NOT EXISTS idx_expiry_index_deadline
    ON expiry_index(expires_at_ms, lock_key);

CREATE TABLE IF NOT EXISTS lease_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lock_key TEXT NOT NULL,
    holder_id TEXT,
    version INTEGER,
    event_type TEXT NOT NULL,
    event_at_ms INTEGER NOT NULL,
    request_id TEXT,
    details_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lease_events_key
    ON lease_events(lock_key, id);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    lock_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('prepared','completed','rolled_back')),
    response_json TEXT,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS intentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    lock_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    prior_state_json TEXT NOT NULL,
    intended_state_json TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('prepared','committed','recovered','recovery_failed')),
    owner_pid INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    recovery_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_intentions_phase
    ON intentions(phase, id);

CREATE TABLE IF NOT EXISTS metric_cache (
    cache_key TEXT PRIMARY KEY,
    revision INTEGER NOT NULL,
    computed_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER NOT NULL,
    scanned_intents INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Store:
    def __init__(self, db_path: str | os.PathLike[str]) -> None:
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        con = self._connect()
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=FULL")
            con.executescript(SCHEMA)
            con.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            con.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES('revision', '0')"
            )
            con.commit()
        finally:
            con.close()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("PRAGMA synchronous=FULL")
        return con

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            yield con
            con.execute("COMMIT")
        except BaseException:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        con = self._connect()
        try:
            con.execute("BEGIN")
            yield con
            con.execute("COMMIT")
        except BaseException:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    @staticmethod
    def json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def get_metadata(con: sqlite3.Connection, key: str) -> str | None:
        row = con.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    @staticmethod
    def revision(con: sqlite3.Connection) -> int:
        value = Store.get_metadata(con, "revision")
        return int(value or 0)

    @staticmethod
    def increment_revision(con: sqlite3.Connection) -> int:
        current = Store.revision(con)
        updated = current + 1
        con.execute(
            "INSERT INTO metadata(key, value) VALUES('revision', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(updated),),
        )
        return updated

    @staticmethod
    def ensure_registered(con: sqlite3.Connection, lock_key: str, now_ms: int) -> None:
        con.execute(
            "INSERT OR IGNORE INTO lock_registry(lock_key, registered_at_ms) VALUES(?, ?)",
            (lock_key, now_ms),
        )
        con.execute(
            "INSERT OR IGNORE INTO lock_state(lock_key, version, updated_at_ms) VALUES(?, 0, ?)",
            (lock_key, now_ms),
        )

    @staticmethod
    def upsert_holder(
        con: sqlite3.Connection, holder_id: str, weight_text: str, weight_q: int, now_ms: int
    ) -> None:
        con.execute(
            "INSERT INTO holders(holder_id, declared_weight, declared_weight_q, updated_at_ms) "
            "VALUES(?, ?, ?, ?) ON CONFLICT(holder_id) DO UPDATE SET "
            "declared_weight=excluded.declared_weight, declared_weight_q=excluded.declared_weight_q, "
            "updated_at_ms=excluded.updated_at_ms",
            (holder_id, weight_text, weight_q, now_ms),
        )

    @staticmethod
    def snapshot_lock(con: sqlite3.Connection, lock_key: str) -> dict[str, Any]:
        state = con.execute(
            "SELECT lock_key, version, updated_at_ms FROM lock_state WHERE lock_key=?",
            (lock_key,),
        ).fetchone()
        if state is None:
            return {
                "lock_key": lock_key,
                "version": 0,
                "lease": None,
                "registered": False,
            }
        lease = con.execute(
            "SELECT * FROM leases WHERE lock_key=? AND version=?",
            (lock_key, int(state["version"])),
        ).fetchone()
        expiry = con.execute(
            "SELECT expires_at_ms FROM expiry_index WHERE lock_key=?", (lock_key,)
        ).fetchone()
        return {
            "lock_key": lock_key,
            "version": int(state["version"]),
            "state_updated_at_ms": int(state["updated_at_ms"]),
            "registered": True,
            "expiry_index_expires_at_ms": None if expiry is None else int(expiry["expires_at_ms"]),
            "lease": None if lease is None else dict(lease),
        }

    @staticmethod
    def active_lease_at_version(
        con: sqlite3.Connection, lock_key: str, version: int
    ) -> sqlite3.Row | None:
        return con.execute(
            "SELECT * FROM leases WHERE lock_key=? AND version=?", (lock_key, version)
        ).fetchone()

    @staticmethod
    def active_leases(con: sqlite3.Connection, now_ms: int) -> list[LeaseFact]:
        rows = con.execute(
            "SELECT l.lock_key, l.holder_id, l.version, l.granted_at_ms, l.expires_at_ms, "
            "l.ttl_ms, l.renew_count, l.weight_text, l.weight_q "
            "FROM leases l JOIN lock_state s ON s.lock_key=l.lock_key AND s.version=l.version "
            "WHERE l.status='active' AND l.expires_at_ms > ? "
            "ORDER BY l.lock_key, l.holder_id, l.version",
            (now_ms,),
        ).fetchall()
        return [
            LeaseFact(
                lock_key=str(row["lock_key"]),
                holder_id=str(row["holder_id"]),
                version=int(row["version"]),
                granted_at_ms=int(row["granted_at_ms"]),
                expires_at_ms=int(row["expires_at_ms"]),
                ttl_ms=int(row["ttl_ms"]),
                renew_count=int(row["renew_count"]),
                weight_text=str(row["weight_text"]),
                weight_q=int(row["weight_q"]),
            )
            for row in rows
        ]

    @staticmethod
    def holders(con: sqlite3.Connection) -> list[HolderFact]:
        rows = con.execute(
            "SELECT holder_id, declared_weight, declared_weight_q FROM holders ORDER BY holder_id"
        ).fetchall()
        return [
            HolderFact(
                holder_id=str(row["holder_id"]),
                weight_text=str(row["declared_weight"]),
                weight_q=int(row["declared_weight_q"]),
            )
            for row in rows
        ]

    @staticmethod
    def registered_key_count(con: sqlite3.Connection) -> int:
        row = con.execute("SELECT COUNT(*) AS n FROM lock_registry").fetchone()
        return int(row["n"] if row is not None else 0)

    @staticmethod
    def expire_due(con: sqlite3.Connection, now_ms: int) -> list[str]:
        rows = con.execute(
            "SELECT id, lock_key, holder_id, version, expires_at_ms FROM leases "
            "WHERE status='active' AND expires_at_ms <= ? ORDER BY expires_at_ms, lock_key",
            (now_ms,),
        ).fetchall()
        expired: list[str] = []
        for row in rows:
            con.execute(
                "UPDATE leases SET status='expired', ended_at_ms=? WHERE id=? AND status='active'",
                (now_ms, int(row["id"])),
            )
            con.execute("DELETE FROM expiry_index WHERE lock_key=?", (str(row["lock_key"]),))
            Store.event(
                con,
                lock_key=str(row["lock_key"]),
                holder_id=str(row["holder_id"]),
                version=int(row["version"]),
                event_type="expired",
                event_at_ms=now_ms,
                request_id=None,
                details={"expires_at_ms": int(row["expires_at_ms"])},
            )
            expired.append(str(row["lock_key"]))
        return expired

    @staticmethod
    def event(
        con: sqlite3.Connection,
        *,
        lock_key: str,
        holder_id: str | None,
        version: int | None,
        event_type: str,
        event_at_ms: int,
        request_id: str | None,
        details: dict[str, Any],
    ) -> None:
        con.execute(
            "INSERT INTO lease_events(lock_key, holder_id, version, event_type, event_at_ms, request_id, details_json) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (lock_key, holder_id, version, event_type, event_at_ms, request_id, Store.json(details)),
        )

    @staticmethod
    def get_request(con: sqlite3.Connection, request_id: str) -> sqlite3.Row | None:
        return con.execute("SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()

    @staticmethod
    def request_response(row: sqlite3.Row) -> dict[str, Any]:
        if row["response_json"] is None:
            raise ValueError("completed request has no response")
        return json.loads(str(row["response_json"]))

    @staticmethod
    def latest_prepared_intent(
        con: sqlite3.Connection, request_id: str
    ) -> sqlite3.Row | None:
        return con.execute(
            "SELECT * FROM intentions WHERE request_id=? AND phase='prepared' ORDER BY id DESC LIMIT 1",
            (request_id,),
        ).fetchone()

    @staticmethod
    def create_prepared_intent(
        con: sqlite3.Connection,
        *,
        request_id: str,
        operation: str,
        lock_key: str,
        payload_hash: str,
        payload: dict[str, Any],
        prior_state: dict[str, Any],
        now_ms: int,
    ) -> int:
        cursor = con.execute(
            "INSERT INTO intentions(request_id, operation, lock_key, payload_hash, payload_json, "
            "prior_state_json, intended_state_json, phase, owner_pid, created_at_ms) "
            "VALUES(?, ?, ?, ?, ?, ?, '{}', 'prepared', ?, ?)",
            (
                request_id,
                operation,
                lock_key,
                payload_hash,
                Store.json(payload),
                Store.json(prior_state),
                os.getpid(),
                now_ms,
            ),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def finish_intent(
        con: sqlite3.Connection,
        intent_id: int,
        *,
        phase: str,
        intended_state: dict[str, Any],
        now_ms: int,
        note: str | None = None,
    ) -> None:
        con.execute(
            "UPDATE intentions SET phase=?, intended_state_json=?, completed_at_ms=?, recovery_note=? "
            "WHERE id=?",
            (phase, Store.json(intended_state), now_ms, note, intent_id),
        )

    @staticmethod
    def complete_request(
        con: sqlite3.Connection, request_id: str, response: dict[str, Any], now_ms: int
    ) -> None:
        con.execute(
            "UPDATE requests SET status='completed', response_json=?, completed_at_ms=? WHERE request_id=?",
            (Store.json(response), now_ms, request_id),
        )

    @staticmethod
    def upsert_metric_cache(
        con: sqlite3.Connection, revision: int, now_ms: int, payload: dict[str, Any]
    ) -> None:
        con.execute(
            "INSERT INTO metric_cache(cache_key, revision, computed_at_ms, payload_json) "
            "VALUES('cluster', ?, ?, ?) ON CONFLICT(cache_key) DO UPDATE SET "
            "revision=excluded.revision, computed_at_ms=excluded.computed_at_ms, payload_json=excluded.payload_json",
            (revision, now_ms, Store.json(payload)),
        )

    @staticmethod
    def get_metric_cache(con: sqlite3.Connection) -> dict[str, Any] | None:
        row = con.execute(
            "SELECT revision, computed_at_ms, payload_json FROM metric_cache WHERE cache_key='cluster'"
        ).fetchone()
        if row is None:
            return None
        return {
            "revision": int(row["revision"]),
            "computed_at_ms": int(row["computed_at_ms"]),
            "payload": json.loads(str(row["payload_json"])),
        }

    @staticmethod
    def latest_recovery(con: sqlite3.Connection) -> dict[str, Any] | None:
        row = con.execute(
            "SELECT id, started_at_ms, completed_at_ms, scanned_intents, report_json "
            "FROM recovery_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": int(row["id"]),
            "started_at_ms": int(row["started_at_ms"]),
            "completed_at_ms": int(row["completed_at_ms"]),
            "scanned_intents": int(row["scanned_intents"]),
            "report": json.loads(str(row["report_json"])),
        }

    @staticmethod
    def insert_recovery(
        con: sqlite3.Connection,
        started_at_ms: int,
        completed_at_ms: int,
        scanned: int,
        report: dict[str, Any],
    ) -> int:
        cursor = con.execute(
            "INSERT INTO recovery_runs(started_at_ms, completed_at_ms, scanned_intents, report_json) "
            "VALUES(?, ?, ?, ?)",
            (started_at_ms, completed_at_ms, scanned, Store.json(report)),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def all_leases(con: sqlite3.Connection) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in con.execute(
                "SELECT * FROM leases ORDER BY lock_key, version"
            ).fetchall()
        ]

    @staticmethod
    def pending_prepared_intents(con: sqlite3.Connection) -> list[sqlite3.Row]:
        return con.execute(
            "SELECT * FROM intentions WHERE phase='prepared' ORDER BY id"
        ).fetchall()

