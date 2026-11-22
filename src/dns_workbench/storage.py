from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, TypeVar

from .models import QuerySpec, now_ms
from .protocol import records_from_json, records_to_json


T = TypeVar("T")


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS cache_entries (
    cache_key TEXT PRIMARY KEY,
    qname TEXT NOT NULL,
    qtype INTEGER NOT NULL,
    qclass INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('positive', 'negative')),
    negative_kind TEXT,
    rcode INTEGER NOT NULL,
    source_type INTEGER,
    written_at_ms INTEGER NOT NULL,
    original_ttl_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    records_json TEXT NOT NULL,
    authorities_json TEXT NOT NULL,
    query_id TEXT,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cache_name_type ON cache_entries(qname, qtype, qclass);
CREATE INDEX IF NOT EXISTS idx_cache_expiry ON cache_entries(expires_at_ms);

CREATE TABLE IF NOT EXISTS sessions (
    query_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    qname TEXT NOT NULL,
    qtype INTEGER NOT NULL,
    qclass INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'resolving', 'complete', 'failed', 'aborted')),
    business_code TEXT,
    rcode INTEGER,
    source_type INTEGER,
    response_json TEXT,
    error_json TEXT,
    round_id TEXT,
    transport TEXT NOT NULL,
    client TEXT NOT NULL,
    cache_hit INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_round ON sessions(round_id);
CREATE INDEX IF NOT EXISTS idx_sessions_state ON sessions(state);

CREATE TABLE IF NOT EXISTS intent_log (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'committed', 'rolled_back')),
    created_at_ms INTEGER NOT NULL,
    committed_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intent_log(status);

CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    budget_ms INTEGER NOT NULL,
    status TEXT NOT NULL,
    trace_json TEXT,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_meta (
    meta_key TEXT PRIMARY KEY,
    meta_value TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

PRAGMA user_version=1;
"""


@dataclass(slots=True)
class CacheEntry:
    cache_key: str
    qname: str
    qtype: int
    qclass: int
    kind: str
    negative_kind: str | None
    rcode: int
    source_type: int | None
    written_at_ms: int
    original_ttl_ms: int
    expires_at_ms: int
    records: list[dict[str, Any]]
    authorities: list[dict[str, Any]]
    query_id: str | None
    updated_at_ms: int

    @property
    def remaining_ttl_ms(self) -> int:
        return max(0, self.expires_at_ms - now_ms())

    def to_dict(self, at_ms: int | None = None) -> dict[str, Any]:
        instant = now_ms() if at_ms is None else at_ms
        elapsed = max(0, instant - self.written_at_ms)
        records = _remaining_records(self.records, elapsed)
        authorities = _remaining_records(self.authorities, elapsed)
        return {
            "cache_key": self.cache_key,
            "name": self.qname,
            "type": self.qtype,
            "qclass": self.qclass,
            "kind": self.kind,
            "negative_kind": self.negative_kind,
            "rcode": self.rcode,
            "source_record_type": self.source_type,
            "written_at_ms": self.written_at_ms,
            "original_ttl_ms": self.original_ttl_ms,
            "expires_at_ms": self.expires_at_ms,
            "remaining_ttl_ms": max(0, self.expires_at_ms - instant),
            "remaining_ttl_seconds": round(max(0, self.expires_at_ms - instant) / 1000.0, 3),
            "answers": records,
            "authorities": authorities,
            "query_id": self.query_id,
        }


def _remaining_records(records: Iterable[dict[str, Any]], elapsed_ms: int) -> list[dict[str, Any]]:
    result = []
    for record in records:
        item = dict(record)
        remaining = max(0, int(item.get("ttl_ms", 0)) - elapsed_ms)
        item["ttl_ms"] = remaining
        item["ttl"] = (remaining + 999) // 1000
        result.append(item)
    return result


def _decode_json(value: str | None, default: Any) -> Any:
    if value is None:
        return default
    return json.loads(value)


class Storage:
    """SQLite store with one serialized writer and WAL snapshot readers."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._write_conn: sqlite3.Connection | None = None
        self._thread_lock = threading.Lock()
        self._init_lock = asyncio.Lock()
        self._initialized = False

    @property
    def write_connection(self) -> sqlite3.Connection:
        if self._write_conn is None:
            raise RuntimeError("storage is not initialized")
        return self._write_conn

    async def initialize(self) -> dict[str, Any]:
        async with self._init_lock:
            if self._initialized:
                return await self.latest_recovery() or {}
            await asyncio.to_thread(self._initialize_sync)
            self._initialized = True
            return await self.recover()

    def _initialize_sync(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.db_path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.executescript(SCHEMA)
        self._write_conn = connection

    async def close(self) -> None:
        async with self._init_lock:
            if self._write_conn is not None:
                connection = self._write_conn
                self._write_conn = None
                await asyncio.to_thread(connection.close)
            self._initialized = False

    def _read_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    async def _read(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        def run() -> T:
            connection = self._read_connection()
            try:
                return operation(connection)
            finally:
                connection.close()

        return await asyncio.to_thread(run)

    async def _write(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        def run() -> T:
            with self._thread_lock:
                connection = self.write_connection
                connection.execute("BEGIN IMMEDIATE")
                try:
                    result = operation(connection)
                    connection.execute("COMMIT")
                    return result
                except Exception:
                    connection.execute("ROLLBACK")
                    raise

        return await asyncio.to_thread(run)

    async def recover(self) -> dict[str, Any]:
        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            clean_row = connection.execute(
                "SELECT meta_value FROM runtime_meta WHERE meta_key='clean_shutdown'"
            ).fetchone()
            initialized_row = connection.execute(
                "SELECT meta_value FROM runtime_meta WHERE meta_key='initialized'"
            ).fetchone()
            clean_before = bool(clean_row and clean_row[0] == "1")
            initialized_before = bool(initialized_row and initialized_row[0] == "1")
            first_startup = not initialized_before
            pending = connection.execute(
                "SELECT intent_id, query_id, cache_key, kind, payload_json, created_at_ms "
                "FROM intent_log WHERE status='pending' ORDER BY intent_id"
            ).fetchall()
            rolled_back_cache_keys: list[str] = []
            for row in pending:
                deleted = connection.execute(
                    "DELETE FROM cache_entries WHERE cache_key=? AND query_id=?",
                    (row["cache_key"], row["query_id"]),
                )
                if deleted.rowcount:
                    rolled_back_cache_keys.append(row["cache_key"])
                connection.execute(
                    "UPDATE intent_log SET status='rolled_back' WHERE intent_id=?",
                    (row["intent_id"],),
                )

            aborted_sessions = connection.execute(
                "SELECT query_id, round_id, qname, qtype, qclass, state FROM sessions "
                "WHERE state IN ('pending', 'resolving') ORDER BY created_at_ms, query_id"
            ).fetchall()
            timestamp = now_ms()
            for row in aborted_sessions:
                error = {
                    "code": "E_PROCESS_INTERRUPTED",
                    "message": "session was not committed before process interruption",
                    "context": {"query_id": row["query_id"], "state_before_recovery": row["state"]},
                }
                connection.execute(
                    "UPDATE sessions SET state='aborted', business_code='E_PROCESS_INTERRUPTED', "
                    "error_json=?, updated_at_ms=? WHERE query_id=?",
                    (json.dumps(error, ensure_ascii=False), timestamp, row["query_id"]),
                )

            interrupted_rounds = connection.execute(
                "SELECT round_id, started_at_ms, budget_ms, status FROM rounds "
                "WHERE status='running' ORDER BY started_at_ms, round_id"
            ).fetchall()
            for row in interrupted_rounds:
                connection.execute("UPDATE rounds SET status='interrupted' WHERE round_id=?", (row["round_id"],))

            report = {
                "report_id": None,
                "created_at_ms": timestamp,
                "generated_by": "startup_recovery",
                "source_of_truth": "sqlite_committed_rows",
                "clean_shutdown_before_startup": clean_before,
                "first_startup": first_startup,
                "forced_interruption_detected": initialized_before and not clean_before,
                "pending_intents_found": len(pending),
                "rolled_back_cache_keys": rolled_back_cache_keys,
                "rolled_back_intents": [
                    {
                        "intent_id": int(row["intent_id"]),
                        "query_id": row["query_id"],
                        "cache_key": row["cache_key"],
                        "kind": row["kind"],
                        "created_at_ms": int(row["created_at_ms"]),
                    }
                    for row in pending
                ],
                "aborted_sessions": [
                    {
                        "query_id": row["query_id"],
                        "round_id": row["round_id"],
                        "name": row["qname"],
                        "type": row["qtype"],
                        "qclass": row["qclass"],
                        "state_before_recovery": row["state"],
                    }
                    for row in aborted_sessions
                ],
                "interrupted_rounds": [
                    {
                        "round_id": row["round_id"],
                        "started_at_ms": int(row["started_at_ms"]),
                        "budget_ms": int(row["budget_ms"]),
                    }
                    for row in interrupted_rounds
                ],
                "limits": {
                    "note": "Only facts already committed to SQLite are reported; no synthetic exit report is fabricated.",
                    "completed_cache_entries": int(
                        connection.execute("SELECT COUNT(*) FROM cache_entries").fetchone()[0]
                    ),
                    "completed_sessions": int(
                        connection.execute(
                            "SELECT COUNT(*) FROM sessions WHERE state IN ('complete', 'failed')"
                        ).fetchone()[0]
                    ),
                },
            }
            cursor = connection.execute(
                "INSERT INTO recovery_reports(created_at_ms, report_json) VALUES(?, ?)",
                (timestamp, json.dumps(report, ensure_ascii=False, sort_keys=True)),
            )
            report["report_id"] = int(cursor.lastrowid)
            connection.execute(
                "UPDATE recovery_reports SET report_json=? WHERE report_id=?",
                (json.dumps(report, ensure_ascii=False, sort_keys=True), report["report_id"]),
            )
            connection.execute(
                "INSERT INTO runtime_meta(meta_key, meta_value, updated_at_ms) VALUES('initialized','1',?) "
                "ON CONFLICT(meta_key) DO UPDATE SET meta_value='1', updated_at_ms=excluded.updated_at_ms",
                (timestamp,),
            )
            connection.execute(
                "INSERT INTO runtime_meta(meta_key, meta_value, updated_at_ms) VALUES('clean_shutdown','0',?) "
                "ON CONFLICT(meta_key) DO UPDATE SET meta_value='0', updated_at_ms=excluded.updated_at_ms",
                (timestamp,),
            )
            return report

        return await self._write(operation)

    async def mark_clean_shutdown(self, clean: bool = True) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                "INSERT INTO runtime_meta(meta_key, meta_value, updated_at_ms) VALUES('clean_shutdown',?,?) "
                "ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value, "
                "updated_at_ms=excluded.updated_at_ms",
                ("1" if clean else "0", now_ms()),
            )

        await self._write(operation)

    async def latest_recovery(self) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection) -> dict[str, Any] | None:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
            ).fetchone()
            return None if row is None else json.loads(row["report_json"])

        return await self._read(operation)

    async def list_recovery_reports(self, limit: int = 20) -> list[dict[str, Any]]:
        def operation(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY report_id DESC LIMIT ?",
                (max(1, min(limit, 200)),),
            ).fetchall()
            return [json.loads(row["report_json"]) for row in rows]

        return await self._read(operation)

    async def get_session(self, query_id: str) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection) -> dict[str, Any] | None:
            row = connection.execute("SELECT * FROM sessions WHERE query_id=?", (query_id,)).fetchone()
            return None if row is None else _session_row(row, include_payload=True)

        return await self._read(operation)

    async def start_session(self, spec: QuerySpec, round_id: str) -> None:
        timestamp = now_ms()

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                "INSERT INTO sessions(query_id, fingerprint, qname, qtype, qclass, state, round_id, "
                "transport, client, cache_hit, created_at_ms, updated_at_ms) "
                "VALUES(?,?,?,?,?,'pending',?,?,?,0,?,?) "
                "ON CONFLICT(query_id) DO UPDATE SET fingerprint=excluded.fingerprint, qname=excluded.qname, "
                "qtype=excluded.qtype, qclass=excluded.qclass, state='pending', business_code=NULL, "
                "rcode=NULL, source_type=NULL, response_json=NULL, error_json=NULL, round_id=excluded.round_id, "
                "transport=excluded.transport, client=excluded.client, cache_hit=0, updated_at_ms=excluded.updated_at_ms",
                (
                    spec.query_id,
                    spec.fingerprint,
                    spec.name,
                    spec.qtype,
                    spec.qclass,
                    round_id,
                    spec.source_transport,
                    spec.client,
                    timestamp,
                    timestamp,
                ),
            )

        await self._write(operation)

    async def mark_session_resolving(self, query_id: str, round_id: str) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                "UPDATE sessions SET state='resolving', round_id=?, updated_at_ms=? WHERE query_id=?",
                (round_id, now_ms(), query_id),
            )

        await self._write(operation)

    async def complete_session(
        self,
        query_id: str,
        *,
        state: str,
        rcode: int,
        source_type: int | None,
        business_code: str | None,
        response: dict[str, Any] | None,
        error: dict[str, Any] | None,
        cache_hit: bool = False,
    ) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                "UPDATE sessions SET state=?, rcode=?, source_type=?, business_code=?, response_json=?, "
                "error_json=?, cache_hit=?, updated_at_ms=? WHERE query_id=?",
                (
                    state,
                    rcode,
                    source_type,
                    business_code,
                    json.dumps(response, ensure_ascii=False, sort_keys=True) if response is not None else None,
                    json.dumps(error, ensure_ascii=False, sort_keys=True) if error is not None else None,
                    1 if cache_hit else 0,
                    now_ms(),
                    query_id,
                ),
            )

        await self._write(operation)

    async def write_intent(
        self,
        *,
        query_id: str,
        cache_key: str,
        kind: str,
        payload: dict[str, Any],
    ) -> int:
        timestamp = now_ms()

        def operation(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(
                "INSERT INTO intent_log(query_id, cache_key, kind, payload_json, status, created_at_ms) "
                "VALUES(?,?,?,?, 'pending', ?)",
                (query_id, cache_key, kind, json.dumps(payload, ensure_ascii=False, sort_keys=True), timestamp),
            )
            return int(cursor.lastrowid)

        return await self._write(operation)

    async def commit_cache_and_session(
        self,
        *,
        intent_id: int,
        entry: CacheEntry,
        query_id: str,
        response: dict[str, Any],
        rcode: int,
        source_type: int | None,
    ) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                "INSERT INTO cache_entries(cache_key, qname, qtype, qclass, kind, negative_kind, rcode, "
                "source_type, written_at_ms, original_ttl_ms, expires_at_ms, records_json, authorities_json, "
                "query_id, updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(cache_key) DO UPDATE SET qname=excluded.qname, qtype=excluded.qtype, "
                "qclass=excluded.qclass, kind=excluded.kind, negative_kind=excluded.negative_kind, "
                "rcode=excluded.rcode, source_type=excluded.source_type, written_at_ms=excluded.written_at_ms, "
                "original_ttl_ms=excluded.original_ttl_ms, expires_at_ms=excluded.expires_at_ms, "
                "records_json=excluded.records_json, authorities_json=excluded.authorities_json, "
                "query_id=excluded.query_id, updated_at_ms=excluded.updated_at_ms",
                (
                    entry.cache_key,
                    entry.qname,
                    entry.qtype,
                    entry.qclass,
                    entry.kind,
                    entry.negative_kind,
                    entry.rcode,
                    entry.source_type,
                    entry.written_at_ms,
                    entry.original_ttl_ms,
                    entry.expires_at_ms,
                    json.dumps(entry.records, ensure_ascii=False, sort_keys=True),
                    json.dumps(entry.authorities, ensure_ascii=False, sort_keys=True),
                    entry.query_id,
                    entry.updated_at_ms,
                ),
            )
            connection.execute(
                "UPDATE sessions SET state='complete', rcode=?, source_type=?, business_code=NULL, "
                "response_json=?, error_json=NULL, cache_hit=0, updated_at_ms=? WHERE query_id=?",
                (
                    rcode,
                    source_type,
                    json.dumps(response, ensure_ascii=False, sort_keys=True),
                    now_ms(),
                    query_id,
                ),
            )
            connection.execute(
                "UPDATE intent_log SET status='committed', committed_at_ms=? WHERE intent_id=? AND status='pending'",
                (now_ms(), intent_id),
            )

        await self._write(operation)

    async def get_cache(self, cache_key: str) -> CacheEntry | None:
        def operation(connection: sqlite3.Connection) -> CacheEntry | None:
            row = connection.execute("SELECT * FROM cache_entries WHERE cache_key=?", (cache_key,)).fetchone()
            return None if row is None else _cache_row(row)

        return await self._read(operation)

    async def get_cache_by_name(
        self,
        qname: str,
        qtype: int | None = None,
        *,
        include_expired: bool = False,
    ) -> list[CacheEntry]:
        def operation(connection: sqlite3.Connection) -> list[CacheEntry]:
            if qtype is None:
                rows = connection.execute(
                    "SELECT * FROM cache_entries WHERE qname=? ORDER BY qtype", (qname,)
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM cache_entries WHERE qname=? AND qtype=? ORDER BY qtype", (qname, qtype)
                ).fetchall()
            entries = [_cache_row(row) for row in rows]
            if include_expired:
                return entries
            instant = now_ms()
            return [entry for entry in entries if entry.expires_at_ms > instant]

        return await self._read(operation)

    async def list_cache(self, *, include_expired: bool = False, limit: int = 500) -> list[CacheEntry]:
        def operation(connection: sqlite3.Connection) -> list[CacheEntry]:
            rows = connection.execute(
                "SELECT * FROM cache_entries ORDER BY updated_at_ms DESC LIMIT ?",
                (max(1, min(limit, 5000)),),
            ).fetchall()
            entries = [_cache_row(row) for row in rows]
            if include_expired:
                return entries
            instant = now_ms()
            return [entry for entry in entries if entry.expires_at_ms > instant]

        return await self._read(operation)

    async def cleanup_expired(self, *, now: int | None = None) -> int:
        timestamp = now_ms() if now is None else now

        def operation(connection: sqlite3.Connection) -> int:
            cursor = connection.execute("DELETE FROM cache_entries WHERE expires_at_ms<=?", (timestamp,))
            return int(cursor.rowcount)

        return await self._write(operation)

    async def start_round(self, round_id: str, budget_ms: int, started_at_ms: int) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                "INSERT INTO rounds(round_id, started_at_ms, budget_ms, status, created_at_ms) "
                "VALUES(?,?,?,'running',?) ON CONFLICT(round_id) DO UPDATE SET started_at_ms=excluded.started_at_ms, "
                "budget_ms=excluded.budget_ms, status='running', finished_at_ms=NULL, trace_json=NULL, "
                "created_at_ms=excluded.created_at_ms",
                (round_id, started_at_ms, budget_ms, now_ms()),
            )

        await self._write(operation)

    async def finish_round(self, round_id: str, trace: dict[str, Any], status: str = "complete") -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                "UPDATE rounds SET finished_at_ms=?, status=?, trace_json=? WHERE round_id=?",
                (now_ms(), status, json.dumps(trace, ensure_ascii=False, sort_keys=True), round_id),
            )

        await self._write(operation)

    async def get_round(self, round_id: str) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection) -> dict[str, Any] | None:
            row = connection.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None:
                return None
            return {
                "round_id": row["round_id"],
                "started_at_ms": row["started_at_ms"],
                "finished_at_ms": row["finished_at_ms"],
                "budget_ms": row["budget_ms"],
                "status": row["status"],
                "trace": _decode_json(row["trace_json"], None),
            }

        return await self._read(operation)

    async def list_sessions_for_round(self, round_id: str) -> list[dict[str, Any]]:
        def operation(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = connection.execute(
                "SELECT * FROM sessions WHERE round_id=? ORDER BY created_at_ms, query_id", (round_id,)
            ).fetchall()
            return [_session_row(row, include_payload=False) for row in rows]

        return await self._read(operation)

    async def session_with_round(self, query_id: str) -> dict[str, Any] | None:
        session = await self.get_session(query_id)
        if session is None:
            return None
        if session.get("round_id"):
            session["round"] = await self.get_round(session["round_id"])
        return session


def _cache_row(row: sqlite3.Row) -> CacheEntry:
    return CacheEntry(
        cache_key=row["cache_key"],
        qname=row["qname"],
        qtype=int(row["qtype"]),
        qclass=int(row["qclass"]),
        kind=row["kind"],
        negative_kind=row["negative_kind"],
        rcode=int(row["rcode"]),
        source_type=None if row["source_type"] is None else int(row["source_type"]),
        written_at_ms=int(row["written_at_ms"]),
        original_ttl_ms=int(row["original_ttl_ms"]),
        expires_at_ms=int(row["expires_at_ms"]),
        records=json.loads(row["records_json"]),
        authorities=json.loads(row["authorities_json"]),
        query_id=row["query_id"],
        updated_at_ms=int(row["updated_at_ms"]),
    )


def _session_row(row: sqlite3.Row, *, include_payload: bool) -> dict[str, Any]:
    result: dict[str, Any] = {
        "query_id": row["query_id"],
        "fingerprint": row["fingerprint"],
        "name": row["qname"],
        "type": int(row["qtype"]),
        "qclass": int(row["qclass"]),
        "state": row["state"],
        "business_code": row["business_code"],
        "rcode": None if row["rcode"] is None else int(row["rcode"]),
        "source_record_type": None if row["source_type"] is None else int(row["source_type"]),
        "round_id": row["round_id"],
        "transport": row["transport"],
        "client": row["client"],
        "cache_hit": bool(row["cache_hit"]),
        "created_at_ms": int(row["created_at_ms"]),
        "updated_at_ms": int(row["updated_at_ms"]),
    }
    if include_payload:
        result["response"] = _decode_json(row["response_json"], None)
        result["error"] = _decode_json(row["error_json"], None)
    return result


def cache_entry_from_outcome(
    *,
    spec: QuerySpec,
    written_at_ms: int,
    ttl_ms: int,
    rcode: int,
    source_type: int | None,
    kind: str,
    negative_kind: str | None,
    answers: list[dict[str, Any]],
    authorities: list[dict[str, Any]],
) -> CacheEntry:
    return CacheEntry(
        cache_key=spec.cache_key,
        qname=spec.name,
        qtype=spec.qtype,
        qclass=spec.qclass,
        kind=kind,
        negative_kind=negative_kind,
        rcode=rcode,
        source_type=source_type,
        written_at_ms=written_at_ms,
        original_ttl_ms=ttl_ms,
        expires_at_ms=written_at_ms + ttl_ms,
        records=answers,
        authorities=authorities,
        query_id=spec.query_id,
        updated_at_ms=written_at_ms,
    )


__all__ = [
    "CacheEntry",
    "Storage",
    "cache_entry_from_outcome",
    "records_from_json",
    "records_to_json",
]

