"""SQLite-backed positive/negative cache with single-writer commits and recovery."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .codec import QueryKey
from .errors import BusinessError, E_CACHE_WRITE
from .model import Record, Resolution


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True)
class CacheEntry:
    key: QueryKey
    kind: str
    rcode: int
    answers: tuple[Record, ...]
    authority: tuple[Record, ...]
    negative_ttl: int | None
    negative_ttl_source: str | None
    written_at: float
    expires_at: float
    source_types: tuple[str, ...]
    intent_id: int

    @property
    def remaining_ttl(self) -> int:
        return max(0, int(self.expires_at - time.time()))

    def resolution_at(self, now: float) -> Resolution:
        elapsed = max(0.0, now - self.written_at)
        base = Resolution(
            kind=self.kind,
            rcode=self.rcode,
            answers=self.answers,
            authority=self.authority,
            negative_ttl=self.negative_ttl,
            negative_ttl_source=self.negative_ttl_source,
        )
        return base.remaining_copy(elapsed)

    def to_payload(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "rcode": self.rcode,
            "answers": [record.to_dict() for record in self.answers],
            "authority": [record.to_dict() for record in self.authority],
            "negative_ttl": self.negative_ttl,
            "negative_ttl_source": self.negative_ttl_source,
        }

    def snapshot(self, now: float | None = None) -> dict[str, Any]:
        current = time.time() if now is None else now
        return {
            "key": self.key.name,
            "qtype": self.key.qtype,
            "qclass": self.key.qclass,
            "kind": self.kind,
            "rcode": self.rcode,
            "written_at": self.written_at,
            "expires_at": self.expires_at,
            "remaining_ttl": max(0, int(self.expires_at - current)),
            "source_types": list(self.source_types),
            "negative_ttl_source": self.negative_ttl_source,
            "answer_ttls": [record.ttl for record in self.answers],
            "authority_ttls": [record.ttl for record in self.authority],
            "intent_id": self.intent_id,
        }


class CacheStore:
    """A cache where SQLite is the source of truth and memory is a committed view."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._writer_lock = threading.RLock()
        self._db_lock = threading.RLock()
        self._mem_lock = threading.RLock()
        self._entries: dict[QueryKey, CacheEntry] = {}
        self._hits = 0
        self._misses = 0
        self._expired_removed = 0
        self._closed = False
        self._connection = sqlite3.connect(
            str(self.path), timeout=5.0, check_same_thread=False, isolation_level=None
        )
        self._connection.row_factory = sqlite3.Row
        with self._db_lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("PRAGMA busy_timeout=5000")
        self._create_schema()
        self.recovery_report = self.recover_incomplete()
        self._load_committed_entries()

    def _create_schema(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS resolution_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cache_key TEXT NOT NULL,
            qname TEXT NOT NULL,
            qtype INTEGER NOT NULL,
            qclass INTEGER NOT NULL,
            started_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            completed_at REAL,
            status TEXT NOT NULL CHECK(status IN ('running','committed','failed','interrupted')),
            business_code TEXT,
            chain_steps INTEGER NOT NULL DEFAULT 0,
            last_upstream_name TEXT,
            error_detail TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_status ON resolution_sessions(status);
        CREATE INDEX IF NOT EXISTS idx_sessions_key ON resolution_sessions(cache_key, started_at);

        CREATE TABLE IF NOT EXISTS cache_commit_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL REFERENCES resolution_sessions(id),
            cache_key TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','committed','abandoned')),
            created_at REAL NOT NULL,
            committed_at REAL,
            detail TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_intents_state ON cache_commit_intents(state);

        CREATE TABLE IF NOT EXISTS cache_entries (
            cache_key TEXT PRIMARY KEY,
            session_id INTEGER NOT NULL REFERENCES resolution_sessions(id),
            intent_id INTEGER NOT NULL REFERENCES cache_commit_intents(id),
            qname TEXT NOT NULL,
            qtype INTEGER NOT NULL,
            qclass INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK(kind IN ('positive','negative')),
            rcode INTEGER NOT NULL,
            written_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            remaining_ttl INTEGER NOT NULL,
            source_types TEXT NOT NULL,
            negative_ttl_source TEXT,
            payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cache_expiry ON cache_entries(expires_at);

        CREATE TABLE IF NOT EXISTS recovery_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_at REAL NOT NULL,
            report_json TEXT NOT NULL
        );
        """
        with self._db_lock:
            self._connection.executescript(schema)

    def _transaction(self) -> None:
        self._connection.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self._connection.execute("COMMIT")

    def _rollback(self) -> None:
        try:
            self._connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def start_session(self, key: QueryKey, upstream_name: str | None = None) -> int:
        now = time.time()
        with self._writer_lock, self._db_lock:
            self._transaction()
            try:
                cursor = self._connection.execute(
                    """
                    INSERT INTO resolution_sessions
                        (cache_key, qname, qtype, qclass, started_at, updated_at,
                         status, chain_steps, last_upstream_name)
                    VALUES (?, ?, ?, ?, ?, ?, 'running', 0, ?)
                    """,
                    (key.name, key.name, key.qtype, key.qclass, now, now, upstream_name),
                )
                session_id = int(cursor.lastrowid)
                self._connection.execute(
                    """
                    INSERT INTO cache_commit_intents
                        (session_id, cache_key, state, created_at, detail)
                    VALUES (?, ?, 'pending', ?, ?)
                    """,
                    (
                        session_id,
                        key.name,
                        now,
                        _json_dumps({"phase": "resolution_started", "qtype": key.qtype}),
                    ),
                )
                self._commit()
                return session_id
            except Exception:
                self._rollback()
                raise

    def update_session_progress(
        self, session_id: int, chain_steps: int, last_upstream_name: str
    ) -> None:
        now = time.time()
        with self._db_lock:
            self._connection.execute(
                """
                UPDATE resolution_sessions
                   SET updated_at=?, chain_steps=?, last_upstream_name=?
                 WHERE id=? AND status='running'
                """,
                (now, int(chain_steps), last_upstream_name, int(session_id)),
            )

    def fail_session(self, session_id: int, code: str, detail: str = "") -> None:
        now = time.time()
        with self._db_lock:
            self._transaction()
            try:
                self._connection.execute(
                    "UPDATE cache_commit_intents SET state='abandoned', committed_at=? WHERE session_id=? AND state='pending'",
                    (now, int(session_id)),
                )
                self._connection.execute(
                    """
                    UPDATE resolution_sessions
                       SET status='failed', completed_at=?, updated_at=?,
                           business_code=?, error_detail=?
                     WHERE id=? AND status='running'
                    """,
                    (now, now, code, detail, int(session_id)),
                )
                self._commit()
            except Exception:
                self._rollback()
                raise

    def commit_resolution(
        self, session_id: int, key: QueryKey, resolution: Resolution
    ) -> CacheEntry | None:
        """Persist an intent first, then atomically publish the complete cache entry."""

        entry = self._entry_from_resolution(session_id, key, resolution)
        if entry is None:
            now = time.time()
            with self._db_lock:
                self._transaction()
                try:
                    self._connection.execute(
                        "UPDATE cache_commit_intents SET state='abandoned', committed_at=? WHERE session_id=? AND state='pending'",
                        (now, int(session_id)),
                    )
                    self._connection.execute(
                        """
                        UPDATE resolution_sessions
                           SET status='committed', completed_at=?, updated_at=?
                         WHERE id=? AND status='running'
                        """,
                        (now, now, int(session_id)),
                    )
                    self._commit()
                except Exception:
                    self._rollback()
                    raise
            return None

        with self._writer_lock:
            now = time.time()
            with self._db_lock:
                row = self._connection.execute(
                    "SELECT id FROM cache_commit_intents WHERE session_id=? AND state='pending' ORDER BY id DESC LIMIT 1",
                    (int(session_id),),
                ).fetchone()
                if row is not None:
                    intent_id = int(row["id"])
                else:
                    self._transaction()
                    try:
                        cursor = self._connection.execute(
                            """
                            INSERT INTO cache_commit_intents
                                (session_id, cache_key, state, created_at, detail)
                            VALUES (?, ?, 'pending', ?, ?)
                            """,
                            (
                                int(session_id),
                                key.name,
                                now,
                                _json_dumps({"kind": entry.kind, "source_types": entry.source_types}),
                            ),
                        )
                        intent_id = int(cursor.lastrowid)
                        self._commit()
                    except Exception:
                        self._rollback()
                        raise

            try:
                payload = _json_dumps(entry.to_payload())
                with self._db_lock:
                    self._transaction()
                    try:
                        self._connection.execute(
                            """
                            INSERT INTO cache_entries
                                (cache_key, session_id, intent_id, qname, qtype, qclass,
                                 kind, rcode, written_at, expires_at, remaining_ttl,
                                 source_types, negative_ttl_source, payload)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(cache_key) DO UPDATE SET
                                session_id=excluded.session_id,
                                intent_id=excluded.intent_id,
                                qname=excluded.qname,
                                qtype=excluded.qtype,
                                qclass=excluded.qclass,
                                kind=excluded.kind,
                                rcode=excluded.rcode,
                                written_at=excluded.written_at,
                                expires_at=excluded.expires_at,
                                remaining_ttl=excluded.remaining_ttl,
                                source_types=excluded.source_types,
                                negative_ttl_source=excluded.negative_ttl_source,
                                payload=excluded.payload
                            """,
                            (
                                key.name,
                                int(session_id),
                                intent_id,
                                key.name,
                                key.qtype,
                                key.qclass,
                                entry.kind,
                                entry.rcode,
                                entry.written_at,
                                entry.expires_at,
                                entry.remaining_ttl,
                                ",".join(entry.source_types),
                                entry.negative_ttl_source,
                                payload,
                            ),
                        )
                        self._connection.execute(
                            "UPDATE cache_commit_intents SET state='committed', committed_at=? WHERE id=?",
                            (time.time(), intent_id),
                        )
                        self._connection.execute(
                            """
                            UPDATE resolution_sessions
                               SET status='committed', completed_at=?, updated_at=?
                             WHERE id=? AND status='running'
                            """,
                            (time.time(), time.time(), int(session_id)),
                        )
                        self._commit()
                    except Exception:
                        self._rollback()
                        raise
            except Exception as exc:
                with self._db_lock:
                    self._transaction()
                    try:
                        self._connection.execute(
                            "UPDATE cache_commit_intents SET state='abandoned', committed_at=? WHERE id=?",
                            (time.time(), intent_id),
                        )
                        self._commit()
                    except Exception:
                        self._rollback()
                raise BusinessError(E_CACHE_WRITE, str(exc)) from exc

            published = CacheEntry(
                key=entry.key,
                kind=entry.kind,
                rcode=entry.rcode,
                answers=entry.answers,
                authority=entry.authority,
                negative_ttl=entry.negative_ttl,
                negative_ttl_source=entry.negative_ttl_source,
                written_at=entry.written_at,
                expires_at=entry.expires_at,
                source_types=entry.source_types,
                intent_id=intent_id,
            )
            with self._mem_lock:
                self._entries[key] = published
            return published

    def _entry_from_resolution(
        self, session_id: int, key: QueryKey, resolution: Resolution
    ) -> CacheEntry | None:
        if resolution.is_error:
            return None
        now = time.time()
        lifetimes: list[int] = []
        if resolution.kind == "negative":
            if resolution.negative_ttl is None:
                return None
            lifetimes.append(int(resolution.negative_ttl))
            lifetimes.extend(record.ttl for record in resolution.answers if record.ttl > 0)
        else:
            lifetimes.extend(
                record.ttl for record in (*resolution.answers, *resolution.authority) if record.ttl > 0
            )
        if not lifetimes:
            return None
        lifetime = min(lifetimes)
        if lifetime <= 0:
            return None
        source_types = tuple(
            dict.fromkeys(
                record.type_name for record in (*resolution.answers, *resolution.authority)
            )
        )
        return CacheEntry(
            key=key,
            kind=resolution.kind,
            rcode=resolution.rcode,
            answers=resolution.answers,
            authority=resolution.authority,
            negative_ttl=resolution.negative_ttl,
            negative_ttl_source=resolution.negative_ttl_source,
            written_at=now,
            expires_at=now + lifetime,
            source_types=source_types,
            intent_id=int(session_id),
        )

    def get(self, key: QueryKey, now: float | None = None) -> Resolution | None:
        current = time.time() if now is None else now
        with self._mem_lock:
            entry = self._entries.get(key)
        if entry is not None and entry.expires_at > current:
            with self._mem_lock:
                self._hits += 1
            return entry.resolution_at(current)
        if entry is not None:
            self._remove_keys({key}, count_expired=True)
        with self._mem_lock:
            self._misses += 1
        return None

    def _remove_keys(self, keys: Iterable[QueryKey], *, count_expired: bool = False) -> int:
        key_list = list(keys)
        if not key_list:
            return 0
        names = [key.name for key in key_list]
        placeholders = ",".join("?" for _ in names)
        with self._writer_lock:
            with self._db_lock:
                self._transaction()
                try:
                    cursor = self._connection.execute(
                        f"DELETE FROM cache_entries WHERE cache_key IN ({placeholders})", names
                    )
                    removed = int(cursor.rowcount)
                    self._commit()
                except Exception:
                    self._rollback()
                    raise
            with self._mem_lock:
                for key in key_list:
                    self._entries.pop(key, None)
                if count_expired:
                    self._expired_removed += removed
        return removed

    def purge_expired(self, now: float | None = None) -> int:
        current = time.time() if now is None else now
        with self._writer_lock:
            with self._db_lock:
                rows = self._connection.execute(
                    "SELECT cache_key FROM cache_entries WHERE expires_at <= ?", (current,)
                ).fetchall()
                names = [row["cache_key"] for row in rows]
                if not names:
                    return 0
                placeholders = ",".join("?" for _ in names)
                self._transaction()
                try:
                    self._connection.execute(
                        f"DELETE FROM cache_entries WHERE cache_key IN ({placeholders})", names
                    )
                    self._commit()
                except Exception:
                    self._rollback()
                    raise
            with self._mem_lock:
                for key in list(self._entries):
                    if key.name in names:
                        self._entries.pop(key, None)
                self._expired_removed += len(names)
            return len(names)

    def _load_committed_entries(self) -> None:
        now = time.time()
        with self._db_lock:
            rows = self._connection.execute(
                """
                SELECT c.*
                  FROM cache_entries c
                  JOIN cache_commit_intents i ON i.id=c.intent_id AND i.state='committed'
                  JOIN resolution_sessions s ON s.id=c.session_id AND s.status='committed'
                 WHERE c.expires_at > ?
                """,
                (now,),
            ).fetchall()
        loaded: dict[QueryKey, CacheEntry] = {}
        for row in rows:
            payload = json.loads(row["payload"])
            key = QueryKey(row["qname"], int(row["qtype"]), int(row["qclass"]))
            loaded[key] = CacheEntry(
                key=key,
                kind=row["kind"],
                rcode=int(row["rcode"]),
                answers=tuple(Record.from_dict(item) for item in payload["answers"]),
                authority=tuple(Record.from_dict(item) for item in payload["authority"]),
                negative_ttl=payload["negative_ttl"],
                negative_ttl_source=payload["negative_ttl_source"],
                written_at=float(row["written_at"]),
                expires_at=float(row["expires_at"]),
                source_types=tuple(filter(None, row["source_types"].split(","))),
                intent_id=int(row["intent_id"]),
            )
        with self._mem_lock:
            self._entries = loaded

    def recover_incomplete(self) -> dict[str, Any]:
        """Recover only from durable rows; never infer work that was not persisted."""

        now = time.time()
        with self._writer_lock, self._db_lock:
            running = self._connection.execute(
                "SELECT * FROM resolution_sessions WHERE status='running' ORDER BY id"
            ).fetchall()
            pending = self._connection.execute(
                "SELECT * FROM cache_commit_intents WHERE state='pending' ORDER BY id"
            ).fetchall()
            uncommitted = self._connection.execute(
                """
                SELECT c.cache_key, c.intent_id, i.state AS intent_state, s.status AS session_status
                  FROM cache_entries c
                  LEFT JOIN cache_commit_intents i ON i.id=c.intent_id
                  LEFT JOIN resolution_sessions s ON s.id=c.session_id
                 WHERE i.state IS NULL OR i.state!='committed' OR s.status IS NULL OR s.status!='committed'
                 ORDER BY c.cache_key
                """
            ).fetchall()
            expired_count = int(
                self._connection.execute(
                    "SELECT COUNT(*) AS n FROM cache_entries WHERE expires_at <= ?", (now,)
                ).fetchone()["n"]
            )
            self._transaction()
            try:
                self._connection.execute(
                    """
                    UPDATE resolution_sessions
                       SET status='interrupted', completed_at=?, updated_at=?,
                           business_code=COALESCE(business_code,'E_PROCESS_INTERRUPTED'),
                           error_detail=COALESCE(error_detail,
                               'process ended before an atomic cache commit')
                     WHERE status='running'
                    """,
                    (now, now),
                )
                self._connection.execute(
                    "UPDATE cache_commit_intents SET state='abandoned', committed_at=? WHERE state='pending'",
                    (now,),
                )
                self._connection.execute(
                    """
                    DELETE FROM cache_entries
                     WHERE cache_key IN (
                         SELECT c.cache_key
                           FROM cache_entries c
                           LEFT JOIN cache_commit_intents i ON i.id=c.intent_id
                           LEFT JOIN resolution_sessions s ON s.id=c.session_id
                          WHERE i.state IS NULL OR i.state!='committed'
                             OR s.status IS NULL OR s.status!='committed'
                     )
                    """
                )
                self._connection.execute("DELETE FROM cache_entries WHERE expires_at <= ?", (now,))
                committed_sessions = int(
                    self._connection.execute(
                        "SELECT COUNT(*) AS n FROM resolution_sessions WHERE status='committed'"
                    ).fetchone()["n"]
                )
                committed_entries = int(
                    self._connection.execute("SELECT COUNT(*) AS n FROM cache_entries").fetchone()["n"]
                )
                report = {
                    "generated_at": now,
                    "source": "sqlite",
                    "interrupted_sessions": [
                        {
                            "session_id": int(row["id"]),
                            "cache_key": row["cache_key"],
                            "started_at": float(row["started_at"]),
                            "chain_steps": int(row["chain_steps"]),
                            "last_upstream_name": row["last_upstream_name"],
                            "state": "incomplete",
                        }
                        for row in running
                    ],
                    "abandoned_intents": [
                        {
                            "intent_id": int(row["id"]),
                            "session_id": int(row["session_id"]),
                            "cache_key": row["cache_key"],
                            "state": "abandoned",
                        }
                        for row in pending
                    ],
                    "removed_uncommitted_cache_entries": [
                        {
                            "cache_key": row["cache_key"],
                            "intent_id": int(row["intent_id"]),
                            "intent_state": row["intent_state"],
                            "session_status": row["session_status"],
                        }
                        for row in uncommitted
                    ],
                    "expired_cache_entries_removed": expired_count,
                    "committed_sessions_retained": committed_sessions,
                    "persisted_cache_entries_retained": committed_entries,
                }
                self._connection.execute(
                    "INSERT INTO recovery_reports(generated_at, report_json) VALUES (?, ?)",
                    (now, _json_dumps(report)),
                )
                self._commit()
            except Exception:
                self._rollback()
                raise
        return report

    def latest_recovery_report(self) -> dict[str, Any]:
        with self._db_lock:
            row = self._connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return {
                "source": "sqlite",
                "interrupted_sessions": [],
                "abandoned_intents": [],
                "removed_uncommitted_cache_entries": [],
                "expired_cache_entries_removed": 0,
            }
        return json.loads(row["report_json"])

    def snapshot(self) -> list[dict[str, Any]]:
        now = time.time()
        with self._mem_lock:
            entries = list(self._entries.values())
        return [entry.snapshot(now) for entry in sorted(entries, key=lambda item: item.key.as_tuple())]

    def sessions(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._db_lock:
            rows = self._connection.execute(
                "SELECT * FROM resolution_sessions ORDER BY id DESC LIMIT ?", (int(limit),)
            ).fetchall()
        return [dict(row) for row in rows]

    def intents(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._db_lock:
            rows = self._connection.execute(
                "SELECT * FROM cache_commit_intents ORDER BY id DESC LIMIT ?", (int(limit),)
            ).fetchall()
        return [dict(row) for row in rows]

    def stats(self) -> dict[str, Any]:
        with self._mem_lock:
            hits = self._hits
            misses = self._misses
            entries = len(self._entries)
            expired_removed = self._expired_removed
        return {
            "entries": entries,
            "hits": hits,
            "misses": misses,
            "expired_removed": expired_removed,
        }

    def close(self) -> None:
        with self._writer_lock, self._db_lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()
