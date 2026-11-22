"""SQLite persistence, two-phase intent journal, and write helpers."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from .errors import E_DATABASE, SchedulerError
from .state_machine import ensure_transition


_JOURNAL_LOCKS: dict[str, threading.Lock] = {}
_JOURNAL_LOCKS_GUARD = threading.Lock()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


class WriteContext:
    """Helpers available only inside the atomic business transaction."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        op_id: str,
        op_seq: int,
        kind: str,
        payload: Mapping[str, Any] | None,
    ) -> None:
        self.conn = conn
        self.op_id = op_id
        self.op_seq = op_seq
        self.kind = kind
        self.payload = dict(payload or {})
        self.now = utc_now()

    def next_counter(self, key: str, *, initial: int = 0) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        current = int(row["value"]) if row is not None else initial
        following = current + 1
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(following)),
        )
        return following

    def transition_request(
        self,
        request_id: str,
        to_state: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        row = self.conn.execute(
            "SELECT status FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise SchedulerError(
                "E_REQUEST_NOT_FOUND",
                f"request {request_id!r} does not exist",
                details={"request_id": request_id},
            )
        from_state = str(row["status"])
        if from_state == to_state:
            return False
        ensure_transition(from_state, to_state)
        self.conn.execute(
            "UPDATE requests SET status = ?, last_reason = ?, updated_at = ? "
            "WHERE request_id = ?",
            (to_state, reason, self.now, request_id),
        )
        self.conn.execute(
            "INSERT INTO state_transitions("
            "request_id, from_state, to_state, reason, op_seq, op_id, metadata_json, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                request_id,
                from_state,
                to_state,
                reason,
                self.op_seq,
                self.op_id,
                json_dumps(metadata or {}),
                self.now,
            ),
        )
        return True

    def record_initial_state(
        self,
        request_id: str,
        state: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT INTO state_transitions("
            "request_id, from_state, to_state, reason, op_seq, op_id, metadata_json, created_at"
            ") VALUES (?, NULL, ?, ?, ?, ?, ?, ?)",
            (
                request_id,
                state,
                reason,
                self.op_seq,
                self.op_id,
                json_dumps(metadata or {}),
                self.now,
            ),
        )

    def batch_event(
        self,
        batch_id: str,
        from_status: str | None,
        to_status: str,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT INTO batch_events("
            "batch_id, from_status, to_status, reason, op_seq, op_id, metadata_json, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                batch_id,
                from_status,
                to_status,
                reason,
                self.op_seq,
                self.op_id,
                json_dumps(metadata or {}),
                self.now,
            ),
        )


class Database:
    """One writer at a time, many WAL readers, every business write journaled."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(Path(path).resolve())
        self.intent_journal_path = f"{self.path}.intents.jsonl"
        self.schema_path = Path(__file__).with_name("schema.sql")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    def _journal_lock(self) -> threading.Lock:
        with _JOURNAL_LOCKS_GUARD:
            return _JOURNAL_LOCKS.setdefault(self.intent_journal_path, threading.Lock())

    def append_intent_journal(self, record: Mapping[str, Any]) -> None:
        line = json_dumps({"journaled_at": utc_now(), **dict(record)}) + "\n"
        with self._journal_lock():
            with open(self.intent_journal_path, "a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())

    def connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 15000")
        if readonly:
            # query_only works with normal Windows drive-letter paths and also
            # prevents an accidental write if a read helper is misused.
            conn.execute("PRAGMA query_only = ON")
        else:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("PRAGMA wal_autocheckpoint = 1000")
        return conn

    def ensure_schema(self) -> None:
        sql_text = self.schema_path.read_text(encoding="utf-8")
        conn = self.connect()
        try:
            conn.executescript(sql_text)
            conn.execute(
                "INSERT INTO meta(key, value) VALUES ('schema_version', '1') "
                "ON CONFLICT(key) DO NOTHING"
            )
            for key, value in (
                ("op_seq", "0"),
                ("submission_seq", "0"),
                ("batch_seq", "0"),
                ("allocation_seq", "0"),
                ("round_seq", "0"),
            ):
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO NOTHING",
                    (key, value),
                )
        finally:
            conn.close()
        Path(self.intent_journal_path).touch(exist_ok=True)

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect(readonly=True)
        try:
            conn.execute("BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _get_meta(self, conn: sqlite3.Connection, key: str, default: int = 0) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return int(row["value"]) if row is not None else default

    def prepare_intent(
        self,
        kind: str,
        *,
        request_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            op_seq = self._get_meta(conn, "op_seq") + 1
            op_id = f"op-{op_seq:08d}"
            now = utc_now()
            conn.execute(
                "INSERT INTO intents("
                "op_id, op_seq, kind, request_id, payload_json, status, created_at"
                ") VALUES (?, ?, ?, ?, ?, 'PREPARED', ?)",
                (op_id, op_seq, kind, request_id, json_dumps(payload or {}), now),
            )
            conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'op_seq'", (str(op_seq),)
            )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        try:
            self.append_intent_journal(
                {
                    "event": "PREPARED",
                    "op_id": op_id,
                    "op_seq": op_seq,
                    "kind": kind,
                    "request_id": request_id,
                    "payload": dict(payload or {}),
                }
            )
        except OSError as exc:
            conn = self.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE intents SET status = 'ABORTED', error_json = ?, "
                    "finished_at = ? WHERE op_id = ? AND status = 'PREPARED'",
                    (
                        json_dumps(
                            {
                                "code": E_DATABASE,
                                "message": f"cannot write intent journal: {exc}",
                            }
                        ),
                        utc_now(),
                        op_id,
                    ),
                )
                conn.execute("COMMIT")
            finally:
                conn.close()
            raise SchedulerError(
                E_DATABASE,
                f"cannot write intent journal {self.intent_journal_path}: {exc}",
                details={"op_id": op_id, "path": self.intent_journal_path},
            ) from exc
        return {"op_id": op_id, "op_seq": op_seq}

    def _maybe_failpoint(self, kind: str) -> None:
        value = os.environ.get("QUOTA_SCHEDULER_CRASH_AT", "").strip()
        if value and value in {kind, f"{kind}:before_commit", "before_commit", "*"}:
            # Deliberately no cleanup, signal handling, or Python finalisers.
            os._exit(137)
        delay = os.environ.get("QUOTA_SCHEDULER_TX_DELAY_MS", "").strip()
        if delay:
            try:
                time.sleep(max(0, int(delay)) / 1000.0)
            except ValueError:
                pass

    def mark_intent_aborted(self, op_id: str, error: BaseException) -> None:
        try:
            conn = self.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE intents SET status = 'ABORTED', error_json = ?, "
                    "finished_at = ? WHERE op_id = ? AND status = 'PREPARED'",
                    (json_dumps(error_to_record(error)), utc_now(), op_id),
                )
                conn.execute("COMMIT")
                try:
                    self.append_intent_journal(
                        {
                            "event": "ABORTED",
                            "op_id": op_id,
                            "error": error_to_record(error),
                        }
                    )
                except OSError:
                    pass
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    @contextmanager
    def operation(
        self,
        kind: str,
        *,
        request_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> Iterator[WriteContext]:
        """Write intent first, then execute all business changes atomically.

        If the process is killed after ``prepare_intent`` commits but before the
        business transaction commits, a PREPARED intent remains for recovery.
        If it is killed during the business transaction, SQLite rolls back all
        request, batch and reservation changes together.
        """

        op = self.prepare_intent(kind, request_id=request_id, payload=payload)
        conn = self.connect()
        tx: WriteContext | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status FROM intents WHERE op_id = ?", (op["op_id"],)
            ).fetchone()
            if row is None or row["status"] != "PREPARED":
                raise SchedulerError(
                    E_DATABASE,
                    "intent is not executable",
                    details={"op_id": op["op_id"], "status": row["status"] if row else None},
                )
            tx = WriteContext(
                conn, op["op_id"], int(op["op_seq"]), kind, payload
            )
            yield tx
            self._maybe_failpoint(kind)
            conn.execute(
                "UPDATE intents SET status = 'COMMITTED', finished_at = ? "
                "WHERE op_id = ?",
                (utc_now(), op["op_id"]),
            )
            conn.execute("COMMIT")
            try:
                self.append_intent_journal(
                    {
                        "event": "COMMITTED",
                        "op_id": op["op_id"],
                        "op_seq": int(op["op_seq"]),
                        "kind": kind,
                        "request_id": request_id,
                    }
                )
            except OSError:
                # The business transaction is already durable.  SQLite's
                # intents table remains the authoritative recovery source.
                pass
        except BaseException as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            conn.close()
            self.mark_intent_aborted(op["op_id"], exc)
            raise
        else:
            conn.close()


def error_to_record(error: BaseException) -> dict[str, Any]:
    if isinstance(error, SchedulerError):
        return error.to_dict()
    return {"code": E_DATABASE, "message": str(error), "type": type(error).__name__}


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return None if row is None else {key: row[key] for key in row.keys()}

