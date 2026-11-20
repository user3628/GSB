"""MVCC visibility, edit-session, lock, savepoint, and recovery engine."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .config import Settings
from .errors import AppError, fail
from .storage import Storage, parse_json


ISOLATIONS = {"READ_COMMITTED", "REPEATABLE_READ", "SNAPSHOT"}
FIXED_SNAPSHOT_ISOLATIONS = {"REPEATABLE_READ", "SNAPSHOT"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,160}$")


def _now() -> float:
    return time.time()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _canonical_hash(payload: Any) -> str:
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


@dataclass(slots=True)
class TransactionState:
    txn_id: str
    txn_seq: int
    isolation: str
    status: str = "active"
    snapshot_seq: int | None = None
    locks: set[tuple[str, str]] = field(default_factory=set)
    abort_reason: str | None = None


@dataclass(slots=True)
class Waiter:
    txn_id: str
    key: tuple[str, str]
    holder_txn: str | None
    enqueued_mono: float
    deadline_mono: float
    status: str = "waiting"


@dataclass(slots=True)
class LockState:
    holder: str | None = None
    queue: deque[Waiter] = field(default_factory=deque)


class MVCCEngine:
    """Single-writer/multi-reader MVCC engine.

    The condition variable protects both the in-memory lock/wait graph and the
    single-writer critical section.  Readers do not need that mutex and read a
    complete SQLite commit through WAL snapshots.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.storage = Storage(settings)
        self.storage.initialize()
        self._cv = threading.Condition(threading.RLock())
        self._txns: dict[str, TransactionState] = {}
        self._locks: dict[tuple[str, str], LockState] = {}
        self._waiting: dict[str, Waiter] = {}
        self._waits_for: dict[str, str] = {}
        self._lock_intent_ids: dict[tuple[str, str], int] = {}
        self._started_at = _now()
        self._recovery_report: dict[str, Any] = {}
        self._recovery_report = self._recover_on_start()

    # ------------------------------------------------------------------
    # Public transaction API
    # ------------------------------------------------------------------
    def open_transaction(self, isolation: str, txn_id: str | None = None) -> dict[str, Any]:
        iso = self._normalize_isolation(isolation)
        requested_id = self._validate_optional_identifier(txn_id, "txn_id")
        with self._cv:
            if requested_id is not None:
                existing = self._load_txn_row(requested_id)
                if existing is not None:
                    if existing["isolation"] != iso:
                        raise fail(
                            "E_CONFLICTING_TXN",
                            "the transaction id already exists with a different isolation level",
                            txn_id=requested_id,
                            existing_isolation=existing["isolation"],
                            requested_isolation=iso,
                        )
                    return self._txn_public(existing, replayed=True)

            if len(self._txns) >= self.settings.max_active_txns:
                raise fail(
                    "E_TXN_LIMIT",
                    "global active transaction budget is exhausted",
                    max_active_txns=self.settings.max_active_txns,
                    active_txns=len(self._txns),
                )

            new_txn_id = requested_id or f"txn-{uuid.uuid4().hex}"
            started_at = _now()
            self.storage.record_intent(
                new_txn_id,
                "txn_open",
                {"isolation": iso, "txn_id": new_txn_id},
            )
            conn = self.storage.connect()
            txn_seq: int
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute("SELECT value FROM meta WHERE key='next_txn_seq'").fetchone()
                txn_seq = int(row["value"])
                conn.execute(
                    "UPDATE meta SET value=? WHERE key='next_txn_seq'",
                    (str(txn_seq + 1),),
                )
                conn.execute(
                    "INSERT INTO transactions(txn_id,txn_seq,isolation,status,started_at) VALUES(?,?,?,?,?)",
                    (new_txn_id, txn_seq, iso, "active", started_at),
                )
                conn.execute("COMMIT")
            except sqlite3.IntegrityError:
                conn.execute("ROLLBACK")
                raise fail(
                    "E_CONFLICTING_TXN",
                    "transaction id was concurrently created",
                    txn_id=new_txn_id,
                )
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

            state = TransactionState(
                txn_id=new_txn_id,
                txn_seq=txn_seq,
                isolation=iso,
                status="active",
                snapshot_seq=None,
            )
            self._txns[new_txn_id] = state
            return {
                "ok": True,
                "txn_id": new_txn_id,
                "txn_seq": txn_seq,
                "isolation": iso,
                "status": "active",
                "snapshot_id": None,
                "started_at": started_at,
                "replayed": False,
                "budgets": {
                    "max_active_txns": self.settings.max_active_txns,
                    "max_waiters": self.settings.max_waiters,
                    "lock_wait_ms": self.settings.lock_wait_ms,
                    "edit_budget_ms": self.settings.edit_budget_ms,
                },
            }

    def get_transaction(self, txn_id: str) -> dict[str, Any]:
        with self._cv:
            state = self._txns.get(txn_id)
            if state is not None:
                return {
                    "ok": True,
                    "txn_id": state.txn_id,
                    "txn_seq": state.txn_seq,
                    "isolation": state.isolation,
                    "status": state.status,
                    "snapshot_id": self._snapshot_id(state),
                    "snapshot_seq": state.snapshot_seq,
                    "locks": [self._lock_context(key) for key in sorted(state.locks)],
                    "abort_reason": state.abort_reason,
                }
            row = self._load_txn_row(txn_id)
            if row is None:
                raise fail("E_TXN_NOT_FOUND", "transaction does not exist", txn_id=txn_id)
            return self._txn_public(row, replayed=False)

    def read_row(self, txn_id: str, table: str, row_id: str) -> dict[str, Any]:
        table, row_id = self._validate_location(table, row_id)
        with self._cv:
            state = self._require_active(txn_id)
            snapshot_seq, snapshot_id = self._statement_snapshot_locked(state)
        conn = self.storage.connect()
        try:
            conn.execute("BEGIN")
            version = self._visible_version(conn, state.txn_id, snapshot_seq, table, row_id)
            conn.execute("COMMIT")
        finally:
            conn.close()
        return {
            "ok": True,
            "txn_id": txn_id,
            "isolation": state.isolation,
            "snapshot_id": snapshot_id,
            "row": self._serialize_version(version) if version is not None else None,
        }

    def query_rows(self, txn_id: str, table: str) -> dict[str, Any]:
        table = self._validate_identifier(table, "table")
        with self._cv:
            state = self._require_active(txn_id)
            snapshot_seq, snapshot_id = self._statement_snapshot_locked(state)
        conn = self.storage.connect()
        try:
            conn.execute("BEGIN")
            rows = conn.execute(
                "SELECT rv.*, c.commit_seq AS created_commit_seq, d.commit_seq AS deleted_commit_seq "
                "FROM row_versions rv "
                "LEFT JOIN committed_transactions c ON c.txn_id=rv.created_txn "
                "LEFT JOIN committed_transactions d ON d.txn_id=rv.deleted_txn "
                "WHERE rv.table_name=? "
                "ORDER BY rv.row_id ASC, rv.row_version DESC, rv.version_id DESC",
                (table,),
            ).fetchall()
            conn.execute("COMMIT")
        finally:
            conn.close()
        selected: dict[str, sqlite3.Row] = {}
        for row in rows:
            if row["row_id"] in selected:
                continue
            if self._row_visible(row, state.txn_id, snapshot_seq):
                selected[row["row_id"]] = row
        return {
            "ok": True,
            "txn_id": txn_id,
            "table": table,
            "isolation": state.isolation,
            "snapshot_id": snapshot_id,
            "rows": [self._serialize_version(selected[key]) for key in sorted(selected)],
            "count": len(selected),
        }

    def list_tables(self) -> dict[str, Any]:
        conn = self.storage.connect()
        try:
            rows = conn.execute(
                "SELECT table_name, COUNT(DISTINCT row_id) AS logical_rows FROM row_versions GROUP BY table_name ORDER BY table_name"
            ).fetchall()
        finally:
            conn.close()
        return {
            "ok": True,
            "tables": [dict(row) for row in rows],
        }

    def open_edit_session(self, txn_id: str, table: str, row_id: str) -> dict[str, Any]:
        table, row_id = self._validate_location(table, row_id)
        session_id = f"edit-{uuid.uuid4().hex}"
        with self._cv:
            state = self._require_active(txn_id)
            snapshot_seq, snapshot_id = self._statement_snapshot_locked(state)
            opened_at = _now()
            session = {
                "session_id": session_id,
                "txn_id": txn_id,
                "table_name": table,
                "row_id": row_id,
                "snapshot_id": snapshot_id,
                "snapshot_seq": snapshot_seq,
                "opened_at": opened_at,
                "expires_at": opened_at + self.settings.edit_budget_ms / 1000.0,
                "budget_ms": self.settings.edit_budget_ms,
            }
            self.storage.record_intent(txn_id, "edit_open", session)
            conn = self.storage.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                version = self._visible_version(conn, txn_id, snapshot_seq, table, row_id)
                if version is None:
                    conn.execute("ROLLBACK")
                    raise fail(
                        "E_ROW_NOT_FOUND",
                        "no visible row version exists for the requested edit session",
                        txn_id=txn_id,
                        table=table,
                        row_id=row_id,
                        snapshot_id=snapshot_id,
                    )
                conn.execute(
                    "INSERT INTO edit_sessions(session_id,txn_id,table_name,row_id,version_id,row_version,snapshot_id,snapshot_seq,status,opened_at,expires_at,budget_ms) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        session_id,
                        txn_id,
                        table,
                        row_id,
                        version["version_id"],
                        version["row_version"],
                        snapshot_id,
                        snapshot_seq,
                        "open",
                        opened_at,
                        session["expires_at"],
                        self.settings.edit_budget_ms,
                    ),
                )
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
            lock = self._lock_context((table, row_id))
            return {
                "ok": True,
                "txn_id": txn_id,
                "session_id": session_id,
                "status": "open",
                "isolation": state.isolation,
                "snapshot_id": snapshot_id,
                "snapshot_seq": snapshot_seq,
                "opened_at": opened_at,
                "expires_at": session["expires_at"],
                "edit_budget_ms": self.settings.edit_budget_ms,
                "row": self._serialize_version(version),
                "row_version": version["row_version"],
                "version_id": version["version_id"],
                "lock": lock,
            }

    def get_edit_session(self, txn_id: str, session_id: str) -> dict[str, Any]:
        with self._cv:
            self._require_active(txn_id)
            session = self._load_session_locked(txn_id, session_id)
            lock = self._lock_context((session["table_name"], session["row_id"]))
            remaining_ms = max(0, int((session["expires_at"] - _now()) * 1000))
            return {
                "ok": True,
                "session": self._serialize_session(session),
                "remaining_budget_ms": remaining_ms,
                "lock": lock,
            }

    def save_edit_session(
        self,
        txn_id: str,
        session_id: str,
        row_version: int,
        data: dict[str, Any] | None = None,
        delete: bool = False,
    ) -> dict[str, Any]:
        expected_row_version = self._validate_row_version(row_version)
        if not delete:
            self._validate_row_data(data)
        with self._cv:
            state = self._require_active(txn_id)
            session = self._load_session_locked(txn_id, session_id)
            if session["status"] not in {"open", "saved"}:
                raise fail(
                    "E_EDIT_SESSION_NOT_FOUND",
                    "edit session is no longer writable",
                    txn_id=txn_id,
                    session_id=session_id,
                    status=session["status"],
                )
            key = (session["table_name"], session["row_id"])
            self._raise_if_expired(session, "save")
            if int(session["row_version"]) != expected_row_version:
                latest = self._latest_editable_locked(state, *key)
                raise fail(
                    "E_EDIT_STALE",
                    "the supplied row version does not match the version captured when the editor opened",
                    txn_id=txn_id,
                    session_id=session_id,
                    supplied_row_version=expected_row_version,
                    session_row_version=int(session["row_version"]),
                    table=key[0],
                    row_id=key[1],
                    latest=self._serialize_version(latest) if latest is not None else None,
                )

            newly_acquired = False
            try:
                newly_acquired = self._acquire_lock_locked(state, key)
                self._raise_if_expired(session, "after_lock_wait")
                latest = self._latest_editable_locked(state, *key)
                if latest is None or int(latest["version_id"]) != int(session["version_id"]):
                    raise fail(
                        "E_EDIT_STALE",
                        "the version shown in the edit panel is no longer the current committed version",
                        txn_id=txn_id,
                        session_id=session_id,
                        opened_row_version=int(session["row_version"]),
                        opened_version_id=int(session["version_id"]),
                        supplied_row_version=expected_row_version,
                        latest=self._serialize_version(latest) if latest is not None else None,
                        latest_snapshot_id=self._current_snapshot_id_locked(),
                        stale_reason="superseded_or_deleted_after_editor_open",
                    )

                result = self._write_version_locked(
                    state,
                    key,
                    latest,
                    data=data if not delete else None,
                    delete=delete,
                    session=session,
                )
            except BaseException:
                if newly_acquired:
                    self._release_lock_locked(state.txn_id, key, "failed_save_release")
                raise
            result["session"] = self.get_edit_session(txn_id, session_id)["session"]
            result["lock"] = self._lock_context(key)
            return result

    def update_row(
        self,
        txn_id: str,
        table: str,
        row_id: str,
        row_version: int,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        table, row_id = self._validate_location(table, row_id)
        expected = self._validate_row_version(row_version)
        self._validate_row_data(data)
        key = (table, row_id)
        with self._cv:
            state = self._require_active(txn_id)
            self._statement_snapshot_locked(state)
            newly_acquired = False
            try:
                newly_acquired = self._acquire_lock_locked(state, key)
                latest = self._latest_editable_locked(state, table, row_id)
                if latest is None or int(latest["row_version"]) != expected:
                    raise self._stale_error(txn_id, table, row_id, expected, latest)
                return self._write_version_locked(
                    state,
                    key,
                    latest,
                    data=data,
                    delete=False,
                    session=None,
                )
            except BaseException:
                if newly_acquired:
                    self._release_lock_locked(txn_id, key, "failed_update_release")
                raise

    def delete_row(
        self,
        txn_id: str,
        table: str,
        row_id: str,
        row_version: int,
    ) -> dict[str, Any]:
        table, row_id = self._validate_location(table, row_id)
        expected = self._validate_row_version(row_version)
        key = (table, row_id)
        with self._cv:
            state = self._require_active(txn_id)
            self._statement_snapshot_locked(state)
            newly_acquired = False
            try:
                newly_acquired = self._acquire_lock_locked(state, key)
                latest = self._latest_editable_locked(state, table, row_id)
                if latest is None or int(latest["row_version"]) != expected:
                    raise self._stale_error(txn_id, table, row_id, expected, latest)
                return self._write_version_locked(
                    state,
                    key,
                    latest,
                    data=None,
                    delete=True,
                    session=None,
                )
            except BaseException:
                if newly_acquired:
                    self._release_lock_locked(txn_id, key, "failed_delete_release")
                raise

    def create_savepoint(self, txn_id: str, name: str) -> dict[str, Any]:
        name = self._validate_identifier(name, "savepoint name")
        with self._cv:
            state = self._require_active(txn_id)
            self._statement_snapshot_locked(state)
            intent_id = self.storage.record_intent(txn_id, "savepoint", {"name": name})
            conn = self.storage.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                op_row = conn.execute(
                    "SELECT COALESCE(MAX(op_seq),0) AS n FROM txn_write_ops WHERE txn_id=?",
                    (txn_id,),
                ).fetchone()
                sessions = conn.execute(
                    "SELECT * FROM edit_sessions WHERE txn_id=? ORDER BY session_id",
                    (txn_id,),
                ).fetchall()
                locks = [self._lock_context(key) for key in sorted(state.locks)]
                actual_intent_seq = int(
                    conn.execute("SELECT COALESCE(MAX(intent_id),0) FROM intents WHERE txn_id=?", (txn_id,)).fetchone()[0]
                )
                snapshot_seq = state.snapshot_seq
                if snapshot_seq is None:
                    snapshot_seq = self.storage.current_commit_seq(conn)
                conn.execute(
                    "DELETE FROM savepoints WHERE txn_id=? AND name=?",
                    (txn_id, name),
                )
                cur = conn.execute(
                    "INSERT INTO savepoints(txn_id,name,snapshot_seq,op_seq,intent_seq,sessions_json,locks_json,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (
                        txn_id,
                        name,
                        snapshot_seq,
                        int(op_row["n"]),
                        max(int(intent_id), actual_intent_seq),
                        _json([dict(row) for row in sessions]),
                        _json(locks),
                        _now(),
                    ),
                )
                savepoint_seq = int(cur.lastrowid)
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
            return {
                "ok": True,
                "txn_id": txn_id,
                "savepoint": name,
                "savepoint_seq": savepoint_seq,
                "snapshot_id": self._snapshot_id(state),
                "snapshot_seq": snapshot_seq,
                "locks_held": [self._lock_context(key) for key in sorted(state.locks)],
                "sessions_captured": len(sessions),
            }

    def rollback_to_savepoint(self, txn_id: str, name: str) -> dict[str, Any]:
        name = self._validate_identifier(name, "savepoint name")
        with self._cv:
            state = self._require_active(txn_id)
            conn = self.storage.connect()
            try:
                savepoint = conn.execute(
                    "SELECT * FROM savepoints WHERE txn_id=? AND name=?",
                    (txn_id, name),
                ).fetchone()
            finally:
                conn.close()
            if savepoint is None:
                raise fail(
                    "E_SAVEPOINT_NOT_FOUND",
                    "savepoint does not exist for this transaction",
                    txn_id=txn_id,
                    savepoint=name,
                )
            self.storage.record_intent(txn_id, "savepoint_rollback", {"name": name})
            sessions_snapshot = parse_json(savepoint["sessions_json"], [])
            locks_snapshot = parse_json(savepoint["locks_json"], [])
            snapshot_keys = {(item["table"], item["row_id"]) for item in locks_snapshot}
            undone_writes = 0
            conn = self.storage.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                ops = conn.execute(
                    "SELECT * FROM txn_write_ops WHERE txn_id=? AND op_seq>? ORDER BY op_seq DESC",
                    (txn_id, int(savepoint["op_seq"])),
                ).fetchall()
                for op in ops:
                    if op["new_version_id"] is not None:
                        conn.execute(
                            "DELETE FROM row_versions WHERE version_id=? AND created_txn=?",
                            (int(op["new_version_id"]), txn_id),
                        )
                    if op["old_version_id"] is not None:
                        conn.execute(
                            "UPDATE row_versions SET deleted_txn=? WHERE version_id=?",
                            (op["old_deleted_before"], int(op["old_version_id"])),
                        )
                    undone_writes += 1
                conn.execute(
                    "DELETE FROM txn_write_ops WHERE txn_id=? AND op_seq>?",
                    (txn_id, int(savepoint["op_seq"])),
                )
                conn.execute("DELETE FROM edit_sessions WHERE txn_id=?", (txn_id,))
                for session in sessions_snapshot:
                    self._insert_session_row(conn, session)
                conn.execute(
                    "UPDATE intents SET status='rolled_back', updated_at=? "
                    "WHERE txn_id=? AND intent_id>? AND status='open' AND kind<>'savepoint_rollback'",
                    (_now(), txn_id, int(savepoint["intent_seq"])),
                )
                conn.execute(
                    "DELETE FROM savepoints WHERE txn_id=? AND savepoint_seq>?",
                    (txn_id, int(savepoint["savepoint_seq"])),
                )
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

            released: list[dict[str, Any]] = []
            for key in list(state.locks):
                if key not in snapshot_keys:
                    released.append(self._lock_context(key))
                    self._release_lock_locked(txn_id, key, "savepoint_rollback")
            self._cv.notify_all()
            return {
                "ok": True,
                "txn_id": txn_id,
                "savepoint": name,
                "snapshot_id": self._snapshot_id(state),
                "snapshot_seq": state.snapshot_seq,
                "undone_writes": undone_writes,
                "restored_sessions": len(sessions_snapshot),
                "released_locks": released,
                "locks_held": [self._lock_context(key) for key in sorted(state.locks)],
            }

    def commit_transaction(self, txn_id: str, request_context: dict[str, Any] | None = None) -> dict[str, Any]:
        request_context = request_context or {}
        request_hash = _canonical_hash(request_context)
        with self._cv:
            row = self._load_txn_row(txn_id)
            if row is None:
                raise fail("E_TXN_NOT_FOUND", "transaction does not exist", txn_id=txn_id)
            if row["status"] == "committed":
                if row["commit_request_hash"] != request_hash:
                    raise fail(
                        "E_CONFLICTING_TXN",
                        "the transaction was already committed with different request content",
                        txn_id=txn_id,
                        committed_request_hash=row["commit_request_hash"],
                        received_request_hash=request_hash,
                    )
                result = parse_json(row["commit_result_json"], {})
                result["replayed"] = True
                return result
            if row["status"] != "active":
                raise fail(
                    "E_TXN_NOT_ACTIVE",
                    "transaction cannot be committed in its current state",
                    txn_id=txn_id,
                    status=row["status"],
                    abort_reason=row["abort_reason"],
                )
            state = self._txns.get(txn_id)
            if state is None:
                raise fail(
                    "E_TXN_NOT_ACTIVE",
                    "active transaction is not owned by this process",
                    txn_id=txn_id,
                    reason="not_owned_after_restart",
                )

            commit_intent = self.storage.record_intent(
                txn_id,
                "commit",
                {"request_hash": request_hash},
            )
            committed_at = _now()
            conn = self.storage.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                current = conn.execute(
                    "SELECT status FROM transactions WHERE txn_id=?",
                    (txn_id,),
                ).fetchone()
                if current is None or current["status"] != "active":
                    conn.execute("ROLLBACK")
                    raise fail(
                        "E_TXN_NOT_ACTIVE",
                        "transaction status changed before the single-writer commit",
                        txn_id=txn_id,
                    )
                meta = conn.execute("SELECT value FROM meta WHERE key='next_commit_seq'").fetchone()
                commit_seq = int(meta["value"])
                conn.execute(
                    "UPDATE meta SET value=? WHERE key='next_commit_seq'",
                    (str(commit_seq + 1),),
                )
                updated_versions = conn.execute(
                    "UPDATE row_versions SET commit_seq=? WHERE created_txn=? AND commit_seq IS NULL",
                    (commit_seq, txn_id),
                ).rowcount
                conn.execute(
                    "INSERT INTO committed_transactions(txn_id,commit_seq,committed_at) VALUES(?,?,?)",
                    (txn_id, commit_seq, committed_at),
                )
                committed_versions = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM row_versions WHERE created_txn=?",
                        (txn_id,),
                    ).fetchone()[0]
                )
                result = {
                    "ok": True,
                    "txn_id": txn_id,
                    "status": "committed",
                    "isolation": row["isolation"],
                    "commit_seq": commit_seq,
                    "committed_at": committed_at,
                    "versions_finalized": updated_versions,
                    "committed_versions": committed_versions,
                    "replayed": False,
                }
                conn.execute(
                    "UPDATE transactions SET status='committed', committed_at=?, snapshot_seq=?, commit_seq=?, "
                    "commit_request_hash=?, commit_result_json=? WHERE txn_id=?",
                    (
                        committed_at,
                        state.snapshot_seq,
                        commit_seq,
                        request_hash,
                        _json(result),
                        txn_id,
                    ),
                )
                conn.execute(
                    "UPDATE edit_sessions SET status='closed' WHERE txn_id=? AND status IN ('open','saved')",
                    (txn_id,),
                )
                conn.execute(
                    "UPDATE intents SET status='committed', updated_at=? WHERE txn_id=? AND status='open'",
                    (_now(), txn_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

            state.status = "committed"
            self._release_all_locks_locked(state, "commit")
            self._txns.pop(txn_id, None)
            self._cv.notify_all()
            return result

    def rollback_transaction(self, txn_id: str, reason: str = "client_request") -> dict[str, Any]:
        with self._cv:
            row = self._load_txn_row(txn_id)
            if row is None:
                raise fail("E_TXN_NOT_FOUND", "transaction does not exist", txn_id=txn_id)
            if row["status"] == "rolled_back":
                return {
                    "ok": True,
                    "txn_id": txn_id,
                    "status": "rolled_back",
                    "abort_reason": row["abort_reason"],
                    "replayed": True,
                }
            if row["status"] != "active":
                raise fail(
                    "E_TXN_NOT_ACTIVE",
                    "transaction cannot be rolled back in its current state",
                    txn_id=txn_id,
                    status=row["status"],
                )
            state = self._txns.get(txn_id)
            if state is None:
                raise fail(
                    "E_TXN_NOT_ACTIVE",
                    "active transaction is not owned by this process",
                    txn_id=txn_id,
                )
            self.storage.record_intent(txn_id, "rollback", {"reason": reason})
            self._abort_transaction_locked(
                state,
                reason=reason,
                final_status="rolled_back",
                intent_status="rolled_back",
            )
            return {
                "ok": True,
                "txn_id": txn_id,
                "status": "rolled_back",
                "abort_reason": reason,
                "replayed": False,
            }

    # ------------------------------------------------------------------
    # Diagnostics / shutdown / recovery
    # ------------------------------------------------------------------
    def locks_snapshot(self) -> dict[str, Any]:
        with self._cv:
            locks = []
            for key, lock in sorted(self._locks.items()):
                if lock.holder is None and not lock.queue:
                    continue
                locks.append(
                    {
                        **self._lock_context(key),
                        "holder_txn_id": lock.holder,
                        "queued_waiters": len(lock.queue),
                        "queue": [
                            {
                                "txn_id": waiter.txn_id,
                                "status": waiter.status,
                                "waited_ms": int((time.monotonic() - waiter.enqueued_mono) * 1000),
                            }
                            for waiter in lock.queue
                        ],
                    }
                )
            return {
                "ok": True,
                "locks": locks,
                "waiting_count": len(self._waiting),
                "wait_graph": dict(self._waits_for),
            }

    def stats(self) -> dict[str, Any]:
        with self._cv:
            return {
                "ok": True,
                "pid": __import__("os").getpid(),
                "uptime_seconds": round(_now() - self._started_at, 3),
                "active_txns": len(self._txns),
                "max_active_txns": self.settings.max_active_txns,
                "waiting_txns": len(self._waiting),
                "max_waiters": self.settings.max_waiters,
                "lock_wait_ms": self.settings.lock_wait_ms,
                "edit_budget_ms": self.settings.edit_budget_ms,
                "recovery": self._recovery_summary(),
            }

    def get_recovery_report(self) -> dict[str, Any]:
        return self._recovery_report

    def graceful_shutdown(self, reason: str = "graceful_shutdown") -> dict[str, Any]:
        with self._cv:
            active_ids = list(self._txns)
            for txn_id in active_ids:
                state = self._txns[txn_id]
                self._abort_transaction_locked(
                    state,
                    reason=reason,
                    final_status="rolled_back",
                    intent_status="rolled_back",
                )
            conn = self.storage.connect()
            try:
                committed = int(
                    conn.execute("SELECT COUNT(*) FROM transactions WHERE status='committed'").fetchone()[0]
                )
                committed_versions = int(
                    conn.execute("SELECT COUNT(*) FROM row_versions WHERE commit_seq IS NOT NULL").fetchone()[0]
                )
            finally:
                conn.close()
            report = {
                "report_kind": "graceful_shutdown",
                "complete_exit_report": True,
                "reason": reason,
                "generated_at": _now(),
                "active_transactions_rolled_back": active_ids,
                "committed_transactions_preserved": committed,
                "committed_row_versions_preserved": committed_versions,
                "graceful_exit_report_available": True,
            }
            self.storage.write_json_atomic(self.settings.shutdown_report_path, report)
            return report

    # ------------------------------------------------------------------
    # Snapshot and visibility helpers
    # ------------------------------------------------------------------
    def _normalize_isolation(self, isolation: Any) -> str:
        value = str(isolation or "").strip().upper().replace(" ", "_").replace("-", "_")
        if value not in ISOLATIONS:
            raise fail(
                "E_INVALID_ISOLATION",
                "unsupported isolation level",
                requested=isolation,
                allowed=sorted(ISOLATIONS),
            )
        return value

    def _statement_snapshot_locked(self, state: TransactionState) -> tuple[int, str]:
        if state.isolation == "READ_COMMITTED":
            seq = self.storage.current_commit_seq()
            return seq, f"read_committed:{seq}"
        if state.snapshot_seq is None:
            seq = self.storage.current_commit_seq()
            state.snapshot_seq = seq
            conn = self.storage.connect()
            try:
                conn.execute(
                    "UPDATE transactions SET snapshot_seq=? WHERE txn_id=? AND status='active'",
                    (seq, state.txn_id),
                )
            finally:
                conn.close()
            return seq, f"snapshot:{seq}"
        return state.snapshot_seq, f"snapshot:{state.snapshot_seq}"

    def _snapshot_id(self, state: TransactionState) -> str | None:
        if state.isolation == "READ_COMMITTED":
            return None
        return f"snapshot:{state.snapshot_seq}" if state.snapshot_seq is not None else None

    def _current_snapshot_id_locked(self) -> str:
        return f"current:{self.storage.current_commit_seq()}"

    def _visible_version(
        self,
        conn: sqlite3.Connection,
        txn_id: str,
        snapshot_seq: int,
        table: str,
        row_id: str,
    ) -> sqlite3.Row | None:
        rows = conn.execute(
            "SELECT rv.*, c.commit_seq AS created_commit_seq, d.commit_seq AS deleted_commit_seq "
            "FROM row_versions rv "
            "LEFT JOIN committed_transactions c ON c.txn_id=rv.created_txn "
            "LEFT JOIN committed_transactions d ON d.txn_id=rv.deleted_txn "
            "WHERE rv.table_name=? AND rv.row_id=? "
            "ORDER BY rv.row_version DESC, rv.version_id DESC",
            (table, row_id),
        ).fetchall()
        for row in rows:
            if self._row_visible(row, txn_id, snapshot_seq):
                return row
        return None

    def _row_visible(self, row: sqlite3.Row, txn_id: str, snapshot_seq: int) -> bool:
        created_visible = row["created_txn"] == txn_id or (
            row["created_commit_seq"] is not None and int(row["created_commit_seq"]) <= snapshot_seq
        )
        if not created_visible:
            return False
        if row["deleted_txn"] is None:
            return True
        if row["deleted_txn"] == txn_id:
            return False
        return not (
            row["deleted_commit_seq"] is not None
            and int(row["deleted_commit_seq"]) <= snapshot_seq
        )

    def _latest_editable_locked(
        self,
        state: TransactionState,
        table: str,
        row_id: str,
    ) -> sqlite3.Row | None:
        conn = self.storage.connect()
        try:
            conn.execute("BEGIN")
            snapshot_seq = self.storage.current_commit_seq(conn)
            version = self._visible_version(conn, state.txn_id, snapshot_seq, table, row_id)
            conn.execute("COMMIT")
            return version
        finally:
            conn.close()

    def _serialize_version(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "table": row["table_name"],
            "row_id": row["row_id"],
            "data": parse_json(row["row_data_json"], {}),
            "row_version": int(row["row_version"]),
            "version_id": int(row["version_id"]),
            "created_txn": row["created_txn"],
            "deleted_txn": row["deleted_txn"],
            "commit_seq": int(row["commit_seq"]) if row["commit_seq"] is not None else None,
            "created_commit_seq": (
                int(row["created_commit_seq"]) if row["created_commit_seq"] is not None else None
            ),
            "deleted_commit_seq": (
                int(row["deleted_commit_seq"]) if row["deleted_commit_seq"] is not None else None
            ),
        }

    # ------------------------------------------------------------------
    # Writes and sessions
    # ------------------------------------------------------------------
    def _write_version_locked(
        self,
        state: TransactionState,
        key: tuple[str, str],
        current: sqlite3.Row,
        data: dict[str, Any] | None,
        delete: bool,
        session: dict[str, Any] | None,
    ) -> dict[str, Any]:
        table, row_id = key
        txn_id = state.txn_id
        intent_id = self.storage.record_intent(
            txn_id,
            "row_write",
            {
                "table": table,
                "row_id": row_id,
                "old_version_id": int(current["version_id"]),
                "delete": delete,
            },
        )
        now = _now()
        conn = self.storage.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            old_version_id = int(current["version_id"])
            old_deleted_before = current["deleted_txn"]
            new_version_id: int | None = None
            new_row_version: int | None = None
            if delete:
                changed = conn.execute(
                    "UPDATE row_versions SET deleted_txn=? WHERE version_id=? AND deleted_txn IS NULL",
                    (txn_id, old_version_id),
                ).rowcount
                if changed != 1:
                    conn.execute("ROLLBACK")
                    raise fail(
                        "E_EDIT_STALE",
                        "the current row version changed while acquiring the write right",
                        txn_id=txn_id,
                        table=table,
                        row_id=row_id,
                        expected_version_id=old_version_id,
                    )
            else:
                row = conn.execute(
                    "SELECT COALESCE(MAX(row_version),0) AS n FROM row_versions WHERE table_name=? AND row_id=?",
                    (table, row_id),
                ).fetchone()
                new_row_version = int(row["n"]) + 1
                cur = conn.execute(
                    "INSERT INTO row_versions(table_name,row_id,row_version,row_data_json,created_txn,deleted_txn,commit_seq,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (table, row_id, new_row_version, _json(data or {}), txn_id, None, None, now),
                )
                new_version_id = int(cur.lastrowid)
                changed = conn.execute(
                    "UPDATE row_versions SET deleted_txn=? WHERE version_id=? AND deleted_txn IS NULL",
                    (txn_id, old_version_id),
                ).rowcount
                if changed != 1:
                    conn.execute("ROLLBACK")
                    raise fail(
                        "E_EDIT_STALE",
                        "the current row version changed while acquiring the write right",
                        txn_id=txn_id,
                        table=table,
                        row_id=row_id,
                        expected_version_id=old_version_id,
                    )
            op = conn.execute(
                "INSERT INTO txn_write_ops(txn_id,kind,table_name,row_id,old_version_id,old_deleted_before,new_version_id,created_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    txn_id,
                    "delete" if delete else "update",
                    table,
                    row_id,
                    old_version_id,
                    old_deleted_before,
                    new_version_id,
                    now,
                ),
            )
            op_seq = int(op.lastrowid)
            if session is not None:
                if delete:
                    conn.execute(
                        "UPDATE edit_sessions SET status='deleted',last_saved_at=?,saved_version_id=NULL WHERE session_id=?",
                        (now, session["session_id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE edit_sessions SET version_id=?,row_version=?,last_saved_at=?,saved_version_id=? WHERE session_id=?",
                        (new_version_id, new_row_version, now, new_version_id, session["session_id"]),
                    )
            conn.execute(
                "UPDATE intents SET updated_at=? WHERE intent_id=?",
                (now, intent_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        conn = self.storage.connect()
        try:
            snapshot_seq = self.storage.current_commit_seq(conn)
            version = self._visible_version(conn, txn_id, snapshot_seq, table, row_id)
        finally:
            conn.close()
        result = {
            "ok": True,
            "txn_id": txn_id,
            "table": table,
            "row_id": row_id,
            "operation": "delete" if delete else "update",
            "deleted": delete,
            "row": None if delete else self._serialize_version(version),
            "old_version": self._serialize_version(current),
            "new_version_id": new_version_id,
            "new_row_version": new_row_version,
            "op_seq": op_seq,
            "lock": self._lock_context(key),
        }
        if delete:
            result["row"] = None
        return result

    def _raise_if_expired(self, session: dict[str, Any], stage: str) -> None:
        now = _now()
        if now >= float(session["expires_at"]):
            raise fail(
                "E_EDIT_EXPIRED",
                "the edit session exceeded its interaction latency budget",
                txn_id=session["txn_id"],
                session_id=session["session_id"],
                table=session["table_name"],
                row_id=session["row_id"],
                opened_at=float(session["opened_at"]),
                save_observed_at=now,
                elapsed_ms=int((now - float(session["opened_at"])) * 1000),
                budget_ms=int(session["budget_ms"]),
                decision_basis="server_arrival_time_minus_persisted_opened_at",
                checked_stage=stage,
                row_version=int(session["row_version"]),
            )

    def _load_session_locked(self, txn_id: str, session_id: str) -> dict[str, Any]:
        if not IDENTIFIER_RE.match(session_id or ""):
            raise fail("E_INVALID_REQUEST", "invalid edit session id", session_id=session_id)
        conn = self.storage.connect()
        try:
            row = conn.execute(
                "SELECT * FROM edit_sessions WHERE session_id=? AND txn_id=?",
                (session_id, txn_id),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise fail(
                "E_EDIT_SESSION_NOT_FOUND",
                "edit session does not exist for this transaction",
                txn_id=txn_id,
                session_id=session_id,
            )
        return dict(row)

    def _serialize_session(self, session: dict[str, Any]) -> dict[str, Any]:
        return {
            "session_id": session["session_id"],
            "txn_id": session["txn_id"],
            "table": session["table_name"],
            "row_id": session["row_id"],
            "version_id": int(session["version_id"]),
            "row_version": int(session["row_version"]),
            "snapshot_id": session["snapshot_id"],
            "snapshot_seq": int(session["snapshot_seq"]),
            "status": session["status"],
            "opened_at": float(session["opened_at"]),
            "expires_at": float(session["expires_at"]),
            "budget_ms": int(session["budget_ms"]),
            "last_saved_at": float(session["last_saved_at"]) if session["last_saved_at"] is not None else None,
            "saved_version_id": int(session["saved_version_id"]) if session["saved_version_id"] is not None else None,
        }

    def _insert_session_row(self, conn: sqlite3.Connection, session: dict[str, Any]) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO edit_sessions(session_id,txn_id,table_name,row_id,version_id,row_version,snapshot_id,snapshot_seq,status,opened_at,expires_at,budget_ms,last_saved_at,saved_version_id) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                session["session_id"],
                session["txn_id"],
                session["table_name"],
                session["row_id"],
                int(session["version_id"]),
                int(session["row_version"]),
                session["snapshot_id"],
                int(session["snapshot_seq"]),
                session["status"],
                float(session["opened_at"]),
                float(session["expires_at"]),
                int(session["budget_ms"]),
                session.get("last_saved_at"),
                session.get("saved_version_id"),
            ),
        )

    def _stale_error(
        self,
        txn_id: str,
        table: str,
        row_id: str,
        expected_row_version: int,
        latest: sqlite3.Row | None,
    ) -> AppError:
        return fail(
            "E_EDIT_STALE",
            "the supplied row version is no longer current",
            txn_id=txn_id,
            table=table,
            row_id=row_id,
            supplied_row_version=expected_row_version,
            latest=self._serialize_version(latest) if latest is not None else None,
            latest_snapshot_id=self._current_snapshot_id_locked(),
            stale_reason="superseded_or_deleted_by_committed_transaction",
        )

    # ------------------------------------------------------------------
    # Locks, wait graph, and deadlock handling
    # ------------------------------------------------------------------
    def _acquire_lock_locked(self, state: TransactionState, key: tuple[str, str]) -> bool:
        if key in state.locks:
            return False
        lock = self._locks.setdefault(key, LockState())
        self._prune_wait_queue_locked(lock)
        if lock.holder is None and not lock.queue:
            lock.holder = state.txn_id
            state.locks.add(key)
            intent_id = self.storage.record_intent(
                state.txn_id,
                "lock_acquired",
                {"table": key[0], "row_id": key[1], "after_wait": False},
            )
            self._lock_intent_ids[(state.txn_id, key)] = intent_id
            return True

        holder_id = lock.holder
        if state.isolation == "READ_COMMITTED" and holder_id is not None and holder_id != state.txn_id:
            raise fail(
                "E_WRITE_CONFLICT",
                "another transaction holds the exclusive row write right",
                txn_id=state.txn_id,
                isolation=state.isolation,
                lock=self._lock_context(key),
                holder_txn_id=holder_id,
                queued_waiters=len(lock.queue),
                policy="READ_COMMITTED_FAIL_FAST",
            )

        if len(self._waiting) >= self.settings.max_waiters:
            raise fail(
                "E_LOCK_QUEUE_FULL",
                "the bounded lock wait queue has no capacity",
                txn_id=state.txn_id,
                lock=self._lock_context(key),
                holder_txn_id=holder_id,
                max_waiters=self.settings.max_waiters,
                waiting_count=len(self._waiting),
            )

        enqueued = time.monotonic()
        deadline = enqueued + self.settings.lock_wait_ms / 1000.0
        waiter = Waiter(
            txn_id=state.txn_id,
            key=key,
            holder_txn=holder_id,
            enqueued_mono=enqueued,
            deadline_mono=deadline,
            status="waiting",
        )
        lock.queue.append(waiter)
        self._waiting[state.txn_id] = waiter
        if holder_id is not None:
            self._waits_for[state.txn_id] = holder_id

        cycle = self._find_cycle_locked(state.txn_id)
        if cycle:
            victim_id = max(cycle, key=lambda item: self._txn_seq_for_locked(item))
            context = {
                "cycle": cycle,
                "victim_txn_id": victim_id,
                "requester_txn_id": state.txn_id,
                "victim_policy": "largest txn_seq in cycle (deterministic youngest victim)",
                "lock": self._lock_context(key),
            }
            if victim_id == state.txn_id:
                self._remove_waiter_locked(waiter, "victim")
                self._abort_transaction_locked(
                    state,
                    reason="deadlock_victim",
                    final_status="rolled_back",
                    intent_status="rolled_back",
                )
                self._cv.notify_all()
                raise AppError(
                    code="E_DEADLOCK_VICTIM",
                    message="transaction was selected as the deterministic deadlock victim and rolled back",
                    context=context,
                )
            victim_state = self._txns.get(victim_id)
            if victim_state is not None:
                self._abort_transaction_locked(
                    victim_state,
                    reason="deadlock_victim",
                    final_status="rolled_back",
                    intent_status="rolled_back",
                )
                self._cv.notify_all()
                if waiter.status == "acquired":
                    return True

        while True:
            if waiter.status == "acquired":
                return True
            if waiter.status == "timeout":
                raise fail(
                    "E_LOCK_TIMEOUT",
                    "timed out while waiting for the exclusive row write right",
                    txn_id=state.txn_id,
                    lock=self._lock_context(key),
                    holder_txn_id=lock.holder,
                    waited_ms=int((time.monotonic() - enqueued) * 1000),
                    wait_limit_ms=self.settings.lock_wait_ms,
                    wait_primitive="threading.Condition.wait(monotonic_remaining)",
                )
            current_state = self._txns.get(state.txn_id)
            if waiter.status == "victim" or (
                current_state is not None and current_state.abort_reason == "deadlock_victim"
            ):
                raise fail(
                    "E_DEADLOCK_VICTIM",
                    "transaction was selected as a deadlock victim",
                    txn_id=state.txn_id,
                    lock=self._lock_context(key),
                    cell="lock_wait",
                )
            if current_state is None or current_state.status != "active" or waiter.status == "canceled":
                raise fail(
                    "E_TXN_CANCELED",
                    "transaction was canceled while waiting for the row write right",
                    txn_id=state.txn_id,
                    lock=self._lock_context(key),
                    waited_ms=int((time.monotonic() - enqueued) * 1000),
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                waited_ms = int((time.monotonic() - enqueued) * 1000)
                self._remove_waiter_locked(waiter, "timeout")
                self._cv.notify_all()
                raise fail(
                    "E_LOCK_TIMEOUT",
                    "timed out while waiting for the exclusive row write right",
                    txn_id=state.txn_id,
                    lock=self._lock_context(key),
                    holder_txn_id=lock.holder,
                    waited_ms=waited_ms,
                    wait_limit_ms=self.settings.lock_wait_ms,
                    wait_primitive="threading.Condition.wait(monotonic_remaining)",
                )
            self._cv.wait(remaining)

    def _grant_next_locked(self, lock: LockState) -> None:
        while lock.queue:
            waiter = lock.queue[0]
            if waiter.status != "waiting":
                lock.queue.popleft()
                continue
            if time.monotonic() >= waiter.deadline_mono:
                lock.queue.popleft()
                waiter.status = "timeout"
                self._waiting.pop(waiter.txn_id, None)
                self._waits_for.pop(waiter.txn_id, None)
                self._cv.notify_all()
                continue
            state = self._txns.get(waiter.txn_id)
            if state is None or state.status != "active":
                lock.queue.popleft()
                waiter.status = "canceled"
                self._waiting.pop(waiter.txn_id, None)
                self._waits_for.pop(waiter.txn_id, None)
                continue
            lock.queue.popleft()
            lock.holder = state.txn_id
            state.locks.add(waiter.key)
            waiter.status = "acquired"
            self._waiting.pop(waiter.txn_id, None)
            self._waits_for.pop(waiter.txn_id, None)
            intent_id = self.storage.record_intent(
                state.txn_id,
                "lock_acquired",
                {
                    "table": waiter.key[0],
                    "row_id": waiter.key[1],
                    "after_wait": True,
                },
            )
            self._lock_intent_ids[(state.txn_id, waiter.key)] = intent_id
            self._cv.notify_all()
            return
        lock.holder = None
        self._cv.notify_all()

    def _release_lock_locked(self, txn_id: str, key: tuple[str, str], reason: str) -> None:
        lock = self._locks.get(key)
        state = self._txns.get(txn_id)
        if state is not None:
            state.locks.discard(key)
        if lock is not None and lock.holder == txn_id:
            lock.holder = None
            intent_id = self._lock_intent_ids.pop((txn_id, key), None)
            if intent_id is not None:
                self._mark_intent_status(intent_id, "released")
            self._grant_next_locked(lock)
        self._cv.notify_all()

    def _release_all_locks_locked(self, state: TransactionState, reason: str) -> None:
        for key in list(state.locks):
            self._release_lock_locked(state.txn_id, key, reason)
        state.locks.clear()

    def _prune_wait_queue_locked(self, lock: LockState) -> None:
        if not lock.queue:
            return
        active: deque[Waiter] = deque()
        for waiter in lock.queue:
            if waiter.status == "waiting":
                active.append(waiter)
            else:
                self._waiting.pop(waiter.txn_id, None)
                self._waits_for.pop(waiter.txn_id, None)
        lock.queue = active

    def _remove_waiter_locked(self, waiter: Waiter, status: str) -> None:
        waiter.status = status
        lock = self._locks.get(waiter.key)
        if lock is not None:
            lock.queue = deque(item for item in lock.queue if item is not waiter)
        self._waiting.pop(waiter.txn_id, None)
        self._waits_for.pop(waiter.txn_id, None)

    def _find_cycle_locked(self, start: str) -> list[str] | None:
        path: list[str] = []
        seen: set[str] = set()
        current = start
        while current in self._waits_for:
            if current in seen:
                return None
            seen.add(current)
            path.append(current)
            current = self._waits_for[current]
            if current == start:
                return path
        return None

    def _txn_seq_for_locked(self, txn_id: str) -> int:
        state = self._txns.get(txn_id)
        if state is not None:
            return state.txn_seq
        row = self._load_txn_row(txn_id)
        return int(row["txn_seq"]) if row is not None else -1

    def _lock_context(self, key: tuple[str, str]) -> dict[str, Any]:
        lock = self._locks.get(key)
        holder = lock.holder if lock is not None else None
        return {
            "table": key[0],
            "row_id": key[1],
            "locked": holder is not None,
            "holder_txn_id": holder,
            "queued_waiters": len(lock.queue) if lock is not None else 0,
        }

    def _mark_intent_status(self, intent_id: int, status: str) -> None:
        conn = self.storage.connect()
        try:
            conn.execute(
                "UPDATE intents SET status=?, updated_at=? WHERE intent_id=?",
                (status, _now(), intent_id),
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Abort and startup recovery
    # ------------------------------------------------------------------
    def _abort_transaction_locked(
        self,
        state: TransactionState,
        reason: str,
        final_status: str,
        intent_status: str,
    ) -> None:
        txn_id = state.txn_id
        if state.status != "active":
            self._release_all_locks_locked(state, reason)
            return
        state.status = final_status
        state.abort_reason = reason
        waiter = self._waiting.get(txn_id)
        if waiter is not None:
            waiter.status = "victim" if reason == "deadlock_victim" else "canceled"
            lock = self._locks.get(waiter.key)
            if lock is not None:
                lock.queue = deque(item for item in lock.queue if item is not waiter)
            self._waiting.pop(txn_id, None)
            self._waits_for.pop(txn_id, None)

        conn = self.storage.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            created = conn.execute(
                "DELETE FROM row_versions WHERE created_txn=?",
                (txn_id,),
            ).rowcount
            restored = conn.execute(
                "UPDATE row_versions SET deleted_txn=NULL WHERE deleted_txn=?",
                (txn_id,),
            ).rowcount
            sessions = conn.execute(
                "UPDATE edit_sessions SET status=? WHERE txn_id=? AND status IN ('open','saved','deleted')",
                ("rolled_back" if final_status == "rolled_back" else final_status, txn_id),
            ).rowcount
            conn.execute("DELETE FROM txn_write_ops WHERE txn_id=?", (txn_id,))
            conn.execute("DELETE FROM savepoints WHERE txn_id=?", (txn_id,))
            conn.execute(
                "UPDATE transactions SET status=?, abort_reason=? WHERE txn_id=?",
                (final_status, reason, txn_id),
            )
            conn.execute(
                "UPDATE intents SET status=?, updated_at=? WHERE txn_id=? AND status='open'",
                (intent_status if intent_status != "recovered_rolled_back" else "recovered_rolled_back", _now(), txn_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        self._release_all_locks_locked(state, reason)
        self._txns.pop(txn_id, None)
        self._cv.notify_all()

    def _recover_on_start(self) -> dict[str, Any]:
        started_at = _now()
        conn = self.storage.connect()
        try:
            active = conn.execute(
                "SELECT * FROM transactions WHERE status='active' ORDER BY txn_seq"
            ).fetchall()
            active_ids = [row["txn_id"] for row in active]
            pending_by_txn: dict[str, int] = {}
            lock_intents_by_txn: dict[str, int] = {}
            for txn_id in active_ids:
                pending_by_txn[txn_id] = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM intents WHERE txn_id=? AND status='open'",
                        (txn_id,),
                    ).fetchone()[0]
                )
                lock_intents_by_txn[txn_id] = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM intents WHERE txn_id=? AND kind='lock_acquired' AND status='open'",
                        (txn_id,),
                    ).fetchone()[0]
                )
        finally:
            conn.close()

        removed_versions = 0
        cleared_deletions = 0
        released_sessions = 0
        lock_intents_released = sum(lock_intents_by_txn.values())
        if active_ids:
            conn = self.storage.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                placeholders = ",".join("?" for _ in active_ids)
                removed_versions = conn.execute(
                    f"DELETE FROM row_versions WHERE created_txn IN ({placeholders})",
                    active_ids,
                ).rowcount
                cleared_deletions = conn.execute(
                    f"UPDATE row_versions SET deleted_txn=NULL WHERE deleted_txn IN ({placeholders})",
                    active_ids,
                ).rowcount
                released_sessions = conn.execute(
                    f"UPDATE edit_sessions SET status='recovered_aborted' WHERE txn_id IN ({placeholders}) AND status IN ('open','saved','deleted')",
                    active_ids,
                ).rowcount
                conn.execute(
                    f"DELETE FROM txn_write_ops WHERE txn_id IN ({placeholders})",
                    active_ids,
                )
                conn.execute(
                    f"DELETE FROM savepoints WHERE txn_id IN ({placeholders})",
                    active_ids,
                )
                conn.execute(
                    f"UPDATE transactions SET status='recovered_aborted', abort_reason='startup_recovery' WHERE txn_id IN ({placeholders})",
                    active_ids,
                )
                conn.execute(
                    f"UPDATE intents SET status='recovered_rolled_back', updated_at=? WHERE txn_id IN ({placeholders}) AND status='open'",
                    [_now(), *active_ids],
                )
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

        conn = self.storage.connect()
        try:
            orphan = conn.execute(
                "SELECT COUNT(*) FROM intents i LEFT JOIN transactions t ON t.txn_id=i.txn_id "
                "WHERE i.status='open' AND i.txn_id IS NOT NULL AND t.txn_id IS NULL"
            ).fetchone()[0]
            if orphan:
                conn.execute(
                    "UPDATE intents SET status='abandoned', updated_at=? WHERE status='open' AND txn_id IS NOT NULL "
                    "AND txn_id NOT IN (SELECT txn_id FROM transactions)",
                    (_now(),),
                )
            committed_txns = int(
                conn.execute("SELECT COUNT(*) FROM transactions WHERE status='committed'").fetchone()[0]
            )
            committed_versions = int(
                conn.execute("SELECT COUNT(*) FROM row_versions WHERE commit_seq IS NOT NULL").fetchone()[0]
            )
            total_intents = int(conn.execute("SELECT COUNT(*) FROM intents").fetchone()[0])
        finally:
            conn.close()

        finished_at = _now()
        if active_ids:
            state_label = "recovered_unfinished_transactions"
        elif orphan:
            state_label = "orphan_intents_cleaned"
        else:
            state_label = "clean_start"
        report = {
            "report_kind": "startup_recovery",
            "status": state_label,
            "generated_at": finished_at,
            "detection_started_at": started_at,
            "evidence_source": "sqlite:transactions+intents+row_versions+edit_sessions",
            "recovered_txns": [
                {
                    "txn_id": row["txn_id"],
                    "txn_seq": int(row["txn_seq"]),
                    "isolation": row["isolation"],
                    "started_at": float(row["started_at"]),
                    "snapshot_seq": int(row["snapshot_seq"]) if row["snapshot_seq"] is not None else None,
                    "pending_intents": pending_by_txn.get(row["txn_id"], 0),
                    "open_lock_intents": lock_intents_by_txn.get(row["txn_id"], 0),
                }
                for row in active
            ],
            "uncommitted_versions_removed": removed_versions,
            "delete_markers_cleared": cleared_deletions,
            "edit_sessions_released": released_sessions,
            "lock_resources_released": lock_intents_released,
            "orphan_intents_abandoned": int(orphan),
            "committed_transactions_preserved": committed_txns,
            "committed_row_versions_preserved": committed_versions,
            "intent_log_file": str(self.settings.intent_log_path),
            "total_intents_scanned": total_intents,
            "graceful_exit_report_available": False,
            "notes": [
                "This report is reconstructed only from durable SQLite facts after startup.",
                "A forced process termination cannot produce a complete graceful-exit report.",
            ],
        }
        self.storage.write_json_atomic(self.settings.recovery_report_path, report)
        conn = self.storage.connect()
        try:
            conn.execute(
                "INSERT INTO recovery_reports(created_at,report_json) VALUES(?,?)",
                (finished_at, _json(report)),
            )
        finally:
            conn.close()
        self.storage.append_intent_log(
            {
                "kind": "recovery_report",
                "status": report["status"],
                "payload": {
                    "recovered_txns": [item["txn_id"] for item in report["recovered_txns"]],
                    "uncommitted_versions_removed": removed_versions,
                },
                "created_at": finished_at,
            }
        )
        return report

    def _recovery_summary(self) -> dict[str, Any]:
        return {
            "status": self._recovery_report.get("status"),
            "recovered_txn_count": len(self._recovery_report.get("recovered_txns", [])),
            "uncommitted_versions_removed": self._recovery_report.get("uncommitted_versions_removed", 0),
            "report_kind": self._recovery_report.get("report_kind"),
            "graceful_exit_report_available": False,
        }

    # ------------------------------------------------------------------
    # Validation and transaction metadata helpers
    # ------------------------------------------------------------------
    def _require_active(self, txn_id: str) -> TransactionState:
        state = self._txns.get(txn_id)
        if state is not None:
            if state.status != "active":
                raise fail(
                    "E_TXN_NOT_ACTIVE",
                    "transaction is not active",
                    txn_id=txn_id,
                    status=state.status,
                    abort_reason=state.abort_reason,
                )
            return state
        row = self._load_txn_row(txn_id)
        if row is None:
            raise fail("E_TXN_NOT_FOUND", "transaction does not exist", txn_id=txn_id)
        raise fail(
            "E_TXN_NOT_ACTIVE",
            "transaction is not active in this process",
            txn_id=txn_id,
            status=row["status"],
            abort_reason=row["abort_reason"],
        )

    def _load_txn_row(self, txn_id: str) -> sqlite3.Row | None:
        conn = self.storage.connect()
        try:
            return conn.execute(
                "SELECT * FROM transactions WHERE txn_id=?",
                (txn_id,),
            ).fetchone()
        finally:
            conn.close()

    def _txn_public(self, row: sqlite3.Row, replayed: bool) -> dict[str, Any]:
        snapshot_id = None
        if row["snapshot_seq"] is not None:
            snapshot_id = f"snapshot:{int(row['snapshot_seq'])}"
        result = {
            "ok": True,
            "txn_id": row["txn_id"],
            "txn_seq": int(row["txn_seq"]),
            "isolation": row["isolation"],
            "status": row["status"],
            "snapshot_id": snapshot_id,
            "snapshot_seq": int(row["snapshot_seq"]) if row["snapshot_seq"] is not None else None,
            "started_at": float(row["started_at"]),
            "committed_at": float(row["committed_at"]) if row["committed_at"] is not None else None,
            "commit_seq": int(row["commit_seq"]) if row["commit_seq"] is not None else None,
            "abort_reason": row["abort_reason"],
            "replayed": replayed,
        }
        if row["commit_result_json"]:
            result["commit_result"] = parse_json(row["commit_result_json"], {})
        return result

    def _validate_identifier(self, value: Any, field_name: str) -> str:
        text = str(value or "")
        if not IDENTIFIER_RE.match(text):
            raise fail(
                "E_INVALID_REQUEST",
                f"invalid {field_name}",
                field=field_name,
                value=value,
                pattern=IDENTIFIER_RE.pattern,
            )
        return text

    def _validate_optional_identifier(self, value: Any, field_name: str) -> str | None:
        if value is None or value == "":
            return None
        return self._validate_identifier(value, field_name)

    def _validate_location(self, table: Any, row_id: Any) -> tuple[str, str]:
        return self._validate_identifier(table, "table"), self._validate_identifier(row_id, "row_id")

    def _validate_row_version(self, value: Any) -> int:
        try:
            version = int(value)
        except (TypeError, ValueError):
            raise fail("E_INVALID_REQUEST", "row_version must be a positive integer", row_version=value)
        if version < 1:
            raise fail("E_INVALID_REQUEST", "row_version must be positive", row_version=value)
        return version

    def _validate_row_data(self, value: Any) -> None:
        if not isinstance(value, dict):
            raise fail(
                "E_INVALID_REQUEST",
                "row data must be a JSON object",
                received_type=type(value).__name__,
            )

