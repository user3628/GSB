"""SQLite-backed plan versions, idempotency records, and crash recovery."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .business import EngineError, NotFoundError, RequestConflictError
from .expressions import canonical_json


SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    intent_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL UNIQUE,
    request_id TEXT NOT NULL,
    sql TEXT NOT NULL,
    statement_key TEXT NOT NULL,
    status TEXT NOT NULL,
    injection_json TEXT NOT NULL DEFAULT '{}',
    business_code TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_intents_request ON intents(request_id);
CREATE TABLE IF NOT EXISTS versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    statement_key TEXT NOT NULL,
    sql TEXT NOT NULL,
    normalized_plan TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_versions_request ON versions(request_id);
CREATE INDEX IF NOT EXISTS idx_versions_statement ON versions(statement_key);
CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    sql TEXT NOT NULL,
    statement_key TEXT NOT NULL,
    version_id INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS statement_runs (
    statement_key TEXT PRIMARY KEY,
    version_id INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS recovery_reports (
    recovery_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class PlanStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.writer_lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(SCHEMA)
        finally:
            connection.close()

    def find_request(self, request_id: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT request_id, sql, statement_key, version_id, result_json, status, created_at "
                "FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            connection.close()

    def find_statement(self, statement_key: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT statement_key, version_id, result_json, created_at "
                "FROM statement_runs WHERE statement_key = ?",
                (statement_key,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            connection.close()

    def ensure_request_compatible(self, request_id: str, sql: str) -> dict[str, Any] | None:
        existing = self.find_request(request_id)
        if existing is None:
            return None
        if existing["sql"] != sql:
            raise RequestConflictError(request_id)
        return existing

    def begin_intent(
        self,
        request_id: str,
        sql: str,
        statement_key: str,
        injection: dict[str, Any] | None = None,
    ) -> str:
        with self.writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(intent_seq), 0) + 1 AS next_seq FROM intents"
                ).fetchone()["next_seq"]
                digest = hashlib.sha256(f"{request_id}\0{sql}".encode("utf-8")).hexdigest()[:8]
                intent_id = f"intent_{sequence:06d}_{digest}"
                connection.execute(
                    "INSERT INTO intents(intent_id, request_id, sql, statement_key, status, injection_json) "
                    "VALUES (?, ?, ?, ?, 'pending', ?)",
                    (intent_id, request_id, sql, statement_key, canonical_json(injection or {})),
                )
                connection.execute("COMMIT")
                return intent_id
            except Exception:
                connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def abort_intent(
        self, intent_id: str, business_code: str, message: str, context: dict[str, Any]
    ) -> None:
        detail = {"business_code": business_code, "message": message, "context": context}
        with self.writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE intents SET status='aborted', business_code=?, detail_json=?, "
                    "updated_at=CURRENT_TIMESTAMP WHERE intent_id=? AND status='pending'",
                    (business_code, canonical_json(detail), intent_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def commit_result(
        self,
        *,
        intent_id: str,
        request_id: str,
        sql: str,
        statement_key: str,
        normalized_plan: str,
        plan_json: str,
        result: dict[str, Any],
    ) -> tuple[int, dict[str, Any]]:
        with self.writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                request_row = connection.execute(
                    "SELECT sql, result_json FROM requests WHERE request_id = ?", (request_id,)
                ).fetchone()
                if request_row is not None:
                    if request_row["sql"] != sql:
                        raise RequestConflictError(request_id)
                    connection.execute("ROLLBACK")
                    stored = json.loads(request_row["result_json"])
                    return int(stored["version_id"]), stored
                cursor = connection.execute(
                    "INSERT INTO versions(request_id, statement_key, sql, normalized_plan, plan_json, result_json) "
                    "VALUES (?, ?, ?, ?, ?, '{}')",
                    (request_id, statement_key, sql, normalized_plan, plan_json),
                )
                version_id = int(cursor.lastrowid)
                stored_result = {**result, "version_id": version_id, "statement_key": statement_key}
                result_json = canonical_json(stored_result)
                connection.execute(
                    "UPDATE versions SET result_json=? WHERE version_id=?",
                    (result_json, version_id),
                )
                connection.execute(
                    "INSERT INTO statement_runs(statement_key, version_id, result_json) VALUES (?, ?, ?)",
                    (statement_key, version_id, result_json),
                )
                connection.execute(
                    "INSERT INTO requests(request_id, sql, statement_key, version_id, result_json, status) "
                    "VALUES (?, ?, ?, ?, ?, 'committed')",
                    (request_id, sql, statement_key, version_id, result_json),
                )
                connection.execute(
                    "UPDATE intents SET status='committed', business_code='OK', "
                    "detail_json=?, updated_at=CURRENT_TIMESTAMP WHERE intent_id=? AND status='pending'",
                    (
                        canonical_json(
                            {
                                "version_id": version_id,
                                "statement_key": statement_key,
                                "outcome": "plan_version_committed",
                            }
                        ),
                        intent_id,
                    ),
                )
                connection.execute("COMMIT")
                return version_id, stored_result
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def register_reused_request(
        self,
        *,
        intent_id: str,
        request_id: str,
        sql: str,
        statement_key: str,
        version_id: int,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        with self.writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                stored_result = {**result, "version_id": version_id, "statement_key": statement_key}
                result_json = canonical_json(stored_result)
                connection.execute(
                    "INSERT INTO requests(request_id, sql, statement_key, version_id, result_json, status) "
                    "VALUES (?, ?, ?, ?, ?, 'committed')",
                    (request_id, sql, statement_key, version_id, result_json),
                )
                connection.execute(
                    "UPDATE intents SET status='committed', business_code='OK', detail_json=?, "
                    "updated_at=CURRENT_TIMESTAMP WHERE intent_id=? AND status='pending'",
                    (
                        canonical_json(
                            {
                                "version_id": version_id,
                                "statement_key": statement_key,
                                "outcome": "reused_statement_version",
                            }
                        ),
                        intent_id,
                    ),
                )
                connection.execute("COMMIT")
                return stored_result
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            connection.close()

    def intents_for_request(self, request_id: str) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM intents WHERE request_id = ? ORDER BY intent_seq", (request_id,)
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    def recover_pending_intents(self) -> dict[str, Any]:
        with self.writer_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                pending = connection.execute(
                    "SELECT * FROM intents WHERE status='pending' ORDER BY intent_seq"
                ).fetchall()
                facts: list[dict[str, Any]] = []
                for row in pending:
                    version_row = connection.execute(
                        "SELECT version_id FROM versions WHERE request_id = ? "
                        "ORDER BY version_id DESC LIMIT 1",
                        (row["request_id"],),
                    ).fetchone()
                    if version_row is None:
                        action = "aborted_without_committed_version"
                        last_version = None
                    else:
                        action = "aborted_and_retained_last_complete_version"
                        last_version = int(version_row["version_id"])
                    fact = {
                        "intent_id": row["intent_id"],
                        "request_id": row["request_id"],
                        "sql_sha256": hashlib.sha256(row["sql"].encode("utf-8")).hexdigest(),
                        "before_status": "pending",
                        "after_status": "aborted",
                        "action": action,
                        "last_complete_version": last_version,
                        "business_code": "E_RECOVERED_AFTER_CRASH",
                    }
                    connection.execute(
                        "UPDATE intents SET status='aborted', business_code='E_RECOVERED_AFTER_CRASH', "
                        "detail_json=?, updated_at=CURRENT_TIMESTAMP WHERE intent_id=?",
                        (canonical_json(fact), row["intent_id"]),
                    )
                    facts.append(fact)
                pending_after = connection.execute(
                    "SELECT COUNT(*) AS count FROM intents WHERE status='pending'"
                ).fetchone()["count"]
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(recovery_seq), 0) + 1 AS next_seq FROM recovery_reports"
                ).fetchone()["next_seq"]
                report = {
                    "report_type": "startup_recovery",
                    "recovery_seq": sequence,
                    "scanned_pending_intents": len(pending),
                    "recovered_count": len(facts),
                    "pending_after_recovery": int(pending_after),
                    "facts": facts,
                    "diff": {
                        "pending_before": len(pending),
                        "pending_after": int(pending_after),
                        "versions_deleted": 0,
                        "versions_retained": sum(1 for fact in facts if fact["last_complete_version"] is not None),
                    },
                    "complete_exit_report_available": False,
                    "complete_exit_report_reason": (
                        "A forced termination can leave only persisted intent and version facts; "
                        "no complete in-process exit report is implied."
                    ),
                    "source": "persisted intents, requests, versions, and recovery_reports tables",
                }
                connection.execute(
                    "INSERT INTO recovery_reports(report_json) VALUES (?)", (canonical_json(report),)
                )
                connection.execute("COMMIT")
                return report
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def latest_recovery_report(self) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT recovery_seq, report_json, created_at FROM recovery_reports "
                "ORDER BY recovery_seq DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            report = json.loads(row["report_json"])
            report["persisted_at"] = row["created_at"]
            return report
        finally:
            connection.close()

    def status(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            def count(table: str) -> int:
                return int(connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"])

            pending = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM intents WHERE status='pending'"
                ).fetchone()["count"]
            )
            return {
                "store_path": str(self.db_path),
                "requests": count("requests"),
                "versions": count("versions"),
                "statement_runs": count("statement_runs"),
                "intents": count("intents"),
                "pending_intents": pending,
                "recovery_reports": count("recovery_reports"),
            }
        finally:
            connection.close()

    def require_request_result(self, request_id: str) -> dict[str, Any]:
        row = self.find_request(request_id)
        if row is None:
            raise NotFoundError("request", request_id)
        result = json.loads(row["result_json"])
        result["request_id"] = request_id
        result["status"] = row["status"]
        result["reuse_kind"] = "request_id"
        result["reused"] = True
        return result

    def require_intent(self, intent_id: str) -> dict[str, Any]:
        row = self.get_intent(intent_id)
        if row is None:
            raise NotFoundError("intent", intent_id)
        return row

    def safe_exit_summary(self) -> dict[str, Any]:
        status = self.status()
        return {
            "summary_type": "graceful_exit",
            "complete": True,
            "status": status,
            "note": "This summary is emitted only on a controlled, safe shutdown.",
        }


def statement_key_for(sql: str) -> str:
    normalized = sql.strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

