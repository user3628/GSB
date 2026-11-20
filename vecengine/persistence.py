from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    payload_hash TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    plan_summary_json TEXT NOT NULL,
    execution_json TEXT NOT NULL,
    dataset TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    path TEXT,
    batch_size INTEGER,
    selection_json TEXT,
    total_rows INTEGER NOT NULL,
    output_rows INTEGER,
    output_columns INTEGER,
    completed_batches INTEGER NOT NULL DEFAULT 0,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    result_path TEXT,
    result_sha256 TEXT,
    reuse_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS batch_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    batch_index INTEGER NOT NULL,
    row_start INTEGER NOT NULL,
    row_end INTEGER NOT NULL,
    rows_in INTEGER NOT NULL,
    rows_out INTEGER NOT NULL,
    path TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    elapsed_ms REAL NOT NULL,
    memory_estimate_bytes INTEGER NOT NULL,
    expression_nodes INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(session_id, batch_index),
    FOREIGN KEY(session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS intent_log (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    request_id TEXT,
    kind TEXT NOT NULL,
    batch_index INTEGER,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE,
    request_id TEXT,
    created_at TEXT NOT NULL,
    disposition TEXT NOT NULL,
    status_before TEXT,
    status_after TEXT NOT NULL,
    last_completed_batch INTEGER,
    last_persisted_row INTEGER NOT NULL,
    completed_batch_count INTEGER NOT NULL,
    pending_intent_count INTEGER NOT NULL,
    result_available INTEGER NOT NULL,
    facts_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_intent_status ON intent_log(status);
CREATE INDEX IF NOT EXISTS idx_intent_session ON intent_log(session_id);
CREATE INDEX IF NOT EXISTS idx_batch_session ON batch_stats(session_id, batch_index);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Store:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(SCHEMA)

    def create_intent(
        self,
        *,
        session_id: str,
        request_id: str | None,
        kind: str,
        batch_index: int | None = None,
        detail: dict[str, Any] | None = None,
    ) -> int:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO intent_log(
                    session_id, request_id, kind, batch_index, status, detail_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    session_id,
                    request_id,
                    kind,
                    batch_index,
                    json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def complete_intent(self, intent_id: int, status: str = "completed") -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE intent_log SET status=?, updated_at=? WHERE intent_id=?",
                (status, utc_now(), intent_id),
            )

    def create_session(
        self,
        *,
        session_id: str,
        request_id: str,
        payload_hash: str,
        plan: Any,
        plan_summary: dict[str, Any],
        execution: dict[str, Any],
        dataset: str,
        total_rows: int,
        submit_intent_id: int,
    ) -> None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT INTO sessions(
                        session_id, request_id, payload_hash, plan_json, plan_summary_json,
                        execution_json, dataset, status, created_at, total_rows
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
                    """,
                    (
                        session_id,
                        request_id,
                        payload_hash,
                        json.dumps(plan, ensure_ascii=False, sort_keys=True),
                        json.dumps(plan_summary, ensure_ascii=False, sort_keys=True),
                        json.dumps(execution, ensure_ascii=False, sort_keys=True),
                        dataset,
                        now,
                        total_rows,
                    ),
                )
                connection.execute(
                    "UPDATE intent_log SET status='completed', updated_at=? WHERE intent_id=?",
                    (now, submit_intent_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def find_by_request_id(self, request_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM sessions WHERE request_id=?", (request_id,)
            ).fetchone()
        return dict(row) if row else None

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
        return dict(row) if row else None

    def mark_running(self, session_id: str, intent_id: int, selection: dict[str, Any]) -> None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    UPDATE sessions SET status='running', started_at=?, path=?, batch_size=?, selection_json=?
                    WHERE session_id=? AND status='queued'
                    """,
                    (
                        now,
                        selection["path"],
                        selection["batch_size"],
                        json.dumps(selection, ensure_ascii=False, sort_keys=True),
                        session_id,
                    ),
                )
                connection.execute(
                    "UPDATE intent_log SET status='completed', updated_at=? WHERE intent_id=?",
                    (now, intent_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def record_batch_and_checkpoint(
        self,
        *,
        session_id: str,
        stat: dict[str, Any],
        processed_rows: int,
        intent_id: int,
    ) -> None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                session = connection.execute(
                    "SELECT status, completed_batches FROM sessions WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if session is None or session["status"] != "running":
                    raise RuntimeError("session is not running while recording a batch")
                expected_index = int(session["completed_batches"])
                if int(stat["batch_index"]) != expected_index:
                    raise RuntimeError(
                        f"batch index {stat['batch_index']} does not follow checkpoint {expected_index}"
                    )
                connection.execute(
                    """
                    INSERT INTO batch_stats(
                        session_id, batch_index, row_start, row_end, rows_in, rows_out,
                        path, batch_size, elapsed_ms, memory_estimate_bytes,
                        expression_nodes, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        stat["batch_index"],
                        stat["row_start"],
                        stat["row_end"],
                        stat["rows_in"],
                        stat["rows_out"],
                        stat["path"],
                        stat["batch_size"],
                        stat["elapsed_ms"],
                        stat["memory_estimate_bytes"],
                        stat["expression_nodes"],
                        now,
                    ),
                )
                connection.execute(
                    """
                    UPDATE sessions
                    SET completed_batches=completed_batches+1, processed_rows=?
                    WHERE session_id=?
                    """,
                    (processed_rows, session_id),
                )
                connection.execute(
                    "UPDATE intent_log SET status='completed', updated_at=? WHERE intent_id=?",
                    (now, intent_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def publish_result(
        self,
        *,
        session_id: str,
        intent_id: int,
        result_path: str,
        result_sha256: str,
        output_rows: int,
        output_columns: int,
    ) -> None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                session = connection.execute(
                    "SELECT status FROM sessions WHERE session_id=?", (session_id,)
                ).fetchone()
                if session is None or session["status"] != "running":
                    raise RuntimeError("session is not running while publishing its result")
                connection.execute(
                    """
                    UPDATE sessions
                    SET status='ready', finished_at=?, result_path=?, result_sha256=?,
                        output_rows=?, output_columns=?
                    WHERE session_id=?
                    """,
                    (now, result_path, result_sha256, output_rows, output_columns, session_id),
                )
                connection.execute(
                    "UPDATE intent_log SET status='completed', updated_at=? WHERE intent_id=?",
                    (now, intent_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def mark_failed(
        self,
        *,
        session_id: str,
        error_code: str,
        error_message: str,
        intent_id: int | None = None,
    ) -> None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    UPDATE sessions
                    SET status='failed', finished_at=?, error_code=?, error_message=?
                    WHERE session_id=? AND status IN ('queued', 'running')
                    """,
                    (now, error_code, error_message, session_id),
                )
                if intent_id is not None:
                    connection.execute(
                        "UPDATE intent_log SET status='failed', updated_at=? WHERE intent_id=?",
                        (now, intent_id),
                    )
                connection.execute(
                    "UPDATE intent_log SET status='failed', updated_at=? WHERE session_id=? AND status='pending'",
                    (now, session_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def increment_reuse_count(self, session_id: str, intent_id: int) -> None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "UPDATE sessions SET reuse_count=reuse_count+1 WHERE session_id=?",
                    (session_id,),
                )
                connection.execute(
                    "UPDATE intent_log SET status='completed', updated_at=? WHERE intent_id=?",
                    (now, intent_id),
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def list_batch_stats(self, session_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM batch_stats WHERE session_id=? ORDER BY batch_index",
                (session_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_intents(self, session_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM intent_log WHERE session_id=? ORDER BY intent_id",
                (session_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def ready_result_paths(self) -> set[str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT result_path FROM sessions WHERE status='ready' AND result_path IS NOT NULL"
            ).fetchall()
        return {str(row["result_path"]) for row in rows}

    def recover_incomplete_sessions(self) -> list[dict[str, Any]]:
        """Abort sessions that survived a process boundary as incomplete.

        The report deliberately uses only committed batch statistics/checkpoints.
        No partial in-memory output is assumed to exist.
        """

        with self.connect() as connection:
            has_work = connection.execute(
                """
                SELECT 1 FROM sessions WHERE status IN ('queued', 'running')
                UNION ALL
                SELECT 1 FROM intent_log WHERE status='pending'
                LIMIT 1
                """
            ).fetchone()
        if has_work is None:
            return []
        scan_intent_id = self.create_intent(
            session_id="__recovery_scan__",
            request_id=None,
            kind="recovery_scan",
            detail={"write_order": "intent_before_recovery_transaction"},
        )

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "UPDATE intent_log SET status='recovered', updated_at=? "
                    "WHERE kind='recovery_scan' AND status='pending'",
                    (utc_now(),),
                )
                connection.execute(
                    """
                    UPDATE intent_log SET status='recovered', updated_at=?
                    WHERE status='pending' AND kind!='recovery_scan'
                      AND session_id IN (
                          SELECT session_id FROM sessions
                          WHERE status IN ('ready', 'failed', 'aborted')
                      )
                    """,
                    (utc_now(),),
                )
                rows = connection.execute(
                    "SELECT * FROM sessions WHERE status IN ('queued', 'running') ORDER BY created_at"
                ).fetchall()
                reports: list[dict[str, Any]] = []
                now = utc_now()
                for row in rows:
                    report = self._recover_session_row(connection, row, now)
                    reports.append(report)

                orphan_intents = connection.execute(
                    """
                    SELECT * FROM intent_log
                    WHERE status='pending' AND kind!='recovery_scan'
                      AND session_id NOT IN (SELECT session_id FROM sessions)
                    ORDER BY intent_id
                    """
                ).fetchall()
                for intent in orphan_intents:
                    report_id = str(uuid.uuid4())
                    facts = {
                        "kind": "orphan_pending_intent",
                        "intent_id": intent["intent_id"],
                        "intent_kind": intent["kind"],
                        "request_id": intent["request_id"],
                        "detail": json.loads(intent["detail_json"]),
                        "facts_only": True,
                        "result_available": False,
                    }
                    report = {
                        "report_id": report_id,
                        "session_id": f"orphan-intent-{intent['intent_id']}",
                        "request_id": intent["request_id"],
                        "created_at": now,
                        "disposition": "orphan_intent_recovered",
                        "status_before": "intent_without_session",
                        "status_after": "aborted",
                        "last_completed_batch": None,
                        "last_persisted_row": 0,
                        "completed_batch_count": 0,
                        "pending_intent_count": 1,
                        "result_available": False,
                        "facts": facts,
                    }
                    connection.execute(
                        """
                        INSERT OR REPLACE INTO recovery_reports(
                            report_id, session_id, request_id, created_at, disposition,
                            status_before, status_after, last_completed_batch,
                            last_persisted_row, completed_batch_count,
                            pending_intent_count, result_available, facts_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            report_id,
                            report["session_id"],
                            report["request_id"],
                            now,
                            report["disposition"],
                            report["status_before"],
                            report["status_after"],
                            None,
                            0,
                            0,
                            1,
                            0,
                            json.dumps(facts, ensure_ascii=False, sort_keys=True),
                        ),
                    )
                    connection.execute(
                        "UPDATE intent_log SET status='recovered', updated_at=? WHERE intent_id=?",
                        (now, intent["intent_id"]),
                    )
                    reports.append(report)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        self.complete_intent(scan_intent_id)
        return reports

    def _recover_session_row(
        self, connection: sqlite3.Connection, row: sqlite3.Row, now: str
    ) -> dict[str, Any]:
        session_id = str(row["session_id"])
        batch = connection.execute(
            "SELECT * FROM batch_stats WHERE session_id=? ORDER BY batch_index DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        pending = connection.execute(
            "SELECT * FROM intent_log WHERE session_id=? AND status='pending' ORDER BY intent_id",
            (session_id,),
        ).fetchall()
        pending_facts = [
            {
                "intent_id": intent["intent_id"],
                "kind": intent["kind"],
                "batch_index": intent["batch_index"],
                "detail": json.loads(intent["detail_json"]),
            }
            for intent in pending
        ]
        last_batch_index = int(batch["batch_index"]) if batch is not None else None
        last_persisted_row = int(batch["row_end"]) if batch is not None else int(row["processed_rows"])
        completed_count = int(row["completed_batches"])
        report_id = str(uuid.uuid4())
        facts = {
            "kind": "incomplete_session",
            "session_status_before": row["status"],
            "completed_batch_count": completed_count,
            "last_completed_batch": last_batch_index,
            "last_persisted_row": last_persisted_row,
            "pending_intents": pending_facts,
            "result_available": False,
            "facts_only": True,
            "guarantee": "no complete result is promised after forced termination",
        }
        connection.execute(
            """
            UPDATE sessions
            SET status='aborted', finished_at=?, error_code='E_SESSION_ABORTED',
                error_message='process terminated before a complete result was committed'
            WHERE session_id=?
            """,
            (now, session_id),
        )
        connection.execute(
            "UPDATE intent_log SET status='recovered', updated_at=? WHERE session_id=? AND status='pending'",
            (now, session_id),
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO recovery_reports(
                report_id, session_id, request_id, created_at, disposition,
                status_before, status_after, last_completed_batch, last_persisted_row,
                completed_batch_count, pending_intent_count, result_available, facts_json
            ) VALUES (?, ?, ?, ?, 'session_aborted', ?, 'aborted', ?, ?, ?, ?, 0, ?)
            """,
            (
                report_id,
                session_id,
                row["request_id"],
                now,
                row["status"],
                last_batch_index,
                last_persisted_row,
                completed_count,
                len(pending_facts),
                json.dumps(facts, ensure_ascii=False, sort_keys=True),
            ),
        )
        return {
            "report_id": report_id,
            "session_id": session_id,
            "request_id": row["request_id"],
            "created_at": now,
            "disposition": "session_aborted",
            "status_before": row["status"],
            "status_after": "aborted",
            "last_completed_batch": last_batch_index,
            "last_persisted_row": last_persisted_row,
            "completed_batch_count": completed_count,
            "pending_intent_count": len(pending_facts),
            "result_available": False,
            "facts": facts,
        }

    def list_recovery_reports(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY created_at DESC, report_id"
            ).fetchall()
        return [self._decode_recovery_row(row) for row in rows]

    def get_recovery_report(self, session_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM recovery_reports WHERE session_id=?", (session_id,)
            ).fetchone()
        return self._decode_recovery_row(row) if row else None

    @staticmethod
    def _decode_recovery_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["result_available"] = bool(result["result_available"])
        result["facts"] = json.loads(result.pop("facts_json"))
        return result


def measure_elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)
