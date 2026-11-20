from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from app.digest import canonical_json, policy_checksum
from app.errors import (
    E_BATCH_INCOMPLETE,
    E_BATCH_NOT_FOUND,
    E_INVALID_ARGUMENT,
    E_ORDER_NOT_FOUND,
    E_SNAPSHOT_INVALID,
    E_SNAPSHOT_NOT_FOUND,
    E_STALE_SNAPSHOT,
    BusinessError,
)
from app.models import BatchStatus, PolicySnapshot, TraceSample


LEGAL_TRANSITIONS = {
    BatchStatus.PENDING: {BatchStatus.RUNNING, BatchStatus.ABORTED},
    BatchStatus.RUNNING: {BatchStatus.COMPLETED, BatchStatus.ABORTED},
    BatchStatus.COMPLETED: set(),
    BatchStatus.ABORTED: set(),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


class Storage:
    """SQLite persistence with intent-first writes and atomic batch publication."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    version INTEGER PRIMARY KEY,
                    generated_at TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    committed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS write_intents (
                    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT,
                    operation TEXT NOT NULL,
                    state TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    snapshot_version INTEGER NOT NULL REFERENCES snapshots(version),
                    snapshot_checksum TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    trace_checksum TEXT NOT NULL,
                    trace_json TEXT NOT NULL,
                    request_count INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('PENDING','RUNNING','COMPLETED','ABORTED')),
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    stats_json TEXT,
                    conflicts_json TEXT,
                    recovery_report_id TEXT
                );

                CREATE TABLE IF NOT EXISTS decision_staging (
                    batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
                    request_id TEXT NOT NULL,
                    sequence_no INTEGER NOT NULL,
                    route_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    response_status INTEGER NOT NULL,
                    response_latency_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    staged_at TEXT NOT NULL,
                    PRIMARY KEY (batch_id, request_id)
                );

                CREATE TABLE IF NOT EXISTS decisions (
                    batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
                    request_id TEXT NOT NULL,
                    sequence_no INTEGER NOT NULL,
                    route_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    response_status INTEGER NOT NULL,
                    response_latency_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (batch_id, request_id)
                );

                CREATE TABLE IF NOT EXISTS work_orders (
                    order_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
                    route_id TEXT NOT NULL,
                    difference_type TEXT NOT NULL,
                    trigger_condition TEXT NOT NULL,
                    affected_requests INTEGER NOT NULL,
                    total_requests INTEGER NOT NULL,
                    affected_ratio_bp INTEGER NOT NULL,
                    type_weight INTEGER NOT NULL,
                    priority_score INTEGER NOT NULL,
                    priority TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    metrics_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (batch_id, route_id, difference_type)
                );

                CREATE TABLE IF NOT EXISTS recovery_reports (
                    report_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL UNIQUE REFERENCES batches(batch_id),
                    previous_status TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    facts_json TEXT NOT NULL,
                    generated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);
                CREATE INDEX IF NOT EXISTS idx_intents_batch ON write_intents(batch_id);
                CREATE INDEX IF NOT EXISTS idx_work_orders_batch_rank ON work_orders(batch_id, rank);
                """
            )
        finally:
            connection.close()

    def _insert_intent(
        self,
        connection: sqlite3.Connection,
        *,
        batch_id: str | None,
        operation: str,
        detail: dict[str, Any],
        state: str = "PREPARED",
    ) -> int:
        now = utc_now()
        cursor = connection.execute(
            """
            INSERT INTO write_intents(batch_id, operation, state, detail_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (batch_id, operation, state, canonical_json(detail), now, now),
        )
        return int(cursor.lastrowid)

    def _finish_intent(
        self,
        connection: sqlite3.Connection,
        intent_id: int,
        state: str = "COMMITTED",
    ) -> None:
        connection.execute(
            "UPDATE write_intents SET state = ?, updated_at = ? WHERE intent_id = ?",
            (state, utc_now(), intent_id),
        )

    def commit_snapshot(
        self,
        snapshot: PolicySnapshot,
        expected_checksum: str,
    ) -> dict[str, Any]:
        with self._transaction() as connection:
            intent_id = self._insert_intent(
                connection,
                batch_id=None,
                operation="SUBMIT_SNAPSHOT",
                detail={"version": snapshot.version, "expected_checksum": expected_checksum},
            )
            row = connection.execute("SELECT COALESCE(MAX(version), 0) AS version FROM snapshots").fetchone()
            current_version = int(row["version"])
            if snapshot.version <= current_version:
                raise BusinessError(
                    E_STALE_SNAPSHOT,
                    "snapshot version must be greater than the current committed version",
                    status_code=409,
                    details={
                        "submitted_version": snapshot.version,
                        "current_version": current_version,
                    },
                )
            if snapshot.checksum != expected_checksum:
                raise BusinessError(
                    E_SNAPSHOT_INVALID,
                    "snapshot checksum does not match canonical snapshot content",
                    status_code=400,
                    details={
                        "submitted_checksum": snapshot.checksum,
                        "expected_checksum": expected_checksum,
                    },
                )

            committed_at = utc_now()
            payload_json = canonical_json(snapshot.model_dump(mode="json"))
            connection.execute(
                """
                INSERT INTO snapshots(version, generated_at, checksum, payload_json, committed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (snapshot.version, snapshot.generated_at, snapshot.checksum, payload_json, committed_at),
            )
            self._finish_intent(connection, intent_id)
            return {
                "version": snapshot.version,
                "checksum": snapshot.checksum,
                "committed_at": committed_at,
            }

    def current_version(self) -> int | None:
        connection = self._connect()
        try:
            row = connection.execute("SELECT MAX(version) AS version FROM snapshots").fetchone()
            return int(row["version"]) if row["version"] is not None else None
        finally:
            connection.close()

    def get_snapshot(self, version: int) -> PolicySnapshot:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json, checksum FROM snapshots WHERE version = ?",
                (version,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise BusinessError(
                E_SNAPSHOT_NOT_FOUND,
                "policy snapshot version was not found",
                status_code=404,
                details={"version": version},
            )
        snapshot = PolicySnapshot.model_validate_json(row["payload_json"])
        if policy_checksum(snapshot) != row["checksum"]:
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "persisted snapshot checksum verification failed",
                status_code=500,
                details={"version": version},
            )
        return snapshot

    def get_current_snapshot(self) -> PolicySnapshot:
        version = self.current_version()
        if version is None:
            raise BusinessError(
                E_SNAPSHOT_NOT_FOUND,
                "no policy snapshot has been committed",
                status_code=404,
            )
        return self.get_snapshot(version)

    def create_batch(
        self,
        *,
        batch_id: str,
        snapshot: PolicySnapshot,
        trace: TraceSample,
        trace_sha256: str,
    ) -> dict[str, Any]:
        with self._transaction() as connection:
            intent_id = self._insert_intent(
                connection,
                batch_id=batch_id,
                operation="CREATE_BATCH",
                detail={
                    "snapshot_version": snapshot.version,
                    "trace_id": trace.trace_id,
                    "trace_checksum": trace_sha256,
                },
            )
            snapshot_row = connection.execute(
                "SELECT checksum FROM snapshots WHERE version = ?",
                (snapshot.version,),
            ).fetchone()
            if snapshot_row is None:
                raise BusinessError(
                    E_SNAPSHOT_NOT_FOUND,
                    "policy snapshot version was not found",
                    status_code=404,
                    details={"version": snapshot.version},
                )
            existing = connection.execute(
                "SELECT 1 FROM batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if existing is not None:
                raise BusinessError(
                    E_INVALID_ARGUMENT,
                    "batch_id already exists",
                    status_code=409,
                    details={"batch_id": batch_id},
                )
            created_at = utc_now()
            connection.execute(
                """
                INSERT INTO batches(
                    batch_id, snapshot_version, snapshot_checksum, trace_id, trace_checksum,
                    trace_json, request_count, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
                """,
                (
                    batch_id,
                    snapshot.version,
                    snapshot_row["checksum"],
                    trace.trace_id,
                    trace_sha256,
                    canonical_json(trace.model_dump(mode="json")),
                    len(trace.requests),
                    created_at,
                ),
            )
            self._finish_intent(connection, intent_id)
        return self.get_batch_summary(batch_id)

    def start_batch(self, batch_id: str) -> dict[str, Any]:
        with self._transaction() as connection:
            transition_intent = self._insert_intent(
                connection,
                batch_id=batch_id,
                operation="TRANSITION_BATCH",
                detail={"target_status": "RUNNING"},
            )
            row = connection.execute("SELECT status FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
            if row is None:
                raise BusinessError(
                    E_BATCH_NOT_FOUND,
                    "simulation batch was not found",
                    status_code=404,
                    details={"batch_id": batch_id},
                )
            if BatchStatus(row["status"]) != BatchStatus.PENDING:
                raise BusinessError(
                    E_INVALID_ARGUMENT,
                    "illegal batch transition",
                    status_code=409,
                    details={
                        "batch_id": batch_id,
                        "from": row["status"],
                        "to": BatchStatus.RUNNING.value,
                    },
                )
            started_at = utc_now()
            connection.execute(
                "UPDATE batches SET status = 'RUNNING', started_at = ? WHERE batch_id = ?",
                (started_at, batch_id),
            )
            self._finish_intent(connection, transition_intent)
            self._insert_intent(
                connection,
                batch_id=batch_id,
                operation="SIMULATE_BATCH",
                detail={"snapshot_version": self._batch_snapshot_version(connection, batch_id)},
                state="STARTED",
            )
        return self.get_batch_summary(batch_id)

    @staticmethod
    def _batch_snapshot_version(connection: sqlite3.Connection, batch_id: str) -> int:
        row = connection.execute(
            "SELECT snapshot_version FROM batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        if row is None:
            raise BusinessError(
                E_BATCH_NOT_FOUND,
                "simulation batch was not found",
                status_code=404,
                details={"batch_id": batch_id},
            )
        return int(row["snapshot_version"])

    def stage_decisions(self, batch_id: str, decisions: list[dict[str, Any]]) -> int:
        with self._transaction() as connection:
            intent_id = self._insert_intent(
                connection,
                batch_id=batch_id,
                operation="STAGE_DECISIONS",
                detail={"incoming_count": len(decisions)},
            )
            row = connection.execute("SELECT status FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
            if row is None:
                raise BusinessError(
                    E_BATCH_NOT_FOUND,
                    "simulation batch was not found",
                    status_code=404,
                    details={"batch_id": batch_id},
                )
            if row["status"] != BatchStatus.RUNNING.value:
                raise BusinessError(
                    E_INVALID_ARGUMENT,
                    "decisions can only be staged for a RUNNING batch",
                    status_code=409,
                    details={"batch_id": batch_id, "status": row["status"]},
                )
            staged_at = utc_now()
            for decision in decisions:
                connection.execute(
                    """
                    INSERT INTO decision_staging(
                        batch_id, request_id, sequence_no, route_id, decision_type,
                        response_status, response_latency_ms, payload_json, staged_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(batch_id, request_id) DO UPDATE SET
                        sequence_no = excluded.sequence_no,
                        route_id = excluded.route_id,
                        decision_type = excluded.decision_type,
                        response_status = excluded.response_status,
                        response_latency_ms = excluded.response_latency_ms,
                        payload_json = excluded.payload_json,
                        staged_at = excluded.staged_at
                    """,
                    (
                        batch_id,
                        decision["request_id"],
                        decision["sequence_no"],
                        decision["route_id"],
                        decision["decision_type"],
                        decision["response_status"],
                        decision["response_latency_ms"],
                        canonical_json(decision),
                        staged_at,
                    ),
                )
            self._finish_intent(connection, intent_id)
            count_row = connection.execute(
                "SELECT COUNT(*) AS count FROM decision_staging WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            return int(count_row["count"])

    def finalize_batch(
        self,
        batch_id: str,
        *,
        orders: list[dict[str, Any]],
        stats: dict[str, Any],
        conflicts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        with self._transaction() as connection:
            intent_id = self._insert_intent(
                connection,
                batch_id=batch_id,
                operation="FINALIZE_BATCH",
                detail={"order_count": len(orders)},
            )
            batch_row = connection.execute(
                "SELECT status, request_count FROM batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if batch_row is None:
                raise BusinessError(
                    E_BATCH_NOT_FOUND,
                    "simulation batch was not found",
                    status_code=404,
                    details={"batch_id": batch_id},
                )
            if batch_row["status"] != BatchStatus.RUNNING.value:
                raise BusinessError(
                    E_INVALID_ARGUMENT,
                    "only a RUNNING batch can be finalized",
                    status_code=409,
                    details={"batch_id": batch_id, "status": batch_row["status"]},
                )
            staged_rows = connection.execute(
                """
                SELECT request_id, sequence_no, route_id, decision_type, response_status,
                       response_latency_ms, payload_json
                FROM decision_staging WHERE batch_id = ? ORDER BY sequence_no, request_id
                """,
                (batch_id,),
            ).fetchall()
            if len(staged_rows) != int(batch_row["request_count"]):
                raise BusinessError(
                    E_INVALID_ARGUMENT,
                    "cannot finalize: staged decisions do not cover the complete trace",
                    status_code=409,
                    details={
                        "batch_id": batch_id,
                        "staged_count": len(staged_rows),
                        "request_count": int(batch_row["request_count"]),
                    },
                )
            for row in staged_rows:
                connection.execute(
                    """
                    INSERT INTO decisions(
                        batch_id, request_id, sequence_no, route_id, decision_type,
                        response_status, response_latency_ms, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        row["request_id"],
                        row["sequence_no"],
                        row["route_id"],
                        row["decision_type"],
                        row["response_status"],
                        row["response_latency_ms"],
                        row["payload_json"],
                    ),
                )
            created_at = utc_now()
            for order in orders:
                connection.execute(
                    """
                    INSERT INTO work_orders(
                        order_id, batch_id, route_id, difference_type, trigger_condition,
                        affected_requests, total_requests, affected_ratio_bp, type_weight,
                        priority_score, priority, rank, metrics_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order["order_id"],
                        batch_id,
                        order["route_id"],
                        order["difference_type"],
                        order["trigger_condition"],
                        order["affected_requests"],
                        order["total_requests"],
                        order["affected_ratio_bp"],
                        order["type_weight"],
                        order["priority_score"],
                        order["priority"],
                        order["rank"],
                        canonical_json(order["metrics"]),
                        created_at,
                    ),
                )
            completed_at = utc_now()
            connection.execute(
                """
                UPDATE batches SET status = 'COMPLETED', completed_at = ?, stats_json = ?, conflicts_json = ?
                WHERE batch_id = ?
                """,
                (completed_at, canonical_json(stats), canonical_json(conflicts), batch_id),
            )
            connection.execute("DELETE FROM decision_staging WHERE batch_id = ?", (batch_id,))
            connection.execute(
                """
                UPDATE write_intents SET state = 'COMMITTED', updated_at = ?
                WHERE batch_id = ? AND operation = 'SIMULATE_BATCH' AND state = 'STARTED'
                """,
                (utc_now(), batch_id),
            )
            self._finish_intent(connection, intent_id)
        return self.get_batch_summary(batch_id)

    def transition_batch(self, batch_id: str, target: BatchStatus) -> dict[str, Any]:
        if target == BatchStatus.COMPLETED:
            raise BusinessError(
                E_INVALID_ARGUMENT,
                "COMPLETED must use the atomic finalize operation",
                status_code=409,
                details={"batch_id": batch_id},
            )
        with self._transaction() as connection:
            intent_id = self._insert_intent(
                connection,
                batch_id=batch_id,
                operation="TRANSITION_BATCH",
                detail={"target_status": target.value},
            )
            row = connection.execute("SELECT status FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
            if row is None:
                raise BusinessError(
                    E_BATCH_NOT_FOUND,
                    "simulation batch was not found",
                    status_code=404,
                    details={"batch_id": batch_id},
                )
            current = BatchStatus(row["status"])
            if target not in LEGAL_TRANSITIONS[current]:
                raise BusinessError(
                    E_INVALID_ARGUMENT,
                    "illegal batch transition",
                    status_code=409,
                    details={"batch_id": batch_id, "from": current.value, "to": target.value},
                )
            completed_at = utc_now() if target == BatchStatus.ABORTED else None
            connection.execute(
                "UPDATE batches SET status = ?, completed_at = ? WHERE batch_id = ?",
                (target.value, completed_at, batch_id),
            )
            if target == BatchStatus.ABORTED:
                connection.execute(
                    """
                    UPDATE write_intents SET state = 'ABORTED', updated_at = ?
                    WHERE batch_id = ? AND state IN ('PREPARED','STARTED')
                    """,
                    (utc_now(), batch_id),
                )
            self._finish_intent(connection, intent_id)
        return self.get_batch_summary(batch_id)

    def _get_batch_row(self, batch_id: str) -> dict[str, Any]:
        connection = self._connect()
        try:
            row = connection.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
        finally:
            connection.close()
        if row is None:
            raise BusinessError(
                E_BATCH_NOT_FOUND,
                "simulation batch was not found",
                status_code=404,
                details={"batch_id": batch_id},
            )
        return dict(row)

    def get_batch_summary(self, batch_id: str) -> dict[str, Any]:
        row = self._get_batch_row(batch_id)
        completed = row["status"] == BatchStatus.COMPLETED.value
        summary = {
            "batch_id": row["batch_id"],
            "snapshot_version": row["snapshot_version"],
            "snapshot_checksum": row["snapshot_checksum"],
            "trace_id": row["trace_id"],
            "trace_checksum": row["trace_checksum"],
            "request_count": row["request_count"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "recovery_report_id": row["recovery_report_id"],
            "stats": json.loads(row["stats_json"]) if completed and row["stats_json"] else None,
            "conflicts": json.loads(row["conflicts_json"]) if completed and row["conflicts_json"] else None,
            "conclusion_visible": completed,
        }
        return summary

    def get_batch_context(self, batch_id: str) -> dict[str, Any]:
        row = self._get_batch_row(batch_id)
        return {
            **row,
            "trace": TraceSample.model_validate_json(row["trace_json"]),
        }

    def _require_completed(self, batch_id: str) -> dict[str, Any]:
        row = self._get_batch_row(batch_id)
        if row["status"] != BatchStatus.COMPLETED.value:
            raise BusinessError(
                E_BATCH_INCOMPLETE,
                "no complete simulation conclusion is visible for this batch",
                status_code=409,
                details={"batch_id": batch_id, "status": row["status"]},
            )
        return row

    def get_decisions(self, batch_id: str) -> list[dict[str, Any]]:
        self._require_completed(batch_id)
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT payload_json FROM decisions
                WHERE batch_id = ? ORDER BY sequence_no, request_id
                """,
                (batch_id,),
            ).fetchall()
        finally:
            connection.close()
        return [json.loads(row["payload_json"]) for row in rows]

    def get_orders(self, batch_id: str) -> list[dict[str, Any]]:
        self._require_completed(batch_id)
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM work_orders WHERE batch_id = ? ORDER BY rank, order_id",
                (batch_id,),
            ).fetchall()
        finally:
            connection.close()
        return [self._order_row(row) for row in rows]

    @staticmethod
    def _order_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "order_id": row["order_id"],
            "batch_id": row["batch_id"],
            "route_id": row["route_id"],
            "difference_type": row["difference_type"],
            "trigger_condition": row["trigger_condition"],
            "affected_requests": row["affected_requests"],
            "total_requests": row["total_requests"],
            "affected_ratio_bp": row["affected_ratio_bp"],
            "type_weight": row["type_weight"],
            "priority_score": row["priority_score"],
            "priority": row["priority"],
            "rank": row["rank"],
            "metrics": json.loads(row["metrics_json"]),
            "created_at": row["created_at"],
        }

    def get_order(self, order_id: str) -> dict[str, Any]:
        connection = self._connect()
        try:
            row = connection.execute("SELECT * FROM work_orders WHERE order_id = ?", (order_id,)).fetchone()
        finally:
            connection.close()
        if row is None:
            raise BusinessError(
                E_ORDER_NOT_FOUND,
                "difference work order was not found",
                status_code=404,
                details={"order_id": order_id},
            )
        return self._order_row(row)

    def recover_incomplete_batches(self) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT batch_id FROM batches WHERE status IN ('PENDING','RUNNING') ORDER BY created_at, batch_id"
            ).fetchall()
        finally:
            connection.close()
        reports = []
        for row in rows:
            report = self._recover_one(row["batch_id"])
            if report is not None:
                reports.append(report)
        return reports

    def _recover_one(self, batch_id: str) -> dict[str, Any] | None:
        with self._transaction() as connection:
            intent_id = self._insert_intent(
                connection,
                batch_id=batch_id,
                operation="RECOVER_BATCH",
                detail={"reason": "startup_or_explicit_scan"},
            )
            batch = connection.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
            if batch is None or batch["status"] not in ("PENDING", "RUNNING"):
                self._finish_intent(connection, intent_id, "SKIPPED")
                return None
            report_id = "report_" + hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:20]
            final_count_row = connection.execute(
                "SELECT COUNT(*) AS count FROM decisions WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            staged_count_row = connection.execute(
                "SELECT COUNT(*) AS count FROM decision_staging WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            order_count_row = connection.execute(
                "SELECT COUNT(*) AS count FROM work_orders WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            last_staged_row = connection.execute(
                """
                SELECT request_id, sequence_no, decision_type FROM decision_staging
                WHERE batch_id = ? ORDER BY sequence_no DESC LIMIT 1
                """,
                (batch_id,),
            ).fetchone()
            open_intents = connection.execute(
                """
                SELECT intent_id, operation, state, created_at FROM write_intents
                WHERE batch_id = ? AND state IN ('PREPARED','STARTED')
                  AND operation <> 'RECOVER_BATCH'
                ORDER BY intent_id
                """,
                (batch_id,),
            ).fetchall()
            facts = {
                "batch_id": batch_id,
                "previous_status": batch["status"],
                "snapshot_version": batch["snapshot_version"],
                "snapshot_checksum": batch["snapshot_checksum"],
                "trace_id": batch["trace_id"],
                "trace_checksum": batch["trace_checksum"],
                "request_count": batch["request_count"],
                "persisted_final_decisions": int(final_count_row["count"]),
                "persisted_staged_decisions": int(staged_count_row["count"]),
                "last_staged_decision": _row_to_dict(last_staged_row),
                "persisted_orders": int(order_count_row["count"]),
                "open_intents": [dict(item) for item in open_intents],
                "complete_conclusion_available": False,
                "visibility_rule": "incomplete batches expose no decision conclusions",
                "note": (
                    "The process was interrupted before atomic finalization. Recovery only reports "
                    "facts already committed before the interruption."
                ),
            }
            generated_at = utc_now()
            connection.execute(
                """
                INSERT INTO recovery_reports(
                    report_id, batch_id, previous_status, outcome, facts_json, generated_at
                ) VALUES (?, ?, ?, 'ABORTED', ?, ?)
                """,
                (report_id, batch_id, batch["status"], canonical_json(facts), generated_at),
            )
            connection.execute(
                """
                UPDATE batches SET status = 'ABORTED', completed_at = ?, recovery_report_id = ?
                WHERE batch_id = ?
                """,
                (generated_at, report_id, batch_id),
            )
            connection.execute(
                """
                UPDATE write_intents SET state = 'ABORTED', updated_at = ?
                WHERE batch_id = ? AND state IN ('PREPARED','STARTED')
                """,
                (utc_now(), batch_id),
            )
            self._finish_intent(connection, intent_id)
            return self._get_recovery_report_with_connection(connection, report_id)

    @staticmethod
    def _get_recovery_report_with_connection(
        connection: sqlite3.Connection,
        report_id: str,
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT * FROM recovery_reports WHERE report_id = ?",
            (report_id,),
        ).fetchone()
        return Storage._recovery_row(row)

    @staticmethod
    def _recovery_row(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            raise BusinessError(
                E_BATCH_NOT_FOUND,
                "recovery report was not found",
                status_code=404,
            )
        return {
            "report_id": row["report_id"],
            "batch_id": row["batch_id"],
            "previous_status": row["previous_status"],
            "outcome": row["outcome"],
            "facts": json.loads(row["facts_json"]),
            "generated_at": row["generated_at"],
        }

    def list_recovery_reports(self) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY generated_at, report_id"
            ).fetchall()
        finally:
            connection.close()
        return [self._recovery_row(row) for row in rows]

    def get_recovery_report_for_batch(self, batch_id: str) -> dict[str, Any]:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM recovery_reports WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise BusinessError(
                E_BATCH_NOT_FOUND,
                "recovery report was not found for this batch",
                status_code=404,
                details={"batch_id": batch_id},
            )
        return self._recovery_row(row)
