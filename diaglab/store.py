"""SQLite persistence, snapshot reads, idempotent batches and crash recovery."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Sequence

from .errors import (
    BusinessError,
    bad_request,
    batch_aborted,
    conflicting_batch,
    insufficient_samples,
    not_found,
)
from .models import DiagnosticRecord, NegotiationReport, ProbeRequest
from .probe import probe_tcp
from .service_metrics import fetch_service_metrics
from .stats import (
    MIN_RTT_SAMPLES_DEFAULT,
    compare_statistics,
    compute_path_stats,
    root_cause_candidates,
    stats_snapshot,
)


SCHEMA_VERSION = 1


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    batch_id TEXT,
    fingerprint TEXT NOT NULL,
    state TEXT NOT NULL,
    planned_records INTEGER NOT NULL DEFAULT 0,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    commit_id INTEGER,
    result_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS commits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    batch_id TEXT,
    created_at_ms INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    probe_id TEXT NOT NULL,
    path_node TEXT NOT NULL,
    protocol TEXT NOT NULL,
    success INTEGER NOT NULL,
    rtt_us INTEGER NOT NULL,
    payload_bytes INTEGER NOT NULL,
    failure_type TEXT NOT NULL,
    source_format TEXT NOT NULL,
    source_version INTEGER NOT NULL,
    rounding_bit INTEGER NOT NULL,
    rounding_remainder_us INTEGER NOT NULL,
    rounding_error_us INTEGER NOT NULL,
    UNIQUE(commit_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_records_commit ON records(commit_id);
CREATE INDEX IF NOT EXISTS idx_records_path_time ON records(path_node, timestamp_ms);

CREATE TABLE IF NOT EXISTS negotiations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    commit_id INTEGER,
    format TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    decision TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_negotiations_batch ON negotiations(batch_id);

CREATE TABLE IF NOT EXISTS stats_snapshots (
    commit_id INTEGER PRIMARY KEY,
    version INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS service_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_id INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    path_node TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    mode TEXT,
    accepted_connections INTEGER NOT NULL,
    completed_probes INTEGER NOT NULL,
    hung_connections INTEGER NOT NULL,
    bytes_in INTEGER NOT NULL,
    bytes_out INTEGER NOT NULL,
    errors INTEGER NOT NULL,
    throughput_bytes_per_sec REAL NOT NULL,
    raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_service_metrics_path ON service_metrics(path_node, id);

CREATE TABLE IF NOT EXISTS recovery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL,
    batch_id TEXT,
    action TEXT NOT NULL,
    planned_records INTEGER NOT NULL,
    committed_records INTEGER NOT NULL,
    difference_records INTEGER NOT NULL,
    detected_at_ms INTEGER NOT NULL,
    details_json TEXT NOT NULL
);
"""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def make_batch_fingerprint(kind: str, payload: Any) -> str:
    canonical = canonical_json({"kind": kind, "payload": payload}).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _validate_batch_id(batch_id: str) -> None:
    if not batch_id or len(batch_id) > 128:
        raise bad_request("batch_id 必须非空且不超过 128 字符")


class WorkbenchStore:
    def __init__(self, db_path: str | os.PathLike[str]) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._writer_lock = threading.RLock()
        self._initialized = False
        self.startup_recovery_count = 0

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def initialize(self) -> None:
        with self._writer_lock:
            conn = self._connect()
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(SCHEMA_SQL)
                created = now_ms()
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('current_version', '0')"
                )
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('created_at_ms', ?)",
                    (str(created),),
                )
            finally:
                conn.close()
            self.startup_recovery_count = self._recover_pending_intents()
            self._initialized = True

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("WorkbenchStore.initialize() must be called first")

    # ------------------------------------------------------------------
    # Intent and recovery journal
    # ------------------------------------------------------------------
    def _begin_intent(
        self,
        *,
        operation: str,
        batch_id: str,
        fingerprint: str,
        planned_records: int,
        details: dict[str, Any],
    ) -> str:
        intent_id = uuid.uuid4().hex
        timestamp = now_ms()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO intents(
                    intent_id, operation, batch_id, fingerprint, state,
                    planned_records, details_json, created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    operation,
                    batch_id,
                    fingerprint,
                    planned_records,
                    canonical_json(details),
                    timestamp,
                    timestamp,
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return intent_id

    def _finish_intent(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        state: str,
        *,
        note: str | None = None,
    ) -> None:
        row = conn.execute("SELECT details_json FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
        details: dict[str, Any]
        if row is None:
            details = {}
        else:
            try:
                details = json.loads(row["details_json"])
            except json.JSONDecodeError:
                details = {}
        if note:
            details["state_note"] = note
        conn.execute(
            "UPDATE intents SET state=?, updated_at_ms=?, details_json=? WHERE intent_id=?",
            (state, now_ms(), canonical_json(details), intent_id),
        )

    def _mark_intent_failed(self, intent_id: str, reason: str) -> None:
        try:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._finish_intent(conn, intent_id, "failed", note=reason)
                conn.execute("COMMIT")
            finally:
                conn.close()
        except Exception:
            pass

    def _recover_pending_intents(self) -> int:
        conn = self._connect()
        recovered = 0
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT * FROM intents WHERE state IN ('pending', 'running') ORDER BY created_at_ms, intent_id"
            ).fetchall()
            detected = now_ms()
            for row in rows:
                batch_id = row["batch_id"]
                planned = int(row["planned_records"])
                batch = (
                    conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
                    if batch_id
                    else None
                )
                if batch is not None and batch["status"] == "complete":
                    self._finish_intent(
                        conn,
                        row["intent_id"],
                        "complete",
                        note="启动扫描发现数据批次已完整提交，修正意图状态",
                    )
                    continue
                committed = 0
                if batch_id:
                    count_row = conn.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM records r JOIN commits c ON c.id = r.commit_id
                        WHERE c.batch_id = ?
                        """,
                        (batch_id,),
                    ).fetchone()
                    committed = int(count_row["count"]) if count_row else 0
                    if committed == 0:
                        version_row = conn.execute(
                            "SELECT COUNT(*) AS count FROM commits WHERE batch_id=?", (batch_id,)
                        ).fetchone()
                        if version_row and int(version_row["count"]) > 0:
                            committed = planned
                difference = committed - planned
                self._finish_intent(
                    conn,
                    row["intent_id"],
                    "aborted",
                    note="进程在提交前终止；SQLite 原子事务已回滚未提交诊断数据",
                )
                if batch_id:
                    abort_result = {
                        "ok": False,
                        "code": "E_BATCH_ABORTED",
                        "batch_id": batch_id,
                        "message": "进程中断后由启动恢复标记为已中止",
                        "intent_id": row["intent_id"],
                        "planned_records": planned,
                        "committed_records": committed,
                        "difference_records": difference,
                        "recovered_at_ms": detected,
                    }
                    if batch is None:
                        conn.execute(
                            """
                            INSERT INTO batches(
                                batch_id, kind, fingerprint, status, commit_id,
                                result_json, created_at_ms, updated_at_ms
                            ) VALUES(?, ?, ?, 'aborted', NULL, ?, ?, ?)
                            """,
                            (
                                batch_id,
                                row["operation"],
                                row["fingerprint"],
                                canonical_json(abort_result),
                                row["created_at_ms"],
                                detected,
                            ),
                        )
                    elif batch["status"] != "complete":
                        conn.execute(
                            "UPDATE batches SET status='aborted', result_json=?, updated_at_ms=? WHERE batch_id=?",
                            (canonical_json(abort_result), detected, batch_id),
                        )
                conn.execute(
                    """
                    INSERT INTO recovery_events(
                        intent_id, batch_id, action, planned_records, committed_records,
                        difference_records, detected_at_ms, details_json
                    ) VALUES(?, ?, 'aborted', ?, ?, ?, ?, ?)
                    """,
                    (
                        row["intent_id"],
                        batch_id,
                        planned,
                        committed,
                        difference,
                        detected,
                        canonical_json(
                            {
                                "facts_source": "sqlite",
                                "reason": "unfinished intent at startup",
                                "operation": row["operation"],
                                "note": "已落盘意图为事实；未提交事务不在统计与导出中可见。",
                            }
                        ),
                    ),
                )
                recovered += 1
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('last_recovery_at_ms', ?)",
                (str(detected),),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return recovered

    # ------------------------------------------------------------------
    # Batch/result idempotency helpers
    # ------------------------------------------------------------------
    def _check_batch(self, batch_id: str, kind: str, fingerprint: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            row = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            conn.execute("COMMIT")
        finally:
            conn.close()
        if row is None:
            return None
        if row["fingerprint"] != fingerprint or row["kind"] != kind:
            raise conflicting_batch(
                batch_id,
                existing_kind=row["kind"],
                requested_kind=kind,
                existing_fingerprint=row["fingerprint"],
                requested_fingerprint=fingerprint,
            )
        if row["status"] == "complete" and row["result_json"]:
            result = json.loads(row["result_json"])
            result["reused"] = True
            return result
        if row["status"] == "aborted":
            raise batch_aborted(batch_id, result=json.loads(row["result_json"] or "{}"))
        raise batch_aborted(batch_id, status=row["status"], note="批次未处于完整提交状态")

    def _insert_record_rows(
        self,
        conn: sqlite3.Connection,
        commit_id: int,
        records: Sequence[DiagnosticRecord],
    ) -> None:
        conn.executemany(
            """
            INSERT INTO records(
                commit_id, ordinal, timestamp_ms, probe_id, path_node, protocol,
                success, rtt_us, payload_bytes, failure_type, source_format,
                source_version, rounding_bit, rounding_remainder_us, rounding_error_us
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    commit_id,
                    index,
                    record.timestamp_ms,
                    record.probe_id,
                    record.path_node,
                    record.protocol,
                    1 if record.success else 0,
                    record.rtt_us,
                    record.payload_bytes,
                    record.failure_type,
                    record.source_format,
                    record.source_version,
                    record.rounding_bit,
                    record.rounding_remainder_us,
                    record.rounding_error_us,
                )
                for index, record in enumerate(records)
            ],
        )

    def _insert_service_metric(
        self,
        conn: sqlite3.Connection,
        commit_id: int,
        *,
        path_node: str,
        endpoint: str,
        metrics: dict[str, Any],
        timestamp_ms: int,
    ) -> None:
        def int_metric(key: str) -> int:
            value = metrics.get(key, 0)
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        try:
            throughput = float(metrics.get("throughput_bytes_per_sec", 0.0))
        except (TypeError, ValueError):
            throughput = 0.0
        conn.execute(
            """
            INSERT INTO service_metrics(
                commit_id, timestamp_ms, path_node, endpoint, mode,
                accepted_connections, completed_probes, hung_connections,
                bytes_in, bytes_out, errors, throughput_bytes_per_sec, raw_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                commit_id,
                timestamp_ms,
                path_node,
                endpoint,
                str(metrics.get("mode", "unknown")),
                int_metric("accepted_connections"),
                int_metric("completed_probes"),
                int_metric("hung_connections"),
                int_metric("bytes_in"),
                int_metric("bytes_out"),
                int_metric("errors"),
                throughput,
                canonical_json(metrics),
            ),
        )

    def _commit_payload(
        self,
        *,
        intent_id: str,
        kind: str,
        batch_id: str,
        fingerprint: str,
        records: Sequence[DiagnosticRecord],
        negotiation: NegotiationReport | None,
        normalization_report: dict[str, Any] | None,
        extra_result: dict[str, Any],
        service_metric: dict[str, Any] | None = None,
        commit_details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            version_row = conn.execute(
                "SELECT value FROM meta WHERE key='current_version'"
            ).fetchone()
            version = int(version_row["value"]) + 1
            timestamp = now_ms()
            details = commit_details or {}
            cursor = conn.execute(
                """
                INSERT INTO commits(version, kind, batch_id, created_at_ms, record_count, details_json)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (version, kind, batch_id, timestamp, len(records), canonical_json(details)),
            )
            commit_id = int(cursor.lastrowid)
            self._insert_record_rows(conn, commit_id, records)
            if negotiation is not None:
                conn.execute(
                    """
                    INSERT INTO negotiations(
                        batch_id, commit_id, format, format_version, decision, details_json, created_at_ms
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        commit_id,
                        negotiation.format,
                        negotiation.version,
                        negotiation.decision,
                        canonical_json(
                            {
                                **negotiation.as_dict(),
                                "normalization": normalization_report,
                            }
                        ),
                        timestamp,
                    ),
                )
            if service_metric is not None:
                self._insert_service_metric(
                    conn,
                    commit_id,
                    path_node=service_metric["path_node"],
                    endpoint=service_metric["endpoint"],
                    metrics=service_metric["metrics"],
                    timestamp_ms=int(service_metric["timestamp_ms"]),
                )
            all_records = self._all_records(conn)
            snapshot = stats_snapshot(all_records, min_samples=MIN_RTT_SAMPLES_DEFAULT)
            conn.execute(
                """
                INSERT INTO stats_snapshots(commit_id, version, created_at_ms, payload_json)
                VALUES(?, ?, ?, ?)
                """,
                (commit_id, version, timestamp, canonical_json(snapshot)),
            )
            conn.execute(
                "UPDATE meta SET value=? WHERE key='current_version'", (str(version),)
            )
            result: dict[str, Any] = {
                "ok": True,
                "batch_id": batch_id,
                "kind": kind,
                "intent_id": intent_id,
                "reused": False,
                "http_status": 200,
                "commit": {
                    "id": commit_id,
                    "version": version,
                    "record_count": len(records),
                    "created_at_ms": timestamp,
                },
                "records_persisted": len(records),
                "stats_digest": snapshot["calculation_digest"],
            }
            if negotiation is not None:
                result["negotiation"] = negotiation.as_dict()
            if normalization_report is not None:
                result["normalization"] = normalization_report
            result.update(extra_result)
            result["http_status"] = int(result.get("http_status", 200))
            if result["http_status"] >= 400:
                result["ok"] = False
            conn.execute(
                """
                INSERT INTO batches(
                    batch_id, kind, fingerprint, status, commit_id, result_json,
                    created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, 'complete', ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    kind,
                    fingerprint,
                    commit_id,
                    canonical_json(result),
                    timestamp,
                    timestamp,
                ),
            )
            self._finish_intent(conn, intent_id, "complete")
            conn.execute("COMMIT")
            return result
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _submit(
        self,
        *,
        kind: str,
        batch_id: str,
        fingerprint: str,
        planned_records: int,
        operation_details: dict[str, Any],
        builder: Callable[[], tuple[Sequence[DiagnosticRecord], NegotiationReport | None, dict[str, Any] | None, dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]],
    ) -> dict[str, Any]:
        self._require_initialized()
        _validate_batch_id(batch_id)
        with self._writer_lock:
            cached = self._check_batch(batch_id, kind, fingerprint)
            if cached is not None:
                return cached
            intent_id = self._begin_intent(
                operation=kind,
                batch_id=batch_id,
                fingerprint=fingerprint,
                planned_records=planned_records,
                details=operation_details,
            )
            try:
                (
                    records,
                    negotiation,
                    normalization_report,
                    extra_result,
                    service_metric,
                    commit_details,
                ) = builder()
                return self._commit_payload(
                    intent_id=intent_id,
                    kind=kind,
                    batch_id=batch_id,
                    fingerprint=fingerprint,
                    records=records,
                    negotiation=negotiation,
                    normalization_report=normalization_report,
                    extra_result=extra_result,
                    service_metric=service_metric,
                    commit_details=commit_details,
                )
            except Exception as exc:
                self._mark_intent_failed(intent_id, f"{type(exc).__name__}: {exc}")
                raise

    def import_batch(
        self,
        *,
        batch_id: str,
        fingerprint: str,
        records: Sequence[DiagnosticRecord],
        negotiation: NegotiationReport,
        normalization_report: dict[str, Any],
        source: dict[str, Any],
    ) -> dict[str, Any]:
        def builder() -> tuple[Any, Any, Any, Any, Any, Any]:
            return (
                records,
                negotiation,
                normalization_report,
                {
                    "ok": True,
                    "code": None,
                    "message": "诊断包已完整提交",
                    "http_status": 200,
                },
                None,
                {"source": source},
            )

        return self._submit(
            kind="import",
            batch_id=batch_id,
            fingerprint=fingerprint,
            planned_records=len(records),
            operation_details={
                "source": source,
                "format": negotiation.format,
                "version": negotiation.version,
                "decision": negotiation.decision,
            },
            builder=builder,
        )

    def probe_batch(
        self,
        *,
        batch_id: str,
        fingerprint: str,
        request: ProbeRequest,
    ) -> dict[str, Any]:
        def builder() -> tuple[Any, Any, Any, Any, Any, Any]:
            observation = probe_tcp(request)
            extra = {
                "ok": observation.code is None,
                "code": observation.code,
                "message": observation.message,
                "http_status": observation.http_status,
                "probe": observation.as_dict(),
            }
            return (
                [observation.record],
                None,
                None,
                extra,
                None,
                {
                    "probe_request": {
                        "host": request.host,
                        "port": request.port,
                        "path_node": request.path_node,
                        "probe_id": request.probe_id,
                        "timeout_ms": request.timeout_ms,
                        "payload_bytes": request.payload_bytes,
                        "protocol": request.protocol,
                    }
                },
            )

        return self._submit(
            kind="probe",
            batch_id=batch_id,
            fingerprint=fingerprint,
            planned_records=1,
            operation_details={
                "host": request.host,
                "port": request.port,
                "path_node": request.path_node,
                "probe_id": request.probe_id,
                "timeout_ms": request.timeout_ms,
                "payload_bytes": request.payload_bytes,
                "protocol": request.protocol,
            },
            builder=builder,
        )

    def service_metrics_batch(
        self,
        *,
        batch_id: str,
        fingerprint: str,
        path_node: str,
        admin_url: str,
    ) -> dict[str, Any]:
        def builder() -> tuple[Any, Any, Any, Any, Any, Any]:
            collected = fetch_service_metrics(admin_url)
            metrics = collected["metrics"]
            extra = {
                "ok": True,
                "code": None,
                "http_status": 200,
                "service_metrics": {
                    "path_node": path_node,
                    "source_url": collected["source_url"],
                    "collected_at_ms": collected["collected_at_ms"],
                    "metrics": metrics,
                },
            }
            metric = {
                "path_node": path_node,
                "endpoint": collected["source_url"],
                "metrics": metrics,
                "timestamp_ms": collected["collected_at_ms"],
            }
            return (
                [],
                None,
                None,
                extra,
                metric,
                {"path_node": path_node, "admin_url": collected["source_url"]},
            )

        return self._submit(
            kind="service-metrics",
            batch_id=batch_id,
            fingerprint=fingerprint,
            planned_records=0,
            operation_details={"path_node": path_node, "admin_url": admin_url},
            builder=builder,
        )

    def recompute_stats(self, *, batch_id: str, fingerprint: str) -> dict[str, Any]:
        def builder() -> tuple[Any, Any, Any, Any, Any, Any]:
            return ([], None, None, {"ok": True, "http_status": 200}, None, {"operation": "recompute_stats"})

        return self._submit(
            kind="stats-recompute",
            batch_id=batch_id,
            fingerprint=fingerprint,
            planned_records=0,
            operation_details={"operation": "recompute_stats"},
            builder=builder,
        )

    # ------------------------------------------------------------------
    # Snapshot read helpers
    # ------------------------------------------------------------------
    def _current_version(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
        return int(row["value"]) if row else 0

    def _row_to_record(self, row: sqlite3.Row) -> DiagnosticRecord:
        return DiagnosticRecord(
            timestamp_ms=int(row["timestamp_ms"]),
            probe_id=row["probe_id"],
            path_node=row["path_node"],
            protocol=row["protocol"],
            success=bool(row["success"]),
            rtt_us=int(row["rtt_us"]),
            payload_bytes=int(row["payload_bytes"]),
            failure_type=row["failure_type"],
            ordinal=int(row["ordinal"]),
            source_format=row["source_format"],
            source_version=int(row["source_version"]),
            rounding_bit=int(row["rounding_bit"]),
            rounding_remainder_us=int(row["rounding_remainder_us"]),
            rounding_error_us=int(row["rounding_error_us"]),
        )

    def _query_records(
        self,
        conn: sqlite3.Connection,
        *,
        path_node: str | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[DiagnosticRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if path_node is not None:
            clauses.append("path_node=?")
            params.append(path_node)
        if start_ms is not None:
            clauses.append("timestamp_ms>=?")
            params.append(start_ms)
        if end_ms is not None:
            clauses.append("timestamp_ms<?")
            params.append(end_ms)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT * FROM records"
            + where
            + " ORDER BY timestamp_ms, commit_id, ordinal"
        )
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [self._row_to_record(row) for row in conn.execute(sql, params).fetchall()]

    def _all_records(self, conn: sqlite3.Connection) -> list[DiagnosticRecord]:
        return self._query_records(conn)

    def _snapshot_read(self, callback: Callable[[sqlite3.Connection, int], Any]) -> Any:
        self._require_initialized()
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            version = self._current_version(conn)
            result = callback(conn, version)
            conn.execute("COMMIT")
            return result
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def stats(
        self,
        path_node: str,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        min_samples: int = MIN_RTT_SAMPLES_DEFAULT,
    ) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            records = self._query_records(
                conn, path_node=path_node, start_ms=start_ms, end_ms=end_ms
            )
            result = compute_path_stats(
                path_node,
                records,
                start_ms=start_ms,
                end_ms=end_ms,
                min_samples=min_samples,
            )
            result["snapshot_version"] = version
            return result

        return self._snapshot_read(read)

    def root_causes(
        self,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        min_samples: int = MIN_RTT_SAMPLES_DEFAULT,
        path_node: str | None = None,
    ) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            records = self._query_records(
                conn, path_node=path_node, start_ms=start_ms, end_ms=end_ms
            )
            if path_node is not None:
                matching = [record for record in records if record.path_node == path_node]
                compute_path_stats(
                    path_node,
                    matching,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    min_samples=min_samples,
                )
            result = root_cause_candidates(
                records, start_ms=start_ms, end_ms=end_ms, min_samples=min_samples
            )
            result["snapshot_version"] = version
            return result

        return self._snapshot_read(read)

    def compare_paths(
        self,
        path_a: str,
        path_b: str,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        min_samples: int = MIN_RTT_SAMPLES_DEFAULT,
        relative_tolerance: float = 1e-9,
    ) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            records_a = self._query_records(
                conn, path_node=path_a, start_ms=start_ms, end_ms=end_ms
            )
            records_b = self._query_records(
                conn, path_node=path_b, start_ms=start_ms, end_ms=end_ms
            )
            stats_a = compute_path_stats(
                path_a,
                records_a,
                start_ms=start_ms,
                end_ms=end_ms,
                min_samples=min_samples,
            )
            stats_b = compute_path_stats(
                path_b,
                records_b,
                start_ms=start_ms,
                end_ms=end_ms,
                min_samples=min_samples,
            )
            result = compare_statistics(
                stats_a, stats_b, relative_tolerance=relative_tolerance
            )
            result["snapshot_version"] = version
            return result

        return self._snapshot_read(read)

    def records(
        self,
        *,
        path_node: str | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        if limit < 1 or limit > 10_000:
            raise bad_request("limit 必须在 1..10000")

        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            records = self._query_records(
                conn,
                path_node=path_node,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
            )
            return {
                "ok": True,
                "snapshot_version": version,
                "count": len(records),
                "records": [record.audit_dict() for record in records],
            }

        return self._snapshot_read(read)

    def export_records(
        self,
        *,
        path_node: str | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> tuple[int, list[DiagnosticRecord]]:
        def read(conn: sqlite3.Connection, version: int) -> tuple[int, list[DiagnosticRecord]]:
            records = self._query_records(
                conn, path_node=path_node, start_ms=start_ms, end_ms=end_ms
            )
            if not records:
                raise not_found("导出过滤条件没有匹配记录")
            return version, records

        return self._snapshot_read(read)

    def paths(self) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            rows = conn.execute(
                """
                SELECT path_node, COUNT(*) AS total,
                       SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) AS failed,
                       MIN(timestamp_ms) AS first_ms, MAX(timestamp_ms) AS last_ms
                FROM records GROUP BY path_node ORDER BY path_node
                """
            ).fetchall()
            return {
                "ok": True,
                "snapshot_version": version,
                "paths": [dict(row) for row in rows],
            }

        return self._snapshot_read(read)

    def latest_service_metrics(self, limit: int = 50) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            rows = conn.execute(
                "SELECT * FROM service_metrics ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            metrics = []
            for row in rows:
                item = dict(row)
                item["raw"] = json.loads(item.pop("raw_json"))
                metrics.append(item)
            return {"ok": True, "snapshot_version": version, "metrics": metrics}

        return self._snapshot_read(read)

    def overview(self) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            record_count = int(
                conn.execute("SELECT COUNT(*) AS count FROM records").fetchone()["count"]
            )
            path_count = int(
                conn.execute("SELECT COUNT(DISTINCT path_node) AS count FROM records").fetchone()[
                    "count"
                ]
            )
            latest_commit = conn.execute(
                "SELECT * FROM commits ORDER BY version DESC LIMIT 1"
            ).fetchone()
            path_rows = conn.execute(
                """
                SELECT path_node, COUNT(*) AS total,
                       SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) AS failed
                FROM records GROUP BY path_node ORDER BY path_node
                """
            ).fetchall()
            metric_rows = conn.execute(
                "SELECT * FROM service_metrics ORDER BY id DESC LIMIT 20"
            ).fetchall()
            latest_by_path: dict[str, dict[str, Any]] = {}
            for row in metric_rows:
                latest_by_path.setdefault(row["path_node"], dict(row))
            for item in latest_by_path.values():
                item["raw"] = json.loads(item.pop("raw_json"))
            batch_rows = conn.execute(
                "SELECT batch_id, kind, status, commit_id, updated_at_ms FROM batches ORDER BY updated_at_ms DESC LIMIT 8"
            ).fetchall()
            return {
                "ok": True,
                "snapshot_version": version,
                "record_count": record_count,
                "path_count": path_count,
                "latest_commit": dict(latest_commit) if latest_commit else None,
                "paths": [dict(row) for row in path_rows],
                "latest_service_metrics": list(latest_by_path.values()),
                "recent_batches": [dict(row) for row in batch_rows],
                "recovery_pending_intents": int(
                    conn.execute(
                        "SELECT COUNT(*) AS count FROM intents WHERE state IN ('pending','running')"
                    ).fetchone()["count"]
                ),
            }

        return self._snapshot_read(read)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            batch = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if batch is None:
                raise not_found("批次不存在", batch_id=batch_id)
            item = dict(batch)
            item["result"] = json.loads(item.pop("result_json") or "{}")
            negotiations = conn.execute(
                "SELECT * FROM negotiations WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
            item["negotiations"] = []
            for row in negotiations:
                n = dict(row)
                n["details"] = json.loads(n.pop("details_json"))
                item["negotiations"].append(n)
            intents = conn.execute(
                "SELECT intent_id, operation, state, planned_records, created_at_ms, updated_at_ms, details_json "
                "FROM intents WHERE batch_id=? ORDER BY created_at_ms",
                (batch_id,),
            ).fetchall()
            item["intents"] = []
            for row in intents:
                i = dict(row)
                i["details"] = json.loads(i.pop("details_json"))
                item["intents"].append(i)
            item["snapshot_version"] = version
            return {"ok": True, **item}

        return self._snapshot_read(read)

    def list_negotiations(self, limit: int = 100) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            rows = conn.execute(
                "SELECT * FROM negotiations ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            items = []
            for row in rows:
                item = dict(row)
                item["details"] = json.loads(item.pop("details_json"))
                items.append(item)
            return {"ok": True, "snapshot_version": version, "negotiations": items}

        return self._snapshot_read(read)

    def stats_snapshot(self) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            row = conn.execute(
                "SELECT * FROM stats_snapshots WHERE version=? ORDER BY commit_id DESC LIMIT 1",
                (version,),
            ).fetchone()
            if row is None:
                return {
                    "ok": True,
                    "snapshot_version": version,
                    "snapshot": None,
                    "note": "当前版本尚无已物化统计快照",
                }
            return {
                "ok": True,
                "snapshot_version": version,
                "commit_id": row["commit_id"],
                "created_at_ms": row["created_at_ms"],
                "snapshot": json.loads(row["payload_json"]),
            }

        return self._snapshot_read(read)

    def recovery_report(self) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            event_rows = conn.execute(
                "SELECT * FROM recovery_events ORDER BY id DESC LIMIT 100"
            ).fetchall()
            events = []
            for row in event_rows:
                item = dict(row)
                item["details"] = json.loads(item.pop("details_json"))
                events.append(item)
            batch_counts = {
                row["status"]: int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM batches GROUP BY status"
                ).fetchall()
            }
            intent_counts = {
                row["state"]: int(row["count"])
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS count FROM intents GROUP BY state"
                ).fetchall()
            }
            record_count = int(
                conn.execute("SELECT COUNT(*) AS count FROM records").fetchone()["count"]
            )
            latest = conn.execute(
                "SELECT * FROM commits ORDER BY version DESC LIMIT 1"
            ).fetchone()
            recovery_meta = conn.execute(
                "SELECT value FROM meta WHERE key='last_recovery_at_ms'"
            ).fetchone()
            differences = [
                {
                    "intent_id": event["intent_id"],
                    "batch_id": event["batch_id"],
                    "planned_records": event["planned_records"],
                    "committed_records": event["committed_records"],
                    "difference_records": event["difference_records"],
                    "action": event["action"],
                    "detected_at_ms": event["detected_at_ms"],
                }
                for event in events
            ]
            return {
                "ok": True,
                "database": str(self.db_path),
                "generated_at_ms": now_ms(),
                "facts_only": True,
                "source": "SQLite persisted rows; uncommitted WAL frames are not treated as facts",
                "latest_complete_version": version,
                "latest_commit": dict(latest) if latest else None,
                "record_count": record_count,
                "pending_intents_now": int(
                    conn.execute(
                        "SELECT COUNT(*) AS count FROM intents WHERE state IN ('pending','running')"
                    ).fetchone()["count"]
                ),
                "last_recovery_at_ms": int(recovery_meta["value"]) if recovery_meta else None,
                "batch_status_counts": batch_counts,
                "intent_state_counts": intent_counts,
                "recovery_events": events,
                "difference_report": differences,
                "report_contract": "只从已提交的 intents/batches/commits/records/recovery_events 生成；未提交事务不可见。",
            }

        return self._snapshot_read(read)

    def health(self) -> dict[str, Any]:
        def read(conn: sqlite3.Connection, version: int) -> dict[str, Any]:
            conn.execute("SELECT 1").fetchone()
            return {
                "ok": True,
                "snapshot_version": version,
                "sqlite": sqlite3.sqlite_version,
                "database": str(self.db_path),
            }

        return self._snapshot_read(read)

    # ------------------------------------------------------------------
    # Crash simulation used by the end-to-end demonstration
    # ------------------------------------------------------------------
    def crash_after_uncommitted_import(
        self,
        *,
        batch_id: str,
        records: Sequence[DiagnosticRecord],
        marker_path: str | os.PathLike[str],
        fingerprint: str | None = None,
    ) -> None:
        """Persist an intent, then hold an uncommitted write transaction forever.

        The caller is expected to force-kill this process after the marker appears.
        SQLite rolls back the uncommitted WAL transaction on the next connection.
        """

        self._require_initialized()
        fingerprint = fingerprint or make_batch_fingerprint(
            "crash-import", {"batch_id": batch_id, "records": len(records)}
        )
        intent_id = self._begin_intent(
            operation="import",
            batch_id=batch_id,
            fingerprint=fingerprint,
            planned_records=len(records),
            details={"simulated": True, "reason": "force-kill demonstration"},
        )
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        version_row = conn.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
        version = int(version_row["value"]) + 1
        timestamp = now_ms()
        cursor = conn.execute(
            """
            INSERT INTO commits(version, kind, batch_id, created_at_ms, record_count, details_json)
            VALUES(?, 'import', ?, ?, ?, ?)
            """,
            (version, batch_id, timestamp, len(records), canonical_json({"simulated_crash": True})),
        )
        commit_id = int(cursor.lastrowid)
        self._insert_record_rows(conn, commit_id, records)
        conn.execute("UPDATE meta SET value=? WHERE key='current_version'", (str(version),))
        marker = Path(marker_path)
        marker.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ready": True,
            "pid": os.getpid(),
            "intent_id": intent_id,
            "batch_id": batch_id,
            "uncommitted_version": version,
            "planned_records": len(records),
            "marker": str(marker),
        }
        with marker.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        while True:
            time.sleep(60)
