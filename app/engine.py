from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .db import (
    bump_commit_seq,
    initialize_database,
    read_transaction,
    write_transaction,
)
from .errors import (
    BusinessError,
    E_CONCURRENCY_LIMIT,
    E_CONFLICTING_CALL,
    E_EXEC_FAILED,
    E_EXEC_TIMEOUT,
    E_INTERNAL,
    E_INVALID_TRANSITION,
    E_NOT_FOUND,
    E_QUEUE_TIMEOUT,
    E_QUOTA_DENIED,
    E_RECOVERED_INTERRUPTED,
    E_SPEC_INVALID,
    E_START_FAILED,
    E_START_TIMEOUT,
    E_TRACE_OUT_OF_ORDER,
)
from .state_machine import TRANSITIONS, assert_transition
from .transport import LocalHTTPClient, TransportFailure
from .validation import (
    FunctionSpec,
    normalize_function_batch,
    normalize_trace_batch,
    validate_invocation_raw,
)

TERMINAL_CALL_STATUSES = ("SUCCEEDED", "FAILED")
INJECTION_KINDS = {
    "INSTANCE_CREATE_TIMEOUT",
    "EXECUTION_INTERRUPT",
    "DUPLICATE_CALL_ID",
    "QUOTA_BURST",
    "TRACE_OUT_OF_ORDER",
}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return default


def _row(row: Any) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _spec_from_row(row: Any) -> FunctionSpec:
    return FunctionSpec(
        function_id=row["function_id"],
        runtime=row["runtime"],
        init_us=row["init_us"],
        memory_bytes=row["memory_bytes"],
        per_instance_concurrency=row["per_instance_concurrency"],
        function_concurrency_limit=row["function_concurrency_limit"],
        reserved_quota=row["reserved_quota"],
        init_concurrency=row["init_concurrency"],
        prewarm_instances=row["prewarm_instances"],
        queue_capacity=row["queue_capacity"],
    )


@dataclass(frozen=True)
class AdmissionAction:
    call_id: str
    function_id: str
    instance_id: str
    needs_creation: bool
    is_cold_start: bool


class RuntimeEngine:
    """SQLite-backed local runtime simulator.

    Writes are serialized per function in-process and use BEGIN IMMEDIATE in
    SQLite. Readers use a separate deferred transaction and therefore observe
    one complete commit, never a half-applied admission or reclaim.
    """

    def __init__(self, settings: Settings | None = None, *, recover: bool = True) -> None:
        self.settings = settings or Settings.from_env()
        self.settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        initialize_database(self.settings.db_path, self.settings.sqlite_busy_timeout_s)
        self.http = LocalHTTPClient(self.settings.endpoint_base)
        self._locks: dict[str, threading.RLock] = {}
        self._conditions: dict[str, threading.Condition] = {}
        self._meta_lock = threading.RLock()
        self._init_semaphores: dict[tuple[str, int], threading.BoundedSemaphore] = {}
        self.recovery_report: dict[str, Any] | None = None
        if recover:
            self.recovery_report = self.recover_incomplete_work()

    # ------------------------------------------------------------------
    # Locking and common DB helpers
    # ------------------------------------------------------------------
    def _function_lock(self, function_id: str) -> threading.RLock:
        with self._meta_lock:
            lock = self._locks.get(function_id)
            if lock is None:
                lock = threading.RLock()
                self._locks[function_id] = lock
            return lock

    def _condition(self, function_id: str) -> threading.Condition:
        with self._meta_lock:
            condition = self._conditions.get(function_id)
            if condition is None:
                condition = threading.Condition(self._function_lock(function_id))
                self._conditions[function_id] = condition
            return condition

    def _write(self):
        return write_transaction(self.settings.db_path, self.settings.sqlite_busy_timeout_s)

    def _read(self):
        return read_transaction(self.settings.db_path, self.settings.sqlite_busy_timeout_s)

    def _read_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._read() as con:
            return _row(con.execute(sql, params).fetchone())

    def _active_spec(self, con: Any, function_id: str) -> FunctionSpec:
        row = con.execute(
            "SELECT * FROM function_specs WHERE function_id = ? AND active = 1",
            (function_id,),
        ).fetchone()
        if row is None:
            raise BusinessError(
                E_SPEC_INVALID,
                "call references an undeclared function",
                detail={"function_id": function_id},
            )
        return _spec_from_row(row)

    def _call_row(self, con: Any, call_id: str) -> dict[str, Any] | None:
        return _row(con.execute("SELECT * FROM calls WHERE call_id = ?", (call_id,)).fetchone())

    def _instance_row(self, con: Any, instance_id: str) -> dict[str, Any] | None:
        return _row(
            con.execute("SELECT * FROM instances WHERE instance_id = ?", (instance_id,)).fetchone()
        )

    def _quota_row(self, con: Any, function_id: str) -> dict[str, Any]:
        row = con.execute(
            "SELECT * FROM quota_usage WHERE function_id = ?", (function_id,)
        ).fetchone()
        if row is None:
            raise BusinessError(
                E_INTERNAL, "quota row missing for function", detail={"function_id": function_id}
            )
        return dict(row)

    def _runtime_row(self, con: Any, function_id: str) -> dict[str, Any]:
        row = con.execute(
            "SELECT * FROM function_runtime WHERE function_id = ?", (function_id,)
        ).fetchone()
        if row is None:
            raise BusinessError(
                E_INTERNAL, "runtime row missing for function", detail={"function_id": function_id}
            )
        return dict(row)

    def _next_created_seq(self, con: Any) -> int:
        row = con.execute("SELECT COALESCE(MAX(created_seq), 0) + 1 AS seq FROM calls").fetchone()
        return int(row["seq"])

    def _insert_intent(
        self,
        con: Any,
        *,
        kind: str,
        status: str,
        function_id: str | None,
        call_id: str | None,
        instance_id: str | None,
        payload: dict[str, Any],
        error_code: str | None = None,
        error: dict[str, Any] | None = None,
        intent_id: str | None = None,
    ) -> str:
        intent_id = intent_id or _new_id("intent")
        timestamp = now_ms()
        con.execute(
            "INSERT INTO intents(intent_id, kind, status, function_id, call_id, instance_id, "
            "payload_json, error_code, error_json, created_at_ms, updated_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                intent_id,
                kind,
                status,
                function_id,
                call_id,
                instance_id,
                _json(payload),
                error_code,
                _json(error) if error is not None else None,
                timestamp,
                timestamp,
            ),
        )
        return intent_id

    def _update_intent(
        self,
        con: Any,
        intent_id: str,
        *,
        status: str,
        error_code: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        con.execute(
            "UPDATE intents SET status = ?, error_code = ?, error_json = ?, updated_at_ms = ? "
            "WHERE intent_id = ?",
            (
                status,
                error_code,
                _json(error) if error is not None else None,
                now_ms(),
                intent_id,
            ),
        )

    def _notify(self, function_id: str) -> None:
        condition = self._condition(function_id)
        with condition:
            condition.notify_all()

    # ------------------------------------------------------------------
    # Spec and trace ingestion
    # ------------------------------------------------------------------
    def load_specs(self, payload: Any, *, source: str = "inline") -> dict[str, Any]:
        specs, seed = normalize_function_batch(payload)
        batch_id = _new_id("spec-batch")
        timestamp = now_ms()
        versions: dict[str, int] = {}
        with self._write() as con:
            con.execute(
                "INSERT INTO spec_batches(batch_id, source, seed, function_count, created_at_ms) "
                "VALUES (?, ?, ?, ?, ?)",
                (batch_id, source, seed, len(specs), timestamp),
            )
            for spec in specs:
                row = con.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version FROM function_specs "
                    "WHERE function_id = ?",
                    (spec.function_id,),
                ).fetchone()
                version = int(row["version"]) + 1
                usage = con.execute(
                    "SELECT * FROM quota_usage WHERE function_id = ?", (spec.function_id,)
                ).fetchone()
                if usage is not None and (
                    usage["on_demand_used"] > spec.on_demand_capacity
                    or usage["reserved_used"] > spec.reserved_quota
                ):
                    raise BusinessError(
                        E_SPEC_INVALID,
                        "new spec limits are lower than currently occupied quota",
                        detail={
                            "function_id": spec.function_id,
                            "current_on_demand_used": usage["on_demand_used"],
                            "current_reserved_used": usage["reserved_used"],
                            "next_on_demand_capacity": spec.on_demand_capacity,
                            "next_reserved_quota": spec.reserved_quota,
                        },
                        http_status=409,
                    )
                con.execute(
                    "UPDATE function_specs SET active = 0 WHERE function_id = ? AND active = 1",
                    (spec.function_id,),
                )
                con.execute(
                    "INSERT INTO function_specs(function_id, version, batch_id, runtime, init_us, "
                    "memory_bytes, per_instance_concurrency, function_concurrency_limit, "
                    "reserved_quota, init_concurrency, prewarm_instances, queue_capacity, active, "
                    "created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)",
                    (
                        spec.function_id,
                        version,
                        batch_id,
                        spec.runtime,
                        spec.init_us,
                        spec.memory_bytes,
                        spec.per_instance_concurrency,
                        spec.function_concurrency_limit,
                        spec.reserved_quota,
                        spec.init_concurrency,
                        spec.prewarm_instances,
                        spec.queue_capacity,
                        timestamp,
                    ),
                )
                con.execute(
                    "INSERT INTO function_runtime(function_id, spec_version, active_instances, "
                    "available_instances, cold_starts, warm_starts, start_timeouts, exec_timeouts, "
                    "updated_at_ms) VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?) "
                    "ON CONFLICT(function_id) DO UPDATE SET spec_version = excluded.spec_version, "
                    "updated_at_ms = excluded.updated_at_ms",
                    (spec.function_id, version, timestamp),
                )
                con.execute(
                    "INSERT INTO quota_usage(function_id, spec_version, on_demand_used, reserved_used, "
                    "updated_at_ms) VALUES (?, ?, 0, 0, ?) "
                    "ON CONFLICT(function_id) DO UPDATE SET spec_version = excluded.spec_version, "
                    "updated_at_ms = excluded.updated_at_ms",
                    (spec.function_id, version, timestamp),
                )
                versions[spec.function_id] = version
            commit_seq = bump_commit_seq(con)
        self._refresh_semaphores()
        return {
            "batch_id": batch_id,
            "source": source,
            "seed": seed,
            "function_count": len(specs),
            "versions": versions,
            "commit_seq": commit_seq,
        }

    def load_trace(
        self,
        payload: Any,
        *,
        source: str = "inline",
        strict_order: bool = True,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        with self._read() as con:
            known = {
                row["function_id"]
                for row in con.execute(
                    "SELECT function_id FROM function_specs WHERE active = 1"
                ).fetchall()
            }
        normalized_batch_id, records, seed = normalize_trace_batch(
            payload,
            known_functions=known,
            strict_order=strict_order,
            batch_id=batch_id,
            source=source,
        )
        fingerprint = hashlib.sha256(
            _json(
                {
                    "strict_order": strict_order,
                    "records": [record.as_dict() for record in records],
                }
            ).encode("utf-8")
        ).hexdigest()
        timestamp = now_ms()
        with self._write() as con:
            existing = con.execute(
                "SELECT * FROM trace_batches WHERE batch_id = ?", (normalized_batch_id,)
            ).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise BusinessError(
                        E_SPEC_INVALID,
                        "trace batch_id was reused with different content",
                        detail={
                            "batch_id": normalized_batch_id,
                            "existing_record_count": existing["record_count"],
                            "new_record_count": len(records),
                        },
                        http_status=409,
                    )
                return {
                    "batch_id": normalized_batch_id,
                    "record_count": existing["record_count"],
                    "idempotent_reuse": True,
                    "commit_seq": bump_commit_seq(con),
                }
            con.execute(
                "INSERT INTO trace_batches(batch_id, source, seed, strict_order, record_count, "
                "fingerprint, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    normalized_batch_id,
                    source,
                    seed,
                    int(strict_order),
                    len(records),
                    fingerprint,
                    timestamp,
                ),
            )
            for record in records:
                con.execute(
                    "INSERT INTO trace_records(batch_id, sequence, call_id, function_id, arrival_ms, "
                    "execution_us, priority, retry) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        normalized_batch_id,
                        record.sequence,
                        record.call_id,
                        record.function_id,
                        record.arrival_ms,
                        record.execution_us,
                        record.priority,
                        int(record.retry),
                    ),
                )
            commit_seq = bump_commit_seq(con)
        return {
            "batch_id": normalized_batch_id,
            "source": source,
            "seed": seed,
            "record_count": len(records),
            "strict_order": strict_order,
            "idempotent_reuse": False,
            "commit_seq": commit_seq,
        }

    def get_trace(self, batch_id: str) -> dict[str, Any]:
        with self._read() as con:
            batch = con.execute(
                "SELECT * FROM trace_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise BusinessError(E_NOT_FOUND, "trace batch not found", http_status=404)
            records = [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM trace_records WHERE batch_id = ? ORDER BY sequence",
                    (batch_id,),
                ).fetchall()
            ]
            return {**dict(batch), "strict_order": bool(batch["strict_order"]), "records": records}

    def replay_trace(
        self,
        batch_id: str,
        *,
        time_scale: float = 1.0,
        max_workers: int = 32,
    ) -> dict[str, Any]:
        if isinstance(time_scale, bool) or not isinstance(time_scale, (int, float)) or time_scale < 0:
            raise BusinessError(E_SPEC_INVALID, "time_scale must be a non-negative number")
        if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers <= 0:
            raise BusinessError(E_SPEC_INVALID, "max_workers must be a positive integer")
        trace = self.get_trace(batch_id)
        records = trace["records"]
        base = time.monotonic()

        def run_record(record: dict[str, Any]) -> dict[str, Any]:
            target = base + (record["arrival_ms"] / 1000.0) * float(time_scale)
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            try:
                return self.invoke(
                    {
                        "call_id": record["call_id"],
                        "function_id": record["function_id"],
                        "arrival_ms": record["arrival_ms"],
                        "execution_us": record["execution_us"],
                        "priority": record["priority"],
                        "retry": bool(record["retry"]),
                    }
                )
            except BusinessError as exc:
                return {
                    "ok": False,
                    "business_code": exc.code,
                    "message": exc.message,
                    "detail": exc.detail,
                    "call_id": record["call_id"],
                    "function_id": record["function_id"],
                }

        with ThreadPoolExecutor(
            max_workers=min(max_workers, max(1, len(records))),
            thread_name_prefix="trace-replay",
        ) as pool:
            futures = {
                pool.submit(run_record, record): record["sequence"] for record in records
            }
            completed = {}
            for future in as_completed(futures):
                completed[futures[future]] = future.result()
        results = [completed[index] for index in sorted(completed)]
        return {
            "batch_id": batch_id,
            "record_count": len(records),
            "time_scale": float(time_scale),
            "results": results,
            "metrics": self.metrics(),
            "audit": self.audit_invariants(),
        }

    # ------------------------------------------------------------------
    # Instance creation and prewarming
    # ------------------------------------------------------------------
    def _refresh_semaphores(self) -> None:
        with self._read() as con:
            rows = con.execute(
                "SELECT function_id, init_concurrency FROM function_specs WHERE active = 1"
            ).fetchall()
        with self._meta_lock:
            for row in rows:
                key = (row["function_id"], int(row["init_concurrency"]))
                if key not in self._init_semaphores:
                    self._init_semaphores[key] = threading.BoundedSemaphore(
                        int(row["init_concurrency"])
                    )

    def _semaphore_for(self, spec: FunctionSpec) -> threading.BoundedSemaphore:
        key = (spec.function_id, spec.init_concurrency)
        with self._meta_lock:
            semaphore = self._init_semaphores.get(key)
            if semaphore is None:
                semaphore = threading.BoundedSemaphore(spec.init_concurrency)
                self._init_semaphores[key] = semaphore
            return semaphore

    def _create_instance_http(
        self,
        *,
        function_id: str,
        spec: FunctionSpec,
        instance_id: str,
        intent_id: str,
        call_id: str | None,
        create_timeout_injected: bool,
    ) -> tuple[bool, dict[str, Any]]:
        total_started = time.monotonic()
        with self._semaphore_for(spec):
            semaphore_acquired = time.monotonic()
            init_queue_wait_us = int((semaphore_acquired - total_started) * 1_000_000)
            path = "/hang" if create_timeout_injected else "/start"
            try:
                result = self.http.get_json(
                    path,
                    {
                        "duration_us": spec.init_us,
                        "instance_id": instance_id,
                        "function_id": function_id,
                    },
                    timeout_s=self.settings.start_timeout_s,
                )
            except TransportFailure as failure:
                total_elapsed_us = int((time.monotonic() - total_started) * 1_000_000)
                self._finalize_start_failure(
                    function_id=function_id,
                    instance_id=instance_id,
                    intent_id=intent_id,
                    call_id=call_id,
                    failure=failure,
                    init_queue_wait_us=init_queue_wait_us,
                    total_elapsed_us=total_elapsed_us,
                )
                return False, {
                    "business_code": E_START_TIMEOUT if failure.timed_out else E_START_FAILED,
                    "timed_out": failure.timed_out,
                    "elapsed_us": int(failure.elapsed_s * 1_000_000),
                    "start_http_elapsed_us": int(failure.elapsed_s * 1_000_000),
                    "init_queue_wait_us": init_queue_wait_us,
                    "start_elapsed_us": total_elapsed_us,
                    "transport_cause": failure.cause,
                    "timeout_s": self.settings.start_timeout_s,
                    "injected": create_timeout_injected,
                }
        total_elapsed_us = int((time.monotonic() - total_started) * 1_000_000)
        return self._finalize_start_success(
            function_id=function_id,
            instance_id=instance_id,
            intent_id=intent_id,
            call_id=call_id,
            http_elapsed_us=int(result.elapsed_s * 1_000_000),
            init_queue_wait_us=init_queue_wait_us,
            total_elapsed_us=total_elapsed_us,
        )

    def _finalize_start_success(
        self,
        *,
        function_id: str,
        instance_id: str,
        intent_id: str,
        call_id: str | None,
        http_elapsed_us: int,
        init_queue_wait_us: int,
        total_elapsed_us: int,
    ) -> tuple[bool, dict[str, Any]]:
        with self._condition(function_id):
            with self._write() as con:
                instance = self._instance_row(con, instance_id)
                if instance is None or instance["state"] != "CREATING":
                    raise BusinessError(
                        E_INVALID_TRANSITION,
                        "instance is no longer CREATING when start acknowledgement arrives",
                        detail={
                            "instance_id": instance_id,
                            "state": None if instance is None else instance["state"],
                        },
                    )
                target_state = "BUSY" if instance["occupied_slots"] > 0 else "READY"
                assert_transition("CREATING", target_state, instance["occupied_slots"])
                timestamp = now_ms()
                con.execute(
                    "UPDATE instances SET state = ?, last_active_ms = ?, updated_at_ms = ? "
                    "WHERE instance_id = ?",
                    (target_state, timestamp, timestamp, instance_id),
                )
                if call_id is not None:
                    call = self._call_row(con, call_id)
                    if call is not None and call["status"] == "CREATING":
                        call_detail = _loads(call.get("detail_json"), {}) or {}
                        call_detail.update(
                            {
                                "start_elapsed_us": total_elapsed_us,
                                "start_http_elapsed_us": http_elapsed_us,
                                "init_queue_wait_us": init_queue_wait_us,
                            }
                        )
                        con.execute(
                            "UPDATE calls SET status = 'ADMITTED', admitted_at_ms = ?, "
                            "detail_json = ?, updated_at_ms = ? WHERE call_id = ?",
                            (timestamp, _json(call_detail), timestamp, call_id),
                        )
                    else:
                        con.execute(
                            "UPDATE calls SET status = 'ADMITTED', admitted_at_ms = ?, "
                            "updated_at_ms = ? WHERE instance_id = ? AND status = 'CREATING'",
                            (timestamp, timestamp, instance_id),
                        )
                else:
                    con.execute(
                        "UPDATE calls SET status = 'ADMITTED', admitted_at_ms = ?, "
                        "updated_at_ms = ? WHERE instance_id = ? AND status = 'CREATING'",
                        (timestamp, timestamp, instance_id),
                    )
                if target_state == "READY":
                    con.execute(
                        "UPDATE function_runtime SET available_instances = available_instances + 1, "
                        "updated_at_ms = ? WHERE function_id = ?",
                        (timestamp, function_id),
                    )
                self._update_intent(con, intent_id, status="SUCCEEDED")
                commit_seq = bump_commit_seq(con)
            self._notify(function_id)
        return True, {
            "instance_state": target_state,
            "start_elapsed_us": total_elapsed_us,
            "start_http_elapsed_us": http_elapsed_us,
            "init_queue_wait_us": init_queue_wait_us,
            "commit_seq": commit_seq,
        }

    def _finalize_start_failure(
        self,
        *,
        function_id: str,
        instance_id: str,
        intent_id: str,
        call_id: str | None,
        failure: TransportFailure,
        init_queue_wait_us: int,
        total_elapsed_us: int,
    ) -> None:
        code = E_START_TIMEOUT if failure.timed_out else E_START_FAILED
        detail = {
            "instance_id": instance_id,
            "timed_out": failure.timed_out,
            "elapsed_us": int(failure.elapsed_s * 1_000_000),
            "start_http_elapsed_us": int(failure.elapsed_s * 1_000_000),
            "init_queue_wait_us": init_queue_wait_us,
            "start_elapsed_us": total_elapsed_us,
            "transport_cause": failure.cause,
            "timeout_s": self.settings.start_timeout_s,
        }
        with self._condition(function_id):
            with self._write() as con:
                instance = self._instance_row(con, instance_id)
                if instance is not None and instance["state"] != "TERMINATED":
                    con.execute(
                        "UPDATE instances SET state = 'TERMINATED', occupied_slots = 0, "
                        "updated_at_ms = ? WHERE instance_id = ?",
                        (now_ms(), instance_id),
                    )
                    con.execute(
                        "UPDATE function_runtime SET active_instances = MAX(0, active_instances - 1), "
                        "available_instances = MAX(0, available_instances - ?), "
                        "start_timeouts = start_timeouts + ?, updated_at_ms = ? "
                        "WHERE function_id = ?",
                        (
                            1 if instance["state"] == "READY" else 0,
                            1 if failure.timed_out else 0,
                            now_ms(),
                            function_id,
                        ),
                    )
                if call_id is not None:
                    call = self._call_row(con, call_id)
                    if call is not None and call["status"] not in TERMINAL_CALL_STATUSES:
                        if call["quota_bucket"]:
                            self._release_quota(con, function_id, call["quota_bucket"])
                        con.execute(
                            "UPDATE calls SET status = 'FAILED', business_code = ?, message = ?, "
                            "detail_json = ?, completed_at_ms = ?, updated_at_ms = ? WHERE call_id = ?",
                            (
                                code,
                                "instance creation failed or timed out",
                                _json(detail),
                                now_ms(),
                                now_ms(),
                                call_id,
                            ),
                        )
                self._update_intent(con, intent_id, status="FAILED", error_code=code, error=detail)
                bump_commit_seq(con)
            self._notify(function_id)

    def prewarm(self, function_id: str, count: int) -> dict[str, Any]:
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise BusinessError(E_SPEC_INVALID, "prewarm count must be a positive integer")
        with self._read() as con:
            spec = self._active_spec(con, function_id)
        created: list[tuple[str, str, int]] = []
        with self._condition(function_id):
            with self._write() as con:
                active = con.execute(
                    "SELECT COUNT(*) AS n FROM instances WHERE function_id = ? "
                    "AND state IN ('CREATING','READY','BUSY','DRAINING')",
                    (function_id,),
                ).fetchone()["n"]
                to_create = min(count, max(0, spec.max_instances - int(active)))
                timestamp = now_ms()
                version = con.execute(
                    "SELECT version FROM function_specs WHERE function_id = ? AND active = 1",
                    (function_id,),
                ).fetchone()["version"]
                for _ in range(to_create):
                    instance_id = _new_id("instance")
                    intent_id = self._insert_intent(
                        con,
                        kind="CREATE_INSTANCE",
                        status="PENDING",
                        function_id=function_id,
                        call_id=None,
                        instance_id=instance_id,
                        payload={"mode": "PREWARM", "init_us": spec.init_us},
                    )
                    con.execute(
                        "INSERT INTO instances(instance_id, function_id, spec_version, state, capacity, "
                        "occupied_slots, last_active_ms, created_at_ms, updated_at_ms) "
                        "VALUES (?, ?, ?, 'CREATING', ?, 0, ?, ?, ?)",
                        (
                            instance_id,
                            function_id,
                            version,
                            spec.per_instance_concurrency,
                            timestamp,
                            timestamp,
                            timestamp,
                        ),
                    )
                    con.execute(
                        "UPDATE function_runtime SET active_instances = active_instances + 1, "
                        "updated_at_ms = ? WHERE function_id = ?",
                        (timestamp, function_id),
                    )
                    created.append((instance_id, intent_id, version))
                bump_commit_seq(con)
        outcomes: list[dict[str, Any]] = []
        if created:
            workers = min(spec.init_concurrency, len(created))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="prewarm") as pool:
                futures = {
                    pool.submit(
                        self._create_instance_http,
                        function_id=function_id,
                        spec=spec,
                        instance_id=instance_id,
                        intent_id=intent_id,
                        call_id=None,
                        create_timeout_injected=False,
                    ): instance_id
                    for instance_id, intent_id, _ in created
                }
                for future in as_completed(futures):
                    instance_id = futures[future]
                    try:
                        ok, detail = future.result()
                    except Exception as exc:
                        ok, detail = False, {"business_code": E_INTERNAL, "error": str(exc)}
                    outcomes.append({"instance_id": instance_id, "ok": ok, "detail": detail})
        with self._read() as con:
            snapshot = self._snapshot_with_connection(con, function_id=function_id)
        return {
            "function_id": function_id,
            "requested": count,
            "started": len(created),
            "outcomes": outcomes,
            "snapshot": snapshot,
        }

    # ------------------------------------------------------------------
    # Admission, priority queue and invocation execution
    # ------------------------------------------------------------------
    def _quota_decision(
        self,
        spec: FunctionSpec,
        usage: dict[str, Any],
        requested_class: str,
    ) -> str | None:
        if requested_class == "RESERVED":
            if spec.reserved_quota <= 0 or usage["reserved_used"] >= spec.reserved_quota:
                raise BusinessError(
                    E_QUOTA_DENIED,
                    "reserved quota is not declared for this function or is exhausted",
                    detail={
                        "function_id": spec.function_id,
                        "requested_quota_class": requested_class,
                        "reserved_quota": spec.reserved_quota,
                        "on_demand_capacity": spec.on_demand_capacity,
                        "current_on_demand_used": usage["on_demand_used"],
                        "current_reserved_used": usage["reserved_used"],
                    },
                    http_status=403,
                )
            return "RESERVED"
        if requested_class == "ON_DEMAND":
            return (
                "ON_DEMAND"
                if usage["on_demand_used"] < spec.on_demand_capacity
                else None
            )
        if usage["on_demand_used"] < spec.on_demand_capacity:
            return "ON_DEMAND"
        if spec.reserved_quota > 0 and usage["reserved_used"] < spec.reserved_quota:
            return "RESERVED"
        return None

    def _select_existing_instance(self, con: Any, function_id: str) -> dict[str, Any] | None:
        return _row(
            con.execute(
                "SELECT * FROM instances WHERE function_id = ? AND state IN ('READY','BUSY') "
                "AND occupied_slots < capacity ORDER BY "
                "CASE state WHEN 'READY' THEN 0 ELSE 1 END, occupied_slots DESC, "
                "instance_id ASC LIMIT 1",
                (function_id,),
            ).fetchone()
        )

    def _claim_quota(self, con: Any, function_id: str, bucket: str, timestamp: int) -> None:
        column = "on_demand_used" if bucket == "ON_DEMAND" else "reserved_used"
        con.execute(
            f"UPDATE quota_usage SET {column} = {column} + 1, updated_at_ms = ? "
            "WHERE function_id = ?",
            (timestamp, function_id),
        )

    def _release_quota(self, con: Any, function_id: str, bucket: str) -> None:
        column = "on_demand_used" if bucket == "ON_DEMAND" else "reserved_used"
        con.execute(
            f"UPDATE quota_usage SET {column} = MAX(0, {column} - 1), updated_at_ms = ? "
            "WHERE function_id = ?",
            (now_ms(), function_id),
        )

    def _can_assign_instance(self, con: Any, spec: FunctionSpec) -> bool:
        if self._select_existing_instance(con, spec.function_id) is not None:
            return True
        active = con.execute(
            "SELECT COUNT(*) AS n FROM instances WHERE function_id = ? "
            "AND state IN ('CREATING','READY','BUSY','DRAINING')",
            (spec.function_id,),
        ).fetchone()["n"]
        return int(active) < spec.max_instances

    def _assign_instance_locked(
        self, con: Any, *, spec: FunctionSpec, call_id: str, timestamp: int
    ) -> tuple[str, bool, str]:
        existing = self._select_existing_instance(con, spec.function_id)
        if existing is not None:
            new_occupied = int(existing["occupied_slots"]) + 1
            if existing["state"] == "READY":
                assert_transition("READY", "BUSY", new_occupied)
            con.execute(
                "UPDATE instances SET state = 'BUSY', occupied_slots = ?, last_active_ms = ?, "
                "updated_at_ms = ? WHERE instance_id = ?",
                (new_occupied, timestamp, timestamp, existing["instance_id"]),
            )
            if existing["state"] == "READY":
                con.execute(
                    "UPDATE function_runtime SET available_instances = "
                    "MAX(0, available_instances - 1), warm_starts = warm_starts + 1, "
                    "updated_at_ms = ? WHERE function_id = ?",
                    (timestamp, spec.function_id),
                )
            else:
                con.execute(
                    "UPDATE function_runtime SET warm_starts = warm_starts + 1, "
                    "updated_at_ms = ? WHERE function_id = ?",
                    (timestamp, spec.function_id),
                )
            return existing["instance_id"], False, "ADMITTED"
        active = con.execute(
            "SELECT COUNT(*) AS n FROM instances WHERE function_id = ? "
            "AND state IN ('CREATING','READY','BUSY','DRAINING')",
            (spec.function_id,),
        ).fetchone()["n"]
        if int(active) >= spec.max_instances:
            raise BusinessError(
                E_CONCURRENCY_LIMIT,
                "all instance capacity is busy, creating or draining",
                detail={
                    "function_id": spec.function_id,
                    "active_instances": int(active),
                    "max_instances": spec.max_instances,
                },
                http_status=429,
            )
        instance_id = _new_id("instance")
        version = con.execute(
            "SELECT version FROM function_specs WHERE function_id = ? AND active = 1",
            (spec.function_id,),
        ).fetchone()["version"]
        con.execute(
            "INSERT INTO instances(instance_id, function_id, spec_version, state, capacity, "
            "occupied_slots, last_active_ms, created_at_ms, updated_at_ms) "
            "VALUES (?, ?, ?, 'CREATING', ?, 1, ?, ?, ?)",
            (
                instance_id,
                spec.function_id,
                version,
                spec.per_instance_concurrency,
                timestamp,
                timestamp,
                timestamp,
            ),
        )
        con.execute(
            "UPDATE function_runtime SET active_instances = active_instances + 1, "
            "cold_starts = cold_starts + 1, updated_at_ms = ? WHERE function_id = ?",
            (timestamp, spec.function_id),
        )
        return instance_id, True, "CREATING"

    def _insert_call(
        self,
        con: Any,
        *,
        request: dict[str, Any],
        spec: FunctionSpec,
        status: str,
        quota_bucket: str | None,
        instance_id: str | None,
        is_cold_start: bool | None,
        created_seq: int,
        timestamp: int,
    ) -> None:
        version = con.execute(
            "SELECT version FROM function_specs WHERE function_id = ? AND active = 1",
            (spec.function_id,),
        ).fetchone()["version"]
        con.execute(
            "INSERT INTO calls(call_id, fingerprint, function_id, spec_version, arrival_ms, "
            "execution_us, priority, retry, quota_class, quota_bucket, status, is_cold_start, "
            "instance_id, created_seq, injection_json, request_json, created_at_ms, updated_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                request["call_id"],
                request["fingerprint"],
                request["function_id"],
                version,
                request["arrival_ms"],
                request["execution_us"],
                request["priority"],
                int(request["retry"]),
                request["quota_class"],
                quota_bucket,
                status,
                None if is_cold_start is None else int(is_cold_start),
                instance_id,
                created_seq,
                _json(request["injection"]),
                _json(request["request"]),
                timestamp,
                timestamp,
            ),
        )

    def _admit_atomic(
        self, request: dict[str, Any], spec: FunctionSpec
    ) -> AdmissionAction | dict[str, Any]:
        timestamp = now_ms()
        with self._write() as con:
            existing = self._call_row(con, request["call_id"])
            if existing is not None:
                return {"existing": existing}
            usage = self._quota_row(con, request["function_id"])
            queue_count = con.execute(
                "SELECT COUNT(*) AS n FROM calls WHERE function_id = ? AND status = 'QUEUED'",
                (request["function_id"],),
            ).fetchone()["n"]
            bucket = self._quota_decision(spec, usage, request["quota_class"])
            can_admit = (
                int(queue_count) == 0
                and bucket is not None
                and self._can_assign_instance(con, spec)
            )
            if not can_admit:
                if int(queue_count) >= spec.queue_capacity:
                    raise BusinessError(
                        E_CONCURRENCY_LIMIT,
                        "function concurrency and bounded wait queue are both full",
                        detail={
                            "function_id": spec.function_id,
                            "function_concurrency_limit": spec.function_concurrency_limit,
                            "queue_capacity": spec.queue_capacity,
                            "queued_count": int(queue_count),
                            "on_demand_capacity": spec.on_demand_capacity,
                            "on_demand_used": usage["on_demand_used"],
                            "reserved_quota": spec.reserved_quota,
                            "reserved_used": usage["reserved_used"],
                        },
                        http_status=429,
                    )
                self._insert_call(
                    con,
                    request=request,
                    spec=spec,
                    status="QUEUED",
                    quota_bucket=None,
                    instance_id=None,
                    is_cold_start=None,
                    created_seq=self._next_created_seq(con),
                    timestamp=timestamp,
                )
                self._insert_intent(
                    con,
                    kind="ENQUEUE_CALL",
                    status="SUCCEEDED",
                    function_id=request["function_id"],
                    call_id=request["call_id"],
                    instance_id=None,
                    payload={
                        "priority": request["priority"],
                        "retry": request["retry"],
                        "queue_capacity": spec.queue_capacity,
                    },
                )
                bump_commit_seq(con)
                return {"queued": True, "call_id": request["call_id"]}
            assert bucket is not None
            instance_id, cold, call_status = self._assign_instance_locked(
                con, spec=spec, call_id=request["call_id"], timestamp=timestamp
            )
            self._claim_quota(con, request["function_id"], bucket, timestamp)
            self._insert_call(
                con,
                request=request,
                spec=spec,
                status=call_status,
                quota_bucket=bucket,
                instance_id=instance_id,
                is_cold_start=cold,
                created_seq=self._next_created_seq(con),
                timestamp=timestamp,
            )
            self._insert_intent(
                con,
                kind="ADMIT_CALL",
                status="SUCCEEDED",
                function_id=request["function_id"],
                call_id=request["call_id"],
                instance_id=instance_id,
                payload={
                    "quota_class": request["quota_class"],
                    "quota_bucket": bucket,
                    "cold_start": cold,
                },
            )
            if cold:
                self._insert_intent(
                    con,
                    kind="CREATE_INSTANCE",
                    status="PENDING",
                    function_id=request["function_id"],
                    call_id=request["call_id"],
                    instance_id=instance_id,
                    payload={"init_us": spec.init_us, "mode": "INVOCATION"},
                )
            bump_commit_seq(con)
        return AdmissionAction(
            call_id=request["call_id"],
            function_id=request["function_id"],
            instance_id=instance_id,
            needs_creation=cold,
            is_cold_start=cold,
        )

    def _queue_head(self, con: Any, function_id: str) -> dict[str, Any] | None:
        return _row(
            con.execute(
                "SELECT * FROM calls WHERE function_id = ? AND status = 'QUEUED' "
                "ORDER BY priority DESC, retry DESC, arrival_ms ASC, created_seq ASC, "
                "call_id ASC LIMIT 1",
                (function_id,),
            ).fetchone()
        )

    def _promote_queued_atomic(
        self, call_id: str, spec: FunctionSpec
    ) -> AdmissionAction | None:
        timestamp = now_ms()
        with self._write() as con:
            call = self._call_row(con, call_id)
            if call is None or call["status"] != "QUEUED":
                return None
            head = self._queue_head(con, call["function_id"])
            if head is None or head["call_id"] != call_id:
                return None
            usage = self._quota_row(con, call["function_id"])
            bucket = self._quota_decision(spec, usage, call["quota_class"])
            if bucket is None or not self._can_assign_instance(con, spec):
                return None
            instance_id, cold, call_status = self._assign_instance_locked(
                con, spec=spec, call_id=call_id, timestamp=timestamp
            )
            self._claim_quota(con, call["function_id"], bucket, timestamp)
            con.execute(
                "UPDATE calls SET status = ?, quota_bucket = ?, is_cold_start = ?, "
                "instance_id = ?, admitted_at_ms = ?, updated_at_ms = ? WHERE call_id = ?",
                (
                    call_status,
                    bucket,
                    int(cold),
                    instance_id,
                    timestamp,
                    timestamp,
                    call_id,
                ),
            )
            self._insert_intent(
                con,
                kind="ADMIT_CALL",
                status="SUCCEEDED",
                function_id=call["function_id"],
                call_id=call_id,
                instance_id=instance_id,
                payload={
                    "from_queue": True,
                    "quota_class": call["quota_class"],
                    "quota_bucket": bucket,
                    "cold_start": cold,
                },
            )
            if cold:
                self._insert_intent(
                    con,
                    kind="CREATE_INSTANCE",
                    status="PENDING",
                    function_id=call["function_id"],
                    call_id=call_id,
                    instance_id=instance_id,
                    payload={"init_us": spec.init_us, "mode": "QUEUED_INVOCATION"},
                )
            bump_commit_seq(con)
        return AdmissionAction(
            call_id=call_id,
            function_id=call["function_id"],
            instance_id=instance_id,
            needs_creation=cold,
            is_cold_start=cold,
        )

    def invoke(self, raw: Any) -> dict[str, Any]:
        request = validate_invocation_raw(raw)
        with self._read() as con:
            spec = self._active_spec(con, request["function_id"])
        existing = self._read_one("SELECT * FROM calls WHERE call_id = ?", (request["call_id"],))
        if existing is not None:
            return self._resolve_existing_invocation(request, existing)
        with self._condition(request["function_id"]):
            existing = self._read_one(
                "SELECT * FROM calls WHERE call_id = ?", (request["call_id"],)
            )
            if existing is not None:
                return self._resolve_existing_invocation(request, existing, lock_held=True)
            outcome = self._admit_atomic(request, spec)
            if isinstance(outcome, dict) and "existing" in outcome:
                return self._resolve_existing_invocation(
                    request, outcome["existing"], lock_held=True
                )
            if isinstance(outcome, dict) and outcome.get("queued"):
                action = self._wait_for_queue_action(request["call_id"], spec)
                if action is None:
                    return self._call_result_by_id(request["call_id"], deduplicated=False)
            else:
                assert isinstance(outcome, AdmissionAction)
                action = outcome
            self._notify(request["function_id"])
        return self._run_admitted_action(action, spec)

    def _resolve_existing_invocation(
        self, request: dict[str, Any], existing: dict[str, Any], *, lock_held: bool = False
    ) -> dict[str, Any]:
        if existing["fingerprint"] != request["fingerprint"]:
            raise BusinessError(
                E_CONFLICTING_CALL,
                "call_id was already submitted with different invocation content",
                detail={
                    "call_id": request["call_id"],
                    "existing_function_id": existing["function_id"],
                    "existing_quota_class": existing["quota_class"],
                    "existing_status": existing["status"],
                },
                http_status=409,
            )
        if existing["status"] in TERMINAL_CALL_STATUSES:
            return self._format_call(existing, deduplicated=True)
        condition = self._condition(existing["function_id"])
        deadline = time.monotonic() + self.settings.max_queue_wait_s
        with condition:
            while True:
                current = self._read_one(
                    "SELECT * FROM calls WHERE call_id = ?", (existing["call_id"],)
                )
                if current is None:
                    raise BusinessError(E_NOT_FOUND, "call disappeared while waiting")
                if current["status"] in TERMINAL_CALL_STATUSES:
                    return self._format_call(current, deduplicated=True)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BusinessError(
                        E_QUEUE_TIMEOUT,
                        "duplicate waiter timed out before the original call completed",
                        detail={"call_id": existing["call_id"], "status": current["status"]},
                        http_status=504,
                    )
                condition.wait(timeout=min(remaining, 0.1))

    def _wait_for_queue_action(
        self, call_id: str, spec: FunctionSpec
    ) -> AdmissionAction | None:
        deadline = time.monotonic() + self.settings.max_queue_wait_s
        condition = self._condition(spec.function_id)
        with condition:
            while True:
                call = self._read_one("SELECT * FROM calls WHERE call_id = ?", (call_id,))
                if call is None:
                    raise BusinessError(E_NOT_FOUND, "queued call disappeared")
                if call["status"] in TERMINAL_CALL_STATUSES:
                    return None
                if call["status"] == "QUEUED":
                    head = self._queue_head_read(spec.function_id)
                    if head is not None and head["call_id"] == call_id:
                        action = self._promote_queued_atomic(call_id, spec)
                        if action is not None:
                            self._notify(spec.function_id)
                            return action
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    with self._write() as con:
                        current = self._call_row(con, call_id)
                        if current is not None and current["status"] == "QUEUED":
                            con.execute(
                                "UPDATE calls SET status = 'FAILED', business_code = ?, "
                                "message = ?, completed_at_ms = ?, updated_at_ms = ? "
                                "WHERE call_id = ?",
                                (
                                    E_QUEUE_TIMEOUT,
                                    "bounded wait queue deadline expired",
                                    now_ms(),
                                    now_ms(),
                                    call_id,
                                ),
                            )
                            bump_commit_seq(con)
                    self._notify(spec.function_id)
                    return None
                condition.wait(timeout=min(remaining, 0.05))

    def _queue_head_read(self, function_id: str) -> dict[str, Any] | None:
        return self._read_one(
            "SELECT call_id FROM calls WHERE function_id = ? AND status = 'QUEUED' "
            "ORDER BY priority DESC, retry DESC, arrival_ms ASC, created_seq ASC, "
            "call_id ASC LIMIT 1",
            (function_id,),
        )

    def _run_admitted_action(
        self, action: AdmissionAction, spec: FunctionSpec
    ) -> dict[str, Any]:
        call = self._read_one("SELECT * FROM calls WHERE call_id = ?", (action.call_id,))
        if call is None:
            raise BusinessError(E_NOT_FOUND, "admitted call is missing")
        if call["status"] in TERMINAL_CALL_STATUSES:
            return self._format_call(call)
        if action.needs_creation:
            create_intent = self._read_one(
                "SELECT * FROM intents WHERE instance_id = ? AND kind = 'CREATE_INSTANCE' "
                "AND status = 'PENDING' ORDER BY created_at_ms DESC LIMIT 1",
                (action.instance_id,),
            )
            if create_intent is None:
                raise BusinessError(E_INTERNAL, "pending creation intent is missing")
            injection = _loads(call["injection_json"], {})
            ok, _detail = self._create_instance_http(
                function_id=action.function_id,
                spec=spec,
                instance_id=action.instance_id,
                intent_id=create_intent["intent_id"],
                call_id=action.call_id,
                create_timeout_injected=bool(injection.get("create_timeout")),
            )
            if not ok:
                return self._call_result_by_id(action.call_id)
        return self._execute_call(action.call_id)

    def _execute_call(self, call_id: str) -> dict[str, Any]:
        with self._write() as con:
            call = self._call_row(con, call_id)
            if call is None:
                raise BusinessError(E_NOT_FOUND, "call not found", http_status=404)
            if call["status"] in TERMINAL_CALL_STATUSES:
                return self._format_call(call)
            if call["status"] not in {"ADMITTED", "CREATING"}:
                raise BusinessError(
                    E_INVALID_TRANSITION,
                    "call is not in an executable state",
                    detail={"call_id": call_id, "status": call["status"]},
                )
            injection = _loads(call["injection_json"], {})
            intent_id = self._insert_intent(
                con,
                kind="EXECUTE_CALL",
                status="PENDING",
                function_id=call["function_id"],
                call_id=call_id,
                instance_id=call["instance_id"],
                payload={
                    "execution_us": call["execution_us"],
                    "injection": injection,
                    "timeout_s": self.settings.exec_timeout_s,
                },
            )
            timestamp = now_ms()
            con.execute(
                "UPDATE calls SET status = 'EXECUTING', started_at_ms = ?, updated_at_ms = ? "
                "WHERE call_id = ?",
                (timestamp, timestamp, call_id),
            )
            con.execute(
                "UPDATE instances SET last_active_ms = ?, updated_at_ms = ? WHERE instance_id = ?",
                (timestamp, timestamp, call["instance_id"]),
            )
            bump_commit_seq(con)
        path = "/hang" if injection.get("execution_interrupt") else "/execute"
        try:
            result = self.http.get_json(
                path,
                {
                    "duration_us": call["execution_us"],
                    "call_id": call_id,
                    "instance_id": call["instance_id"],
                },
                timeout_s=self.settings.exec_timeout_s,
            )
        except TransportFailure as failure:
            code = E_EXEC_TIMEOUT if failure.timed_out else E_EXEC_FAILED
            detail = {
                "timed_out": failure.timed_out,
                "elapsed_us": int(failure.elapsed_s * 1_000_000),
                "transport_cause": failure.cause,
                "timeout_s": self.settings.exec_timeout_s,
                "injected": bool(injection.get("execution_interrupt")),
            }
            self._finalize_execution(
                call_id=call_id,
                intent_id=intent_id,
                success=False,
                code=code,
                message="execution failed or timed out; the HTTP connection was closed",
                detail=detail,
                timed_out=failure.timed_out,
            )
        else:
            detail = {
                "timed_out": False,
                "elapsed_us": int(result.elapsed_s * 1_000_000),
                "http_status": result.status,
                "injected": bool(injection.get("execution_interrupt")),
            }
            self._finalize_execution(
                call_id=call_id,
                intent_id=intent_id,
                success=True,
                code="OK",
                message="execution completed",
                detail=detail,
                timed_out=False,
            )
        return self._call_result_by_id(call_id)

    def _release_instance_slot(self, con: Any, instance_id: str) -> dict[str, Any]:
        instance = self._instance_row(con, instance_id)
        if instance is None:
            return {"instance_id": instance_id, "state": "MISSING", "occupied_slots": 0}
        timestamp = now_ms()
        occupied = max(0, int(instance["occupied_slots"]) - 1)
        target = instance["state"]
        if instance["state"] == "BUSY" and occupied == 0:
            target = "READY"
            con.execute(
                "UPDATE function_runtime SET available_instances = available_instances + 1, "
                "updated_at_ms = ? WHERE function_id = ?",
                (timestamp, instance["function_id"]),
            )
        elif instance["state"] == "DRAINING" and occupied == 0:
            target = "TERMINATED"
            con.execute(
                "UPDATE function_runtime SET active_instances = MAX(0, active_instances - 1), "
                "updated_at_ms = ? "
                "WHERE function_id = ?",
                (timestamp, instance["function_id"]),
            )
            self._insert_intent(
                con,
                kind="RECLAIM_INSTANCE",
                status="SUCCEEDED",
                function_id=instance["function_id"],
                call_id=None,
                instance_id=instance_id,
                payload={"trigger": "last_inflight_call_completed", "from": "DRAINING"},
            )
        con.execute(
            "UPDATE instances SET state = ?, occupied_slots = ?, updated_at_ms = ? "
            "WHERE instance_id = ?",
            (target, occupied, timestamp, instance_id),
        )
        return {
            "instance_id": instance_id,
            "previous_state": instance["state"],
            "state": target,
            "occupied_slots": occupied,
            "terminated": target == "TERMINATED",
        }

    def _finalize_execution(
        self,
        *,
        call_id: str,
        intent_id: str,
        success: bool,
        code: str,
        message: str,
        detail: dict[str, Any],
        timed_out: bool,
    ) -> None:
        function_id: str | None = None
        with self._write() as con:
            call = self._call_row(con, call_id)
            if call is None:
                raise BusinessError(E_NOT_FOUND, "call not found during finalization")
            function_id = call["function_id"]
            if call["status"] in TERMINAL_CALL_STATUSES:
                self._update_intent(con, intent_id, status="SUCCEEDED")
                bump_commit_seq(con)
                return
            status = "SUCCEEDED" if success else "FAILED"
            timestamp = now_ms()
            if call["quota_bucket"]:
                self._release_quota(con, function_id, call["quota_bucket"])
            self._release_instance_slot(con, call["instance_id"])
            existing_detail = _loads(call.get("detail_json"), {}) or {}
            existing_detail.update(detail)
            con.execute(
                "UPDATE calls SET status = ?, business_code = ?, message = ?, detail_json = ?, "
                "result_json = ?, completed_at_ms = ?, updated_at_ms = ? WHERE call_id = ?",
                (
                    status,
                    code,
                    message,
                    _json(existing_detail),
                    _json({"ok": success, "business_code": code}),
                    timestamp,
                    timestamp,
                    call_id,
                ),
            )
            if timed_out:
                con.execute(
                    "UPDATE function_runtime SET exec_timeouts = exec_timeouts + 1, "
                    "updated_at_ms = ? WHERE function_id = ?",
                    (timestamp, function_id),
                )
            self._update_intent(
                con,
                intent_id,
                status="SUCCEEDED" if success else "FAILED",
                error_code=None if success else code,
                error=None if success else detail,
            )
            bump_commit_seq(con)
        if function_id:
            self._notify(function_id)

    # ------------------------------------------------------------------
    # Read-side result formatting and queries
    # ------------------------------------------------------------------
    def _format_call(self, call: dict[str, Any], *, deduplicated: bool = False) -> dict[str, Any]:
        status = call["status"]
        success = status == "SUCCEEDED"
        detail = _loads(call.get("detail_json"), {}) or {}
        return {
            "ok": success,
            "business_code": call.get("business_code") or "OK",
            "call_id": call["call_id"],
            "function_id": call["function_id"],
            "spec_version": call["spec_version"],
            "status": status,
            "cold_start": (
                bool(call["is_cold_start"])
                if call.get("is_cold_start") is not None
                else None
            ),
            "warm_start": (
                not bool(call["is_cold_start"])
                if call.get("is_cold_start") is not None
                else None
            ),
            "instance_id": call.get("instance_id"),
            "quota_class": call["quota_class"],
            "quota_bucket": call.get("quota_bucket"),
            "arrival_ms": call["arrival_ms"],
            "execution_us": call["execution_us"],
            "priority": call["priority"],
            "retry": bool(call["retry"]),
            "cold_start_latency_us": detail.get("start_elapsed_us"),
            "execution_elapsed_us": detail.get("elapsed_us"),
            "detail": detail,
            "deduplicated": deduplicated,
            "admitted_at_ms": call.get("admitted_at_ms"),
            "started_at_ms": call.get("started_at_ms"),
            "completed_at_ms": call.get("completed_at_ms"),
        }

    def _call_result_by_id(self, call_id: str, *, deduplicated: bool = False) -> dict[str, Any]:
        call = self._read_one("SELECT * FROM calls WHERE call_id = ?", (call_id,))
        if call is None:
            raise BusinessError(E_NOT_FOUND, "call not found", http_status=404)
        return self._format_call(call, deduplicated=deduplicated)

    def get_call(self, call_id: str) -> dict[str, Any]:
        return self._call_result_by_id(call_id)

    def list_functions(self) -> list[dict[str, Any]]:
        with self._read() as con:
            rows = con.execute(
                "SELECT s.*, r.active_instances, r.available_instances, r.cold_starts, "
                "r.warm_starts, r.start_timeouts, r.exec_timeouts, q.on_demand_used, "
                "q.reserved_used FROM function_specs s "
                "JOIN function_runtime r ON r.function_id = s.function_id "
                "JOIN quota_usage q ON q.function_id = s.function_id "
                "WHERE s.active = 1 ORDER BY s.function_id"
            ).fetchall()
            return [dict(row) for row in rows]

    def list_instances(self, function_id: str | None = None) -> list[dict[str, Any]]:
        with self._read() as con:
            if function_id:
                rows = con.execute(
                    "SELECT * FROM instances WHERE function_id = ? "
                    "ORDER BY created_at_ms, instance_id",
                    (function_id,),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM instances ORDER BY created_at_ms, instance_id"
                ).fetchall()
            return [dict(row) for row in rows]

    def quota(self, function_id: str | None = None) -> list[dict[str, Any]]:
        with self._read() as con:
            sql = (
                "SELECT q.function_id, q.spec_version, q.on_demand_used, q.reserved_used, "
                "s.on_demand_capacity, s.reserved_quota, s.function_concurrency_limit "
                "FROM quota_usage q JOIN (SELECT function_id, "
                "function_concurrency_limit - reserved_quota AS on_demand_capacity, "
                "reserved_quota, function_concurrency_limit FROM function_specs "
                "WHERE active = 1) s ON s.function_id = q.function_id "
            )
            params: tuple[Any, ...] = ()
            if function_id:
                sql += "WHERE q.function_id = ? "
                params = (function_id,)
            sql += "ORDER BY q.function_id"
            return [dict(row) for row in con.execute(sql, params).fetchall()]

    def metrics(self, function_id: str | None = None) -> dict[str, Any]:
        with self._read() as con:
            params: tuple[Any, ...] = ()
            where = ""
            if function_id:
                where = "WHERE function_id = ? "
                params = (function_id,)
            runtime = [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM function_runtime " + where + "ORDER BY function_id",
                    params,
                ).fetchall()
            ]
            call_counts = [
                dict(row)
                for row in con.execute(
                    "SELECT function_id, status, COUNT(*) AS n FROM calls "
                    + where
                    + "GROUP BY function_id, status",
                    params,
                ).fetchall()
            ]
            latencies = []
            for row in con.execute(
                "SELECT function_id, status, detail_json FROM calls "
                + where
                + "ORDER BY completed_at_ms",
                params,
            ).fetchall():
                detail = _loads(row["detail_json"], {}) or {}
                latencies.append(
                    {
                        "function_id": row["function_id"],
                        "status": row["status"],
                        "cold_start_latency_us": detail.get("start_elapsed_us"),
                        "execution_elapsed_us": detail.get("elapsed_us"),
                    }
                )
            commit_seq = int(
                con.execute(
                    "SELECT CAST(value AS INTEGER) AS seq FROM runtime_meta "
                    "WHERE key = 'commit_seq'"
                ).fetchone()["seq"]
            )
            return {
                "commit_seq": commit_seq,
                "runtime": runtime,
                "call_counts": call_counts,
                "latencies": latencies,
            }

    def _snapshot_with_connection(
        self, con: Any, *, function_id: str | None = None
    ) -> dict[str, Any]:
        params: tuple[Any, ...] = ()
        where = ""
        if function_id:
            where = "WHERE function_id = ? "
            params = (function_id,)
        specs = [
            dict(row)
            for row in con.execute(
                "SELECT * FROM function_specs WHERE active = 1 ORDER BY function_id"
            ).fetchall()
        ]
        instances = [
            dict(row)
            for row in con.execute(
                "SELECT * FROM instances " + where + "ORDER BY created_at_ms, instance_id",
                params,
            ).fetchall()
        ]
        calls = [
            dict(row)
            for row in con.execute(
                "SELECT * FROM calls " + where + "ORDER BY created_seq", params
            ).fetchall()
        ]
        quotas = [
            dict(row)
            for row in con.execute(
                "SELECT * FROM quota_usage " + where + "ORDER BY function_id", params
            ).fetchall()
        ]
        runtime = [
            dict(row)
            for row in con.execute(
                "SELECT * FROM function_runtime " + where + "ORDER BY function_id", params
            ).fetchall()
        ]
        commit_seq = int(
            con.execute(
                "SELECT CAST(value AS INTEGER) AS seq FROM runtime_meta WHERE key = 'commit_seq'"
            ).fetchone()["seq"]
        )
        return {
            "commit_seq": commit_seq,
            "functions": specs,
            "instances": instances,
            "calls": calls,
            "quota": quotas,
            "runtime": runtime,
        }

    def snapshot(self, function_id: str | None = None) -> dict[str, Any]:
        with self._read() as con:
            return self._snapshot_with_connection(con, function_id=function_id)

    def audit_invariants(self) -> dict[str, Any]:
        violations: list[dict[str, Any]] = []
        with self._read() as con:
            checks = {
                "active_call_without_instance": (
                    "SELECT c.call_id, c.status, c.instance_id FROM calls c "
                    "LEFT JOIN instances i ON i.instance_id = c.instance_id "
                    "WHERE c.status IN ('ADMITTED','CREATING','EXECUTING') "
                    "AND (i.instance_id IS NULL OR i.state = 'TERMINATED')"
                ),
                "active_call_without_quota_bucket": (
                    "SELECT call_id, status FROM calls WHERE status IN "
                    "('ADMITTED','CREATING','EXECUTING') AND quota_bucket IS NULL"
                ),
                "instance_slot_mismatch": (
                    "SELECT i.instance_id, i.state, i.occupied_slots, "
                    "COUNT(c.call_id) AS active_calls FROM instances i "
                    "LEFT JOIN calls c ON c.instance_id = i.instance_id "
                    "AND c.status IN ('ADMITTED','CREATING','EXECUTING') "
                    "WHERE i.state != 'TERMINATED' GROUP BY i.instance_id "
                    "HAVING i.occupied_slots != COUNT(c.call_id)"
                ),
                "terminated_with_occupied_slots": (
                    "SELECT instance_id, occupied_slots FROM instances "
                    "WHERE state = 'TERMINATED' AND occupied_slots != 0"
                ),
                "quota_exceeds_limit": (
                    "SELECT q.function_id, q.on_demand_used, q.reserved_used, "
                    "s.function_concurrency_limit - s.reserved_quota AS on_demand_capacity, "
                    "s.reserved_quota, s.function_concurrency_limit FROM quota_usage q "
                    "JOIN function_specs s ON s.function_id = q.function_id AND s.active = 1 "
                    "WHERE q.on_demand_used > s.function_concurrency_limit - s.reserved_quota "
                    "OR q.reserved_used > s.reserved_quota "
                    "OR q.on_demand_used + q.reserved_used > s.function_concurrency_limit"
                ),
                "quota_usage_mismatch": (
                    "SELECT q.function_id, q.on_demand_used, q.reserved_used, "
                    "COALESCE(SUM(CASE WHEN c.quota_bucket='ON_DEMAND' THEN 1 ELSE 0 END), 0) "
                    "AS expected_on_demand, COALESCE(SUM(CASE WHEN c.quota_bucket='RESERVED' "
                    "THEN 1 ELSE 0 END), 0) AS expected_reserved FROM quota_usage q "
                    "LEFT JOIN calls c ON c.function_id = q.function_id "
                    "AND c.status IN ('ADMITTED','CREATING','EXECUTING') "
                    "GROUP BY q.function_id HAVING q.on_demand_used != COALESCE(SUM(CASE WHEN "
                    "c.quota_bucket='ON_DEMAND' THEN 1 ELSE 0 END), 0) "
                    "OR q.reserved_used != COALESCE(SUM(CASE WHEN c.quota_bucket='RESERVED' "
                    "THEN 1 ELSE 0 END), 0)"
                ),
            }
            for name, sql in checks.items():
                rows = [dict(row) for row in con.execute(sql).fetchall()]
                if rows:
                    violations.append({"check": name, "rows": rows})
        return {"ok": not violations, "violations": violations, "checked_at": iso_now()}

    # ------------------------------------------------------------------
    # Explicit lifecycle operations
    # ------------------------------------------------------------------
    def transition_instance(
        self, instance_id: str, target_state: str, *, reason: str = "explicit"
    ) -> dict[str, Any]:
        instance = self._read_one(
            "SELECT * FROM instances WHERE instance_id = ?", (instance_id,)
        )
        if instance is None:
            raise BusinessError(E_NOT_FOUND, "instance not found", http_status=404)
        function_id = instance["function_id"]
        with self._condition(function_id):
            with self._write() as con:
                current = self._instance_row(con, instance_id)
                if current is None:
                    raise BusinessError(E_NOT_FOUND, "instance not found", http_status=404)
                assert_transition(
                    current["state"], target_state, int(current["occupied_slots"])
                )
                if target_state in {"READY", "BUSY"} and current["state"] == "CREATING":
                    raise BusinessError(
                        E_INVALID_TRANSITION,
                        "CREATING is completed only by the HTTP start acknowledgement",
                        detail={
                            "from": current["state"],
                            "to": target_state,
                            "allowed": sorted(TRANSITIONS[current["state"]]),
                        },
                    )
                timestamp = now_ms()
                con.execute(
                    "UPDATE instances SET state = ?, drain_reason = ?, updated_at_ms = ? "
                    "WHERE instance_id = ?",
                    (
                        target_state,
                        reason if target_state == "DRAINING" else None,
                        timestamp,
                        instance_id,
                    ),
                )
                if current["state"] == "READY" and target_state != "READY":
                    con.execute(
                        "UPDATE function_runtime SET available_instances = "
                        "MAX(0, available_instances - 1), updated_at_ms = ? "
                        "WHERE function_id = ?",
                        (timestamp, function_id),
                    )
                if target_state == "READY" and current["state"] != "READY":
                    con.execute(
                        "UPDATE function_runtime SET available_instances = "
                        "available_instances + 1, updated_at_ms = ? WHERE function_id = ?",
                        (timestamp, function_id),
                    )
                if target_state == "TERMINATED":
                    con.execute(
                        "UPDATE function_runtime SET active_instances = "
                        "MAX(0, active_instances - 1), updated_at_ms = ? WHERE function_id = ?",
                        (timestamp, function_id),
                    )
                    self._insert_intent(
                        con,
                        kind="RECLAIM_INSTANCE",
                        status="SUCCEEDED",
                        function_id=function_id,
                        call_id=None,
                        instance_id=instance_id,
                        payload={"trigger": reason, "from": current["state"], "to": target_state},
                    )
                commit_seq = bump_commit_seq(con)
            self._notify(function_id)
        return {
            "instance_id": instance_id,
            "from": instance["state"],
            "to": target_state,
            "commit_seq": commit_seq,
        }

    def drain_instance(self, instance_id: str, *, reason: str = "manual") -> dict[str, Any]:
        instance = self._read_one(
            "SELECT * FROM instances WHERE instance_id = ?", (instance_id,)
        )
        if instance is None:
            raise BusinessError(E_NOT_FOUND, "instance not found", http_status=404)
        if instance["state"] not in {"READY", "BUSY"}:
            raise BusinessError(
                E_INVALID_TRANSITION,
                "only READY or BUSY instances can enter DRAINING",
                detail={
                    "instance_id": instance_id,
                    "state": instance["state"],
                    "allowed": ["READY", "BUSY"],
                },
            )
        with self._condition(instance["function_id"]):
            with self._write() as con:
                current = self._instance_row(con, instance_id)
                if current is None:
                    raise BusinessError(E_NOT_FOUND, "instance not found", http_status=404)
                assert_transition(
                    current["state"], "DRAINING", int(current["occupied_slots"])
                )
                timestamp = now_ms()
                if current["state"] == "READY":
                    con.execute(
                        "UPDATE function_runtime SET available_instances = "
                        "MAX(0, available_instances - 1), updated_at_ms = ? WHERE function_id = ?",
                        (timestamp, current["function_id"]),
                    )
                if int(current["occupied_slots"]) == 0:
                    assert_transition("DRAINING", "TERMINATED", 0)
                    con.execute(
                        "UPDATE instances SET state = 'TERMINATED', drain_reason = ?, "
                        "updated_at_ms = ? WHERE instance_id = ?",
                        (reason, timestamp, instance_id),
                    )
                    con.execute(
                        "UPDATE function_runtime SET active_instances = "
                        "MAX(0, active_instances - 1), updated_at_ms = ? WHERE function_id = ?",
                        (timestamp, current["function_id"]),
                    )
                    target = "TERMINATED"
                else:
                    con.execute(
                        "UPDATE instances SET state = 'DRAINING', drain_reason = ?, "
                        "updated_at_ms = ? WHERE instance_id = ?",
                        (reason, timestamp, instance_id),
                    )
                    target = "DRAINING"
                self._insert_intent(
                    con,
                    kind="RECLAIM_INSTANCE",
                    status="SUCCEEDED",
                    function_id=current["function_id"],
                    call_id=None,
                    instance_id=instance_id,
                    payload={
                        "trigger": reason,
                        "from": current["state"],
                        "to": target,
                        "in_flight_at_drain": int(current["occupied_slots"]),
                    },
                )
                commit_seq = bump_commit_seq(con)
            self._notify(current["function_id"])
        return {
            "instance_id": instance_id,
            "from": instance["state"],
            "to": target,
            "in_flight_calls": int(instance["occupied_slots"]),
            "commit_seq": commit_seq,
        }

    # ------------------------------------------------------------------
    # Restart recovery from committed facts and pending intents
    # ------------------------------------------------------------------
    def recover_incomplete_work(self) -> dict[str, Any]:
        with self._write() as con:
            pending = [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM intents WHERE status = 'PENDING' "
                    "ORDER BY created_at_ms, intent_id"
                ).fetchall()
            ]
            active_calls = [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM calls WHERE status IN "
                    "('QUEUED','ADMITTED','CREATING','EXECUTING') ORDER BY created_seq"
                ).fetchall()
            ]
            before_quota = {
                row["function_id"]: dict(row)
                for row in con.execute("SELECT * FROM quota_usage").fetchall()
            }
            before_instances = [
                dict(row)
                for row in con.execute(
                    "SELECT instance_id, function_id, state, occupied_slots FROM instances "
                    "WHERE state IN ('CREATING','READY','BUSY','DRAINING') "
                    "ORDER BY instance_id"
                ).fetchall()
            ]
            affected_instances = {
                call["instance_id"] for call in active_calls if call.get("instance_id")
            }
            affected_instances.update(
                row["instance_id"]
                for row in before_instances
                if row["state"] in {"CREATING", "BUSY", "DRAINING"}
            )
            timestamp = now_ms()
            recovered_calls: list[dict[str, Any]] = []
            for call in active_calls:
                detail = {
                    "recovered_at_ms": timestamp,
                    "previous_status": call["status"],
                    "reason": (
                        "process ended before the call reached a terminal committed state"
                    ),
                }
                con.execute(
                    "UPDATE calls SET status = 'FAILED', business_code = ?, message = ?, "
                    "detail_json = ?, completed_at_ms = ?, updated_at_ms = ? WHERE call_id = ?",
                    (
                        E_RECOVERED_INTERRUPTED,
                        "restart recovery finalized an interrupted call using persisted facts only",
                        _json(detail),
                        timestamp,
                        timestamp,
                        call["call_id"],
                    ),
                )
                recovered_calls.append(
                    {
                        "call_id": call["call_id"],
                        "function_id": call["function_id"],
                        "previous_status": call["status"],
                        "previous_instance_id": call.get("instance_id"),
                        "previous_quota_bucket": call.get("quota_bucket"),
                    }
                )
            terminated_instances: list[dict[str, Any]] = []
            for instance in before_instances:
                if instance["instance_id"] not in affected_instances:
                    continue
                con.execute(
                    "UPDATE instances SET state = 'TERMINATED', occupied_slots = 0, "
                    "updated_at_ms = ? WHERE instance_id = ?",
                    (timestamp, instance["instance_id"]),
                )
                terminated_instances.append(
                    {
                        "instance_id": instance["instance_id"],
                        "function_id": instance["function_id"],
                        "previous_state": instance["state"],
                        "previous_occupied_slots": instance["occupied_slots"],
                    }
                )
            # Non-terminal calls are the only authoritative source for quota.
            con.execute(
                "UPDATE quota_usage SET on_demand_used = 0, reserved_used = 0, "
                "updated_at_ms = ?",
                (timestamp,),
            )
            for row in con.execute(
                "SELECT function_id, quota_bucket, COUNT(*) AS n FROM calls "
                "WHERE status IN ('ADMITTED','CREATING','EXECUTING') "
                "AND quota_bucket IS NOT NULL GROUP BY function_id, quota_bucket"
            ).fetchall():
                column = "on_demand_used" if row["quota_bucket"] == "ON_DEMAND" else "reserved_used"
                con.execute(
                    f"UPDATE quota_usage SET {column} = ? WHERE function_id = ?",
                    (row["n"], row["function_id"]),
                )
            con.execute(
                "UPDATE function_runtime SET active_instances = 0, available_instances = 0, "
                "updated_at_ms = ?",
                (timestamp,),
            )
            for row in con.execute(
                "SELECT function_id, "
                "SUM(CASE WHEN state IN ('CREATING','READY','BUSY','DRAINING') "
                "THEN 1 ELSE 0 END) AS active, "
                "SUM(CASE WHEN state = 'READY' THEN 1 ELSE 0 END) AS available "
                "FROM instances GROUP BY function_id"
            ).fetchall():
                con.execute(
                    "UPDATE function_runtime SET active_instances = ?, "
                    "available_instances = ?, updated_at_ms = ? WHERE function_id = ?",
                    (row["active"], row["available"], timestamp, row["function_id"]),
                )
            for intent in pending:
                self._update_intent(
                    con,
                    intent["intent_id"],
                    status="RESCUED",
                    error_code=E_RECOVERED_INTERRUPTED,
                    error={
                        "reason": "intent was PENDING when the previous process stopped",
                        "kind": intent["kind"],
                    },
                )
            after_quota = [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM quota_usage ORDER BY function_id"
                ).fetchall()
            ]
            after_runtime = [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM function_runtime ORDER BY function_id"
                ).fetchall()
            ]
            released = []
            for row in after_quota:
                before = before_quota.get(row["function_id"], {})
                released.append(
                    {
                        "function_id": row["function_id"],
                        "on_demand_released": int(before.get("on_demand_used", 0))
                        - int(row["on_demand_used"]),
                        "reserved_released": int(before.get("reserved_used", 0))
                        - int(row["reserved_used"]),
                        "on_demand_used_after": int(row["on_demand_used"]),
                        "reserved_used_after": int(row["reserved_used"]),
                    }
                )
            report = {
                "mode": "STARTUP_RECOVERY",
                "persisted_facts_only": True,
                "generated_at": iso_now(),
                "pending_intents_found": len(pending),
                "pending_intent_ids": [row["intent_id"] for row in pending],
                "rescued_intents": [
                    {
                        "intent_id": row["intent_id"],
                        "kind": row["kind"],
                        "instance_id": row["instance_id"],
                        "call_id": row["call_id"],
                    }
                    for row in pending
                ],
                "recovered_calls": recovered_calls,
                "terminated_instances": terminated_instances,
                "quota_released": released,
                "runtime_after_recovery": after_runtime,
                "exit_report_available": False,
                "exit_report_note": (
                    "No exit report can exist for a force-terminated process. This report "
                    "is generated after restart and contains only facts committed to SQLite "
                    "before the restart."
                ),
            }
            con.execute(
                "INSERT INTO recovery_reports(mode, persisted_facts_only, report_json, "
                "created_at_ms) VALUES ('STARTUP_RECOVERY', 1, ?, ?)",
                (_json(report), timestamp),
            )
            report["report_id"] = int(
                con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            )
            report["commit_seq"] = bump_commit_seq(con)
        return report

    def latest_recovery_report(self) -> dict[str, Any]:
        row = self._read_one(
            "SELECT * FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
        )
        if row is None:
            return {
                "mode": "STARTUP_RECOVERY",
                "report_id": None,
                "message": "no recovery report yet",
            }
        report = _loads(row["report_json"], {})
        report["report_id"] = row["report_id"]
        report["persisted_facts_only"] = bool(row["persisted_facts_only"])
        return report

    def list_intents(self, *, status: str | None = None) -> list[dict[str, Any]]:
        with self._read() as con:
            if status:
                rows = con.execute(
                    "SELECT * FROM intents WHERE status = ? "
                    "ORDER BY created_at_ms, intent_id",
                    (status,),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM intents ORDER BY created_at_ms, intent_id"
                ).fetchall()
            return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Reproducible anomaly injection and persisted recheck
    # ------------------------------------------------------------------
    def _injection_function(
        self, requested: str | None, *, require_reserved: bool = False
    ) -> FunctionSpec:
        with self._read() as con:
            if requested:
                return self._active_spec(con, requested)
            if require_reserved:
                row = con.execute(
                    "SELECT * FROM function_specs WHERE active = 1 AND reserved_quota > 0 "
                    "ORDER BY function_id LIMIT 1"
                ).fetchone()
                if row is None:
                    raise BusinessError(
                        E_SPEC_INVALID,
                        "QUOTA_BURST requires a function with reserved_quota > 0",
                    )
                return _spec_from_row(row)
            row = con.execute(
                "SELECT * FROM function_specs WHERE active = 1 ORDER BY function_id LIMIT 1"
            ).fetchone()
            if row is None:
                raise BusinessError(E_SPEC_INVALID, "no functions are loaded")
            return _spec_from_row(row)

    def _injection_snapshot(self, function_id: str) -> dict[str, Any]:
        with self._read() as con:
            runtime = self._runtime_row(con, function_id)
            quota = self._quota_row(con, function_id)
            instances = [
                dict(row)
                for row in con.execute(
                    "SELECT instance_id, state, occupied_slots, capacity FROM instances "
                    "WHERE function_id = ? ORDER BY created_at_ms, instance_id",
                    (function_id,),
                ).fetchall()
            ]
            calls = [
                dict(row)
                for row in con.execute(
                    "SELECT call_id, status, business_code, is_cold_start, instance_id, "
                    "quota_bucket FROM calls WHERE function_id = ? ORDER BY created_seq",
                    (function_id,),
                ).fetchall()
            ]
            trace_count = int(
                con.execute("SELECT COUNT(*) AS n FROM trace_batches").fetchone()["n"]
            )
            return {
                "runtime": runtime,
                "quota": quota,
                "instances": instances,
                "calls": calls,
                "trace_batch_count": trace_count,
            }

    def _drain_ready_instances(self, function_id: str) -> None:
        for instance in self.list_instances(function_id):
            if instance["state"] == "READY":
                self.drain_instance(instance["instance_id"], reason="injection-cold-start")

    def _record_injection(self, kind: str, params: dict[str, Any]) -> int:
        timestamp = now_ms()
        with self._write() as con:
            con.execute(
                "INSERT INTO injection_events(kind, params_json, status, created_at_ms, "
                "updated_at_ms) VALUES (?, ?, 'RUNNING', ?, ?)",
                (kind, _json(params), timestamp, timestamp),
            )
            injection_id = int(
                con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            )
            bump_commit_seq(con)
        return injection_id

    def _update_injection(
        self,
        injection_id: int,
        *,
        status: str,
        result_code: str,
        observation: dict[str, Any],
    ) -> None:
        with self._write() as con:
            con.execute(
                "UPDATE injection_events SET status = ?, result_code = ?, "
                "observation_json = ?, updated_at_ms = ? WHERE injection_id = ?",
                (status, result_code, _json(observation), now_ms(), injection_id),
            )
            bump_commit_seq(con)

    def run_injection(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise BusinessError(E_SPEC_INVALID, "injection payload must be an object")
        kind = payload.get("kind")
        if kind not in INJECTION_KINDS:
            raise BusinessError(
                E_SPEC_INVALID,
                "unknown injection kind",
                detail={"kind": kind, "allowed": sorted(INJECTION_KINDS)},
            )
        params = payload.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise BusinessError(E_SPEC_INVALID, "injection params must be an object")
        requested_function = payload.get("function_id") or params.get("function_id")
        requested_call = payload.get("call_id") or params.get("call_id") or _new_id("inj-call")
        base_call = requested_call
        replay_derived = False
        if (
            self._read_one("SELECT 1 AS found FROM calls WHERE call_id = ? LIMIT 1", (base_call,))
            or self._read_one(
                "SELECT 1 AS found FROM calls WHERE instr(call_id, ?) = 1 LIMIT 1",
                (base_call,),
            )
        ):
            base_call = f"{base_call}-replay-{uuid.uuid4().hex[:10]}"
            replay_derived = True
        event_params = {
            **params,
            "function_id": requested_function,
            "call_id": base_call,
        }
        if replay_derived:
            event_params["requested_call_id"] = requested_call
        injection_id = self._record_injection(kind, event_params)
        before: dict[str, Any] | None = None
        result_code = E_INTERNAL
        extra: dict[str, Any] = {}
        function_id: str | None = None
        try:
            if kind == "INSTANCE_CREATE_TIMEOUT":
                spec = self._injection_function(requested_function)
                function_id = spec.function_id
                # Readiness from an earlier scenario would hide the forced cold start.
                self._drain_ready_instances(function_id)
                before = self._injection_snapshot(function_id)
                result = self.invoke(
                    {
                        "call_id": base_call,
                        "function_id": function_id,
                        "execution_us": int(params.get("execution_us", 0)),
                        "priority": int(params.get("priority", 0)),
                        "retry": bool(params.get("retry", False)),
                        "quota_class": params.get("quota_class", "AUTO"),
                        "injection": {"create_timeout": True},
                    }
                )
                result_code = result["business_code"]
                extra = {"call": result, "expected_code": E_START_TIMEOUT}
            elif kind == "EXECUTION_INTERRUPT":
                spec = self._injection_function(requested_function)
                function_id = spec.function_id
                self._drain_ready_instances(function_id)
                before = self._injection_snapshot(function_id)
                result = self.invoke(
                    {
                        "call_id": base_call,
                        "function_id": function_id,
                        "execution_us": int(params.get("execution_us", 100_000)),
                        "priority": int(params.get("priority", 0)),
                        "retry": bool(params.get("retry", False)),
                        "quota_class": params.get("quota_class", "AUTO"),
                        "injection": {"execution_interrupt": True},
                    }
                )
                result_code = result["business_code"]
                extra = {"call": result, "expected_code": E_EXEC_TIMEOUT}
            elif kind == "DUPLICATE_CALL_ID":
                spec = self._injection_function(requested_function)
                function_id = spec.function_id
                self._drain_ready_instances(function_id)
                before = self._injection_snapshot(function_id)
                execution_us = int(params.get("execution_us", 20_000))
                first = self.invoke(
                    {
                        "call_id": base_call,
                        "function_id": function_id,
                        "execution_us": execution_us,
                        "priority": int(params.get("priority", 0)),
                        "retry": bool(params.get("retry", False)),
                    }
                )
                try:
                    self.invoke(
                        {
                            "call_id": base_call,
                            "function_id": function_id,
                            "execution_us": execution_us + 1,
                            "priority": int(params.get("priority", 0)),
                            "retry": bool(params.get("retry", False)),
                        }
                    )
                except BusinessError as exc:
                    result_code = exc.code
                    extra = {
                        "first_call": first,
                        "conflict_code": exc.code,
                        "conflict_detail": exc.detail,
                        "expected_code": E_CONFLICTING_CALL,
                    }
                else:
                    result_code = "UNEXPECTED_SUCCESS"
            elif kind == "QUOTA_BURST":
                spec = self._injection_function(requested_function, require_reserved=True)
                function_id = spec.function_id
                self._drain_ready_instances(function_id)
                before = self._injection_snapshot(function_id)
                execution_us = int(params.get("execution_us", 350_000))
                burst = spec.reserved_quota + 1
                call_ids = [f"{base_call}-{index}" for index in range(burst)]

                def reserved_call(call_id: str) -> dict[str, Any]:
                    try:
                        return self.invoke(
                            {
                                "call_id": call_id,
                                "function_id": function_id,
                                "execution_us": execution_us,
                                "quota_class": "RESERVED",
                            }
                        )
                    except BusinessError as exc:
                        return {
                            "ok": False,
                            "business_code": exc.code,
                            "detail": exc.detail,
                        }

                with ThreadPoolExecutor(
                    max_workers=burst, thread_name_prefix="quota-burst"
                ) as pool:
                    futures = [pool.submit(reserved_call, call_id) for call_id in call_ids]
                    burst_results = [future.result() for future in as_completed(futures)]
                denied = [
                    item for item in burst_results if item["business_code"] == E_QUOTA_DENIED
                ]
                result_code = E_QUOTA_DENIED if denied else "UNEXPECTED_SUCCESS"
                extra = {
                    "burst_size": burst,
                    "reserved_quota": spec.reserved_quota,
                    "denied_count": len(denied),
                    "burst_results": burst_results,
                    "expected_code": E_QUOTA_DENIED,
                }
            elif kind == "TRACE_OUT_OF_ORDER":
                spec = self._injection_function(requested_function)
                function_id = spec.function_id
                before = self._injection_snapshot(function_id)
                arrival = int(params.get("arrival_ms", 1000))
                trace_payload = {
                    "batch_id": _new_id("reordered-trace"),
                    "calls": [
                        {
                            "call_id": f"{base_call}-a",
                            "function_id": function_id,
                            "arrival_ms": arrival,
                            "execution_us": 10_000,
                            "priority": 1,
                            "retry": False,
                        },
                        {
                            "call_id": f"{base_call}-b",
                            "function_id": function_id,
                            "arrival_ms": arrival - 1,
                            "execution_us": 10_000,
                            "priority": 1,
                            "retry": False,
                        },
                    ],
                }
                try:
                    self.load_trace(trace_payload, source="injection")
                except BusinessError as exc:
                    result_code = exc.code
                    extra = {
                        "trace_error": exc.detail,
                        "expected_code": E_TRACE_OUT_OF_ORDER,
                    }
                else:
                    result_code = "UNEXPECTED_SUCCESS"
            if function_id is None:
                raises = self._injection_function(None)
                function_id = raises.function_id
            after = self._injection_snapshot(function_id)
            observation = {
                "kind": kind,
                "params": event_params,
                "before": before,
                "after": after,
                "result_code": result_code,
                **extra,
            }
            self._update_injection(
                injection_id,
                status="COMPLETED",
                result_code=result_code,
                observation=observation,
            )
            return {"injection_id": injection_id, **observation}
        except BusinessError as exc:
            observation = {
                "kind": kind,
                "params": event_params,
                "before": before,
                "result_code": exc.code,
                "error": {"message": exc.message, "detail": exc.detail},
            }
            self._update_injection(
                injection_id,
                status="ERROR",
                result_code=exc.code,
                observation=observation,
            )
            raise
        except Exception as exc:
            observation = {
                "kind": kind,
                "params": event_params,
                "before": before,
                "result_code": E_INTERNAL,
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
            self._update_injection(
                injection_id,
                status="ERROR",
                result_code=E_INTERNAL,
                observation=observation,
            )
            raise BusinessError(
                E_INTERNAL, "injection scenario failed", detail=observation
            )

    def get_injection(self, injection_id: int) -> dict[str, Any]:
        row = self._read_one(
            "SELECT * FROM injection_events WHERE injection_id = ?", (injection_id,)
        )
        if row is None:
            raise BusinessError(E_NOT_FOUND, "injection event not found", http_status=404)
        observation = _loads(row["observation_json"], {}) or {}
        return {
            "injection_id": row["injection_id"],
            "kind": row["kind"],
            "params": _loads(row["params_json"], {}),
            "status": row["status"],
            "result_code": row["result_code"],
            "observation": observation,
            "created_at_ms": row["created_at_ms"],
        }

    def list_injections(self) -> list[dict[str, Any]]:
        with self._read() as con:
            rows = con.execute(
                "SELECT injection_id, kind, params_json, status, result_code, created_at_ms "
                "FROM injection_events ORDER BY injection_id"
            ).fetchall()
            return [
                {
                    "injection_id": row["injection_id"],
                    "kind": row["kind"],
                    "params": _loads(row["params_json"], {}),
                    "status": row["status"],
                    "result_code": row["result_code"],
                    "created_at_ms": row["created_at_ms"],
                }
                for row in rows
            ]

    def recheck_injection(self, injection_id: int) -> dict[str, Any]:
        event = self.get_injection(injection_id)
        observation = event["observation"]
        kind = event["kind"]
        before = observation.get("before") or {}
        after = observation.get("after") or {}
        checks: list[dict[str, Any]] = []

        def add(name: str, expected: Any, actual: Any) -> None:
            checks.append(
                {"name": name, "expected": expected, "actual": actual, "passed": expected == actual}
            )

        if kind == "INSTANCE_CREATE_TIMEOUT":
            add("result_code", E_START_TIMEOUT, event["result_code"])
            add(
                "cold_starts_delta",
                1,
                int(after.get("runtime", {}).get("cold_starts", 0))
                - int(before.get("runtime", {}).get("cold_starts", 0)),
            )
            add(
                "start_timeouts_delta",
                1,
                int(after.get("runtime", {}).get("start_timeouts", 0))
                - int(before.get("runtime", {}).get("start_timeouts", 0)),
            )
            add(
                "quota_after_attempt",
                {"on_demand_used": 0, "reserved_used": 0},
                {
                    "on_demand_used": int(after.get("quota", {}).get("on_demand_used", -1)),
                    "reserved_used": int(after.get("quota", {}).get("reserved_used", -1)),
                },
            )
            call_instance = observation.get("call", {}).get("instance_id")
            add(
                "affected_instance_terminated",
                True,
                all(
                    item["state"] == "TERMINATED"
                    for item in after.get("instances", [])
                    if item["instance_id"] == call_instance
                ),
            )
        elif kind == "EXECUTION_INTERRUPT":
            add("result_code", E_EXEC_TIMEOUT, event["result_code"])
            add(
                "cold_starts_delta",
                1 if observation.get("call", {}).get("cold_start") else 0,
                int(after.get("runtime", {}).get("cold_starts", 0))
                - int(before.get("runtime", {}).get("cold_starts", 0)),
            )
            add(
                "exec_timeouts_delta",
                1,
                int(after.get("runtime", {}).get("exec_timeouts", 0))
                - int(before.get("runtime", {}).get("exec_timeouts", 0)),
            )
            add(
                "quota_after_attempt",
                {"on_demand_used": 0, "reserved_used": 0},
                {
                    "on_demand_used": int(after.get("quota", {}).get("on_demand_used", -1)),
                    "reserved_used": int(after.get("quota", {}).get("reserved_used", -1)),
                },
            )
            add("call_status", "FAILED", observation.get("call", {}).get("status"))
        elif kind == "DUPLICATE_CALL_ID":
            add("result_code", E_CONFLICTING_CALL, event["result_code"])
            first_call = observation.get("first_call", {})
            add(
                "cold_starts_delta",
                (
                    0
                    if first_call.get("deduplicated")
                    else (1 if first_call.get("cold_start") else 0)
                ),
                int(after.get("runtime", {}).get("cold_starts", 0))
                - int(before.get("runtime", {}).get("cold_starts", 0)),
            )
            call_id = observation.get("params", {}).get("call_id")
            add(
                "duplicate_call_rows",
                1,
                sum(1 for call in after.get("calls", []) if call["call_id"] == call_id),
            )
            add(
                "quota_after_attempt",
                {"on_demand_used": 0, "reserved_used": 0},
                {
                    "on_demand_used": int(after.get("quota", {}).get("on_demand_used", -1)),
                    "reserved_used": int(after.get("quota", {}).get("reserved_used", -1)),
                },
            )
        elif kind == "QUOTA_BURST":
            add("result_code", E_QUOTA_DENIED, event["result_code"])
            add(
                "cold_starts_delta",
                sum(
                    1
                    for item in observation.get("burst_results", [])
                    if item.get("cold_start")
                ),
                int(after.get("runtime", {}).get("cold_starts", 0))
                - int(before.get("runtime", {}).get("cold_starts", 0)),
            )
            add(
                "denied_count",
                int(observation.get("burst_size", 0))
                - int(observation.get("reserved_quota", 0)),
                int(observation.get("denied_count", 0)),
            )
            add(
                "reserved_quota_returned",
                {"on_demand_used": 0, "reserved_used": 0},
                {
                    "on_demand_used": int(after.get("quota", {}).get("on_demand_used", -1)),
                    "reserved_used": int(after.get("quota", {}).get("reserved_used", -1)),
                },
            )
        elif kind == "TRACE_OUT_OF_ORDER":
            add("result_code", E_TRACE_OUT_OF_ORDER, event["result_code"])
            add(
                "cold_starts_delta",
                0,
                int(after.get("runtime", {}).get("cold_starts", 0))
                - int(before.get("runtime", {}).get("cold_starts", 0)),
            )
            add(
                "trace_batches_unchanged",
                int(before.get("trace_batch_count", -1)),
                int(after.get("trace_batch_count", -2)),
            )
        consistent = bool(checks) and all(check["passed"] for check in checks)
        return {
            "injection_id": injection_id,
            "kind": kind,
            "params": event["params"],
            "result_code": event["result_code"],
            "checks": checks,
            "consistent": consistent,
        }





