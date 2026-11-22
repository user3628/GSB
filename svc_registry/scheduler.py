from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

from .db import Database, InstanceRow, canonical_json, now_ms
from .errors import E_PROBE_QUEUE_FULL, E_ROUND_BUDGET_EXCEEDED, BusinessError
from .faults import maybe_crash
from .probes import ProbeExecution, execute_probe


@dataclass
class _Job:
    round_id: str
    row: InstanceRow
    attempt_id: int
    future: asyncio.Future[ProbeExecution]
    task: asyncio.Task[ProbeExecution] | None = None


class ProbeScheduler:
    def __init__(
        self,
        database: Database,
        *,
        max_concurrency: int = 4,
        queue_size: int = 8,
        failure_weight_step_permille: int = 50,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        self.db = database
        self.max_concurrency = max_concurrency
        self.queue_size = queue_size
        self.failure_weight_step_permille = max(1, failure_weight_step_permille)
        self._waiting: list[_Job] = []
        self._active_tasks: set[asyncio.Task[Any]] = set()
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True

    async def stop(self) -> None:
        self._started = False
        for job in self._waiting:
            if not job.future.done():
                job.future.cancel()
        self._waiting.clear()
        tasks = list(self._active_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._active_tasks.clear()

    def _launch(self, job: _Job) -> None:
        task = asyncio.create_task(self._run_job(job), name="probe-job")
        job.task = task
        self._active_tasks.add(task)

    async def _run_job(self, job: _Job) -> None:
        try:
            if job.future.cancelled():
                return
            try:
                result = await execute_probe(job.row)
            except asyncio.CancelledError:
                if not job.future.done():
                    job.future.cancel()
                raise
            if not job.future.done():
                job.future.set_result(result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive task boundary
            if not job.future.done():
                job.future.set_result(
                    ProbeExecution(
                        success=False,
                        status="DOWN",
                        outcome="scheduler_worker_error",
                        latency_ms=0,
                        error_code="E_PROBE_FAILED",
                        detail={"error": type(exc).__name__, "message": str(exc)},
                    )
                )
        finally:
            current = asyncio.current_task()
            if current is not None:
                self._active_tasks.discard(current)
            self._dispatch_waiting()

    def _dispatch_waiting(self) -> None:
        while self._started and self._waiting and len(self._active_tasks) < self.max_concurrency:
            job = self._waiting.pop(0)
            if job.future.done():
                continue
            self._launch(job)

    def _submit(self, job: _Job) -> bool:
        if len(self._active_tasks) < self.max_concurrency:
            self._launch(job)
            return True
        if len(self._waiting) < self.queue_size:
            self._waiting.append(job)
            return True
        return False

    def _create_round(
        self,
        rows: list[InstanceRow],
        *,
        budget_ms: int,
    ) -> tuple[str, int, dict[tuple[str, str], int]]:
        round_id = uuid.uuid4().hex
        intent_id = self.db.start_intent(
            "PROBE_ROUND",
            round_id,
            {
                "round_id": round_id,
                "instance_count": len(rows),
                "budget_ms": budget_ms,
                "concurrency": self.max_concurrency,
                "queue_size": self.queue_size,
            },
        )
        created_at = now_ms()
        attempt_ids: dict[tuple[str, str], int] = {}
        with self.db.transaction() as connection:
            connection.execute(
                """
                INSERT INTO probe_rounds (
                    round_id, status, budget_ms, concurrency, queue_size,
                    intent_id, created_at_ms
                ) VALUES (?, 'PROBING', ?, ?, ?, ?, ?)
                """,
                (round_id, budget_ms, self.max_concurrency, self.queue_size, intent_id, created_at),
            )
            for row in rows:
                cursor = connection.execute(
                    """
                    INSERT INTO probe_attempts (
                        round_id, service_name, instance_id, probe_method, state, started_at_ms
                    ) VALUES (?, ?, ?, ?, 'PENDING', ?)
                    """,
                    (round_id, row.service_name, row.instance_id, row.probe_method, created_at),
                )
                attempt_ids[(row.service_name, row.instance_id)] = int(cursor.lastrowid)
        return round_id, intent_id, attempt_ids

    async def run_round(
        self,
        instance_keys: list[tuple[str, str]] | None = None,
        *,
        budget_ms: int = 5000,
    ) -> dict[str, Any]:
        if budget_ms < 1:
            raise BusinessError("E_VALIDATION", "budget_ms must be positive", details={"budget_ms": budget_ms})
        self.start()
        if instance_keys is None:
            rows = self.db.list_instances()
        else:
            rows = []
            seen: set[tuple[str, str]] = set()
            for service_name, instance_id in instance_keys:
                key = (service_name, instance_id)
                if key in seen:
                    continue
                seen.add(key)
                row = self.db.get_instance(service_name, instance_id)
                if row is not None:
                    rows.append(row)
        if not rows:
            return {
                "ok": True,
                "round_id": None,
                "status": "empty",
                "results": [],
                "counts": {"requested": 0, "applied": 0, "skipped": 0, "errors": 0},
            }

        round_id, intent_id, attempt_ids = self._create_round(rows, budget_ms=budget_ms)
        maybe_crash("probe_after_attempts")
        jobs: list[_Job] = []
        rejected: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        for row in rows:
            future: asyncio.Future[ProbeExecution] = loop.create_future()
            job = _Job(
                round_id=round_id,
                row=row,
                attempt_id=attempt_ids[(row.service_name, row.instance_id)],
                future=future,
            )
            if self._submit(job):
                jobs.append(job)
            else:
                future.cancel()
                rejected.append(
                    {
                        "service_name": row.service_name,
                        "instance_id": row.instance_id,
                        "attempt_id": job.attempt_id,
                        "error_code": E_PROBE_QUEUE_FULL,
                        "message": "bounded probe queue is full",
                    }
                )

        futures = [job.future for job in jobs]
        done, pending = await asyncio.wait(futures, timeout=budget_ms / 1000)
        budget_results: dict[tuple[str, str], ProbeExecution] = {}
        for job in jobs:
            if job.future in pending:
                job.future.cancel()
                if job.task is not None:
                    job.task.cancel()
                elif job in self._waiting:
                    self._waiting.remove(job)
                budget_results[(job.row.service_name, job.row.instance_id)] = ProbeExecution(
                    success=False,
                    status="DOWN",
                    outcome="round_budget_exceeded",
                    latency_ms=budget_ms,
                    error_code=E_ROUND_BUDGET_EXCEEDED,
                    detail={"budget_ms": budget_ms, "absolute_close": True},
                )

        results_by_key: dict[tuple[str, str], ProbeExecution] = {}
        for job in jobs:
            key = (job.row.service_name, job.row.instance_id)
            if key in budget_results:
                results_by_key[key] = budget_results[key]
            elif job.future.done() and not job.future.cancelled():
                results_by_key[key] = job.future.result()

        completed_at = now_ms()
        report_results: list[dict[str, Any]] = []
        applied = 0
        skipped = 0
        errors = 0
        with self.db.transaction() as connection:
            for row in rows:
                key = (row.service_name, row.instance_id)
                attempt_id = attempt_ids[key]
                if key not in results_by_key:
                    errors += 1
                    for item in rejected:
                        if item["service_name"] == row.service_name and item["instance_id"] == row.instance_id:
                            connection.execute(
                                """
                                UPDATE probe_attempts SET state = 'ROLLED_BACK', completed_at_ms = ?,
                                    result_json = ? WHERE id = ?
                                """,
                                (completed_at, canonical_json(item), attempt_id),
                            )
                            report_results.append({**item, "applied": False})
                    continue
                execution = results_by_key[key]
                current = self.db.get_instance(row.service_name, row.instance_id, connection=connection)
                if current is None:
                    skipped += 1
                    outcome = {
                        "service_name": row.service_name,
                        "instance_id": row.instance_id,
                        "status": "SKIPPED",
                        "outcome": "instance_disappeared",
                        "applied": False,
                    }
                    connection.execute(
                        "UPDATE probe_attempts SET state = 'ROLLED_BACK', completed_at_ms = ?, result_json = ? WHERE id = ?",
                        (completed_at, canonical_json(outcome), attempt_id),
                    )
                    report_results.append(outcome)
                    continue
                if current.lease_version != row.lease_version or current.record_version != row.record_version:
                    skipped += 1
                    outcome = {
                        "service_name": row.service_name,
                        "instance_id": row.instance_id,
                        "status": "SKIPPED",
                        "outcome": "instance_changed_during_probe",
                        "observed_lease_version": row.lease_version,
                        "current_lease_version": current.lease_version,
                        "applied": False,
                    }
                    connection.execute(
                        "UPDATE probe_attempts SET state = 'ROLLED_BACK', completed_at_ms = ?, result_json = ? WHERE id = ?",
                        (completed_at, canonical_json(outcome), attempt_id),
                    )
                    report_results.append(outcome)
                    continue
                lease_expired = current.lease_expires_at_ms <= completed_at
                weight_before = current.weight_permille
                if lease_expired:
                    health_status = "EXPIRED"
                    health_reason = "lease expired during probe round"
                    weight_after = weight_before
                    final_success = False
                    final_status = "EXPIRED"
                    final_outcome = "lease_expired_during_round"
                    final_error = None
                elif execution.success:
                    health_status = "UP"
                    health_reason = f"probe {execution.outcome}"
                    weight_after = weight_before
                    final_success = True
                    final_status = "UP"
                    final_outcome = execution.outcome
                    final_error = execution.error_code
                else:
                    health_status = "DOWN"
                    health_reason = f"probe {execution.outcome}"
                    weight_after = max(1, weight_before - self.failure_weight_step_permille)
                    final_success = False
                    final_status = execution.status if execution.status != "EXPIRED" else "DOWN"
                    final_outcome = execution.outcome
                    final_error = execution.error_code
                new_record_version = current.record_version + 1
                history_detail = {
                    "endpoint": row.endpoint,
                    "probe_params": row.probe_params,
                    "probe_window_ms": row.probe_window_ms,
                    **(execution.detail or {}),
                }
                connection.execute(
                    """
                    UPDATE instances SET weight_permille = ?, health_status = ?, health_reason = ?,
                        record_version = ?, updated_at_ms = ?
                    WHERE service_name = ? AND instance_id = ? AND record_version = ?
                    """,
                    (
                        weight_after,
                        health_status,
                        health_reason,
                        new_record_version,
                        completed_at,
                        row.service_name,
                        row.instance_id,
                        current.record_version,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO probe_history (
                        round_id, service_name, instance_id, probe_method, checked_at_ms,
                        success, status, outcome, latency_ms, error_code, detail_json,
                        weight_before, weight_after, lease_version, health_status,
                        record_version, created_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        round_id,
                        row.service_name,
                        row.instance_id,
                        row.probe_method,
                        completed_at,
                        1 if final_success else 0,
                        final_status,
                        final_outcome,
                        execution.latency_ms,
                        final_error,
                        canonical_json(history_detail),
                        weight_before,
                        weight_after,
                        current.lease_version,
                        health_status,
                        new_record_version,
                        completed_at,
                    ),
                )
                result_payload = {
                    "service_name": row.service_name,
                    "instance_id": row.instance_id,
                    "probe_method": row.probe_method,
                    "success": final_success,
                    "status": final_status,
                    "outcome": final_outcome,
                    "latency_ms": execution.latency_ms,
                    "error_code": final_error,
                    "detail": history_detail,
                    "weight_before": weight_before,
                    "weight_after": weight_after,
                    "health_status": health_status,
                    "record_version": new_record_version,
                    "applied": True,
                }
                connection.execute(
                    """
                    UPDATE probe_attempts SET state = 'COMPLETED', completed_at_ms = ?, result_json = ?
                    WHERE id = ?
                    """,
                    (completed_at, canonical_json(result_payload), attempt_id),
                )
                report_results.append(result_payload)
                applied += 1

            report = {
                "ok": True,
                "round_id": round_id,
                "status": "committed",
                "budget_ms": budget_ms,
                "concurrency": self.max_concurrency,
                "queue_size": self.queue_size,
                "results": report_results,
                "counts": {
                    "requested": len(rows),
                    "applied": applied,
                    "skipped": skipped,
                    "errors": errors,
                },
            }
            maybe_crash("probe_before_commit")
            connection.execute(
                """
                UPDATE probe_rounds SET status = 'COMMITTED', report_json = ?, completed_at_ms = ?
                WHERE round_id = ?
                """,
                (canonical_json(report), completed_at, round_id),
            )
            self.db.finish_intent(
                connection,
                intent_id,
                "COMMITTED",
                {"round_id": round_id, "counts": report["counts"]},
            )
        return report

    def history(
        self,
        *,
        service_name: str | None = None,
        instance_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if service_name is not None:
            clauses.append("service_name = ?")
            params.append(service_name)
        if instance_id is not None:
            clauses.append("instance_id = ?")
            params.append(instance_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(limit, 1000)))
        with self.db.read() as connection:
            rows = connection.execute(
                f"SELECT * FROM probe_history {where} ORDER BY id DESC LIMIT ?",
                tuple(params),
            ).fetchall()
            results = []
            for row in rows:
                item = dict(row)
                item["success"] = bool(item["success"])
                item["detail"] = __import__("json").loads(item.pop("detail_json"))
                results.append(item)
            return results
