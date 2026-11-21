from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from app.db import Database
from app.errors import BusinessError, STATUS_BY_CODE, business_error
from app.manifest import canonical_object_digest, manifest_from_parts
from app.models import GatewaySettings, ScheduleRoundRequest, utc_now


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass
class Candidate:
    session_id: str
    seq: int
    backend_id: str
    size_bytes: int
    sha256: str
    object_key: str
    priority: int
    session_created_at: str
    estimated_cost_ms: int
    order_index: int


class AdmissionGate:
    """Bounds the sum of active and waiting tasks across concurrent rounds."""

    def __init__(self, global_concurrency: int, queue_limit: int) -> None:
        self.capacity = global_concurrency + queue_limit
        self._used = 0
        self._lock = asyncio.Lock()
        self.peak_used = 0

    async def reserve(self, count: int) -> bool:
        async with self._lock:
            if self._used + count > self.capacity:
                return False
            self._used += count
            self.peak_used = max(self.peak_used, self._used)
            return True

    async def release(self, count: int) -> None:
        async with self._lock:
            self._used = max(0, self._used - count)

    @property
    def used(self) -> int:
        return self._used


class Scheduler:
    def __init__(self, database: Database, settings: GatewaySettings) -> None:
        self.database = database
        self.settings = settings
        self.global_semaphore = asyncio.Semaphore(settings.global_concurrency)
        self.backend_semaphores = {
            backend.id: asyncio.Semaphore(backend.max_concurrency)
            for backend in settings.backends
        }
        self.admission = AdmissionGate(settings.global_concurrency, settings.queue_limit)
        self._round_lock = asyncio.Lock()
        self._active_count = 0
        self._queued_count = 0
        self._peak_active = 0
        self._peak_queued = 0

    async def _http_json(
        self,
        method: str,
        url: str,
        timeout_ms: int,
        *,
        stage: str,
        seq: int,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any] | None]:
        # One short-lived client per request gives write and read independent
        # deadlines and guarantees the socket pool is closed on every path.
        timeout = httpx.Timeout(timeout_ms / 1000.0)
        client = httpx.AsyncClient(timeout=timeout)
        try:
            response = await client.request(method, url, json=json_body, params=params)
            try:
                body = response.json()
            except ValueError:
                body = None
            return response.status_code, body
        except httpx.TimeoutException as error:
            raise business_error(
                "E_BACKEND_TIMEOUT",
                "backend request exceeded its independent timeout and the client was closed",
                stage=stage,
                seq=seq,
                timeout_ms=timeout_ms,
            ) from error
        except httpx.ConnectError as error:
            raise business_error(
                "E_BACKEND_UNAVAILABLE",
                "backend connection was refused or unreachable",
                stage=stage,
                seq=seq,
                error_type=type(error).__name__,
            ) from error
        except httpx.HTTPError as error:
            raise business_error(
                "E_BACKEND_UNAVAILABLE",
                "backend transport failed",
                stage=stage,
                seq=seq,
                error_type=type(error).__name__,
            ) from error
        finally:
            await client.aclose()

    async def _write_and_verify(
        self,
        candidate: Candidate,
        backend_url: str,
        write_timeout_ms: int,
        read_timeout_ms: int,
    ) -> dict[str, Any]:
        write_url = f"{backend_url.rstrip('/')}/v1/backend/write"
        write_status, write_body = await self._http_json(
            "POST",
            write_url,
            write_timeout_ms,
            stage="WRITE",
            seq=candidate.seq,
            json_body={
                "session_id": candidate.session_id,
                "object_key": candidate.object_key,
                "seq": candidate.seq,
                "size_bytes": candidate.size_bytes,
                "sha256": candidate.sha256,
            },
        )
        if write_status == 409:
            raise business_error(
                "E_QUOTA_EXCEEDED",
                "backend rejected the write because of its own capacity budget",
                stage="WRITE",
                seq=candidate.seq,
                backend_status=write_status,
                backend_body=write_body,
            )
        if write_status == 429:
            raise business_error(
                "E_BACKEND_BUSY",
                "backend concurrent-write limit rejected the request",
                stage="WRITE",
                seq=candidate.seq,
            )
        if write_status < 200 or write_status >= 300:
            raise business_error(
                "E_BACKEND_UNAVAILABLE",
                "backend write failed",
                stage="WRITE",
                seq=candidate.seq,
                backend_status=write_status,
                backend_body=write_body,
            )
        read_url = (
            f"{backend_url.rstrip('/')}/v1/backend/read/"
            f"{candidate.session_id}/{candidate.seq}"
        )
        read_status, read_body = await self._http_json(
            "GET",
            read_url,
            read_timeout_ms,
            stage="READBACK",
            seq=candidate.seq,
            params={"object_key": candidate.object_key},
        )
        if read_status < 200 or read_status >= 300 or read_body is None:
            raise business_error(
                "E_BACKEND_UNAVAILABLE",
                "backend readback failed",
                stage="READBACK",
                seq=candidate.seq,
                backend_status=read_status,
            )
        readback_sha = str(read_body.get("sha256", "")).lower()
        if readback_sha != candidate.sha256:
            raise business_error(
                "E_READBACK_MISMATCH",
                "readback digest does not match the submitted digest",
                seq=candidate.seq,
                session_id=candidate.session_id,
                expected_sha256=candidate.sha256,
                readback_sha256=readback_sha,
            )
        return {
            "write_status": write_status,
            "read_status": read_status,
            "readback_sha256": readback_sha,
            "storage_token": (write_body or {}).get("storage_token"),
        }

    async def run_round(self, request: ScheduleRoundRequest) -> dict[str, Any]:
        async with self._round_lock:
            return await self._run_round_locked(request)

    async def _load_candidates(self, session_ids: list[str] | None) -> list[Candidate]:
        where = [
            "s.status='ACTIVE'",
            "p.status IN ('PENDING', 'DEFERRED')",
            "b.enabled=1",
        ]
        params: list[Any] = []
        if session_ids:
            placeholders = ",".join("?" for _ in session_ids)
            where.append(f"s.id IN ({placeholders})")
            params.extend(session_ids)
        sql = f"""
            SELECT p.session_id, p.seq, p.size_bytes, p.sha256, s.object_key,
                   s.priority, s.created_at AS session_created_at,
                   s.backend_id, b.estimated_latency_ms, b.url
            FROM parts p
            JOIN sessions s ON s.id=p.session_id
            JOIN backends b ON b.id=s.backend_id
            WHERE {' AND '.join(where)}
            ORDER BY s.priority ASC, s.created_at ASC, s.id ASC, p.seq ASC
        """
        with self.database.transaction(immediate=False) as connection:
            rows = connection.execute(sql, params).fetchall()
        return [
            Candidate(
                session_id=row["session_id"],
                seq=int(row["seq"]),
                backend_id=row["backend_id"],
                size_bytes=int(row["size_bytes"]),
                sha256=row["sha256"].lower(),
                object_key=row["object_key"],
                priority=int(row["priority"]),
                session_created_at=row["session_created_at"],
                estimated_cost_ms=int(row["estimated_latency_ms"]),
                order_index=index + 1,
            )
            for index, row in enumerate(rows)
        ]

    async def _run_round_locked(self, request: ScheduleRoundRequest) -> dict[str, Any]:
        budget_ms = request.time_budget_ms or self.settings.default_time_budget_ms
        queue_limit = request.queue_limit if request.queue_limit is not None else self.settings.queue_limit
        write_timeout_ms = request.write_timeout_ms or self.settings.write_timeout_ms
        read_timeout_ms = request.read_timeout_ms or self.settings.read_timeout_ms
        candidates = await self._load_candidates(request.session_ids)
        round_id = f"round-{uuid.uuid4().hex[:16]}"
        now = utc_now()

        consumed_ms = 0
        admitted: list[Candidate] = []
        deferred: list[tuple[Candidate, dict[str, Any]]] = []
        for candidate in candidates:
            remaining = budget_ms - consumed_ms
            if candidate.estimated_cost_ms <= remaining:
                admitted.append(candidate)
                consumed_ms += candidate.estimated_cost_ms
            else:
                deferred.append(
                    (
                        candidate,
                        {
                            "policy": "priority_asc_then_seq_asc",
                            "reason": "insufficient_time_budget",
                            "budget_ms": budget_ms,
                            "budget_consumed_ms": consumed_ms,
                            "budget_remaining_ms": max(0, remaining),
                            "required_ms": candidate.estimated_cost_ms,
                            "downgrade_rank": len(deferred) + 1,
                            "dispatched_tasks_are_not_cancelled_by_budget": True,
                        },
                    )
                )

        effective_queue_limit = min(queue_limit, self.settings.queue_limit)
        admission_capacity = self.settings.global_concurrency + effective_queue_limit
        if len(admitted) > admission_capacity:
            raise business_error(
                "E_SCHED_QUEUE_FULL",
                "ready tasks exceed bounded global concurrency plus wait queue",
                ready_tasks=len(admitted),
                global_concurrency=self.settings.global_concurrency,
                queue_limit=effective_queue_limit,
                admission_capacity=admission_capacity,
                quota_reserved=0,
            )
        reserved_gate = await self.admission.reserve(len(admitted)) if admitted else True
        if not reserved_gate:
            raise business_error(
                "E_SCHED_QUEUE_FULL",
                "scheduler admission queue is full",
                requested_slots=len(admitted),
                admission_capacity=self.admission.capacity,
                admitted_elsewhere=self.admission.used,
                quota_reserved=0,
            )

        backend_urls = {backend.id: backend.url for backend in self.settings.backends}
        task_rows: dict[tuple[str, int], dict[str, Any]] = {}
        try:
            with self.database.transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO scheduling_rounds(id, status, time_budget_ms, queue_limit,
                        write_timeout_ms, read_timeout_ms, session_filter_json,
                        summary_json, created_at)
                    VALUES(?, 'RUNNING', ?, ?, ?, ?, ?, '{}', ?)
                    """,
                    (
                        round_id,
                        budget_ms,
                        queue_limit,
                        write_timeout_ms,
                        read_timeout_ms,
                        _json(request.session_ids or []),
                        now,
                    ),
                )
                for candidate, basis in deferred:
                    task_id = f"task-{uuid.uuid4().hex[:16]}"
                    connection.execute(
                        """
                        INSERT INTO round_tasks(id, round_id, session_id, seq, backend_id,
                            state, business_code, reason, priority, order_index,
                            detail_json, created_at, updated_at)
                        VALUES(?, ?, ?, ?, ?, 'DEFERRED', 'E_TIME_BUDGET_DEFERRED',
                               'time budget admission denied', ?, ?, ?, ?, ?)
                        """,
                        (
                            task_id,
                            round_id,
                            candidate.session_id,
                            candidate.seq,
                            candidate.backend_id,
                            candidate.priority,
                            candidate.order_index,
                            _json(basis),
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        """
                        UPDATE parts SET status='DEFERRED', round_id=?, last_error_code=NULL,
                            last_error_context_json=?, updated_at=?
                        WHERE session_id=? AND seq=? AND status IN ('PENDING','DEFERRED')
                        """,
                        (round_id, _json(basis), now, candidate.session_id, candidate.seq),
                    )
                    connection.execute(
                        "UPDATE sessions SET version=version+1, updated_at=? WHERE id=?",
                        (now, candidate.session_id),
                    )
                admitted_cost_after: dict[tuple[str, int], int] = {}
                running_cost = 0
                for candidate in admitted:
                    running_cost += candidate.estimated_cost_ms
                    admitted_cost_after[(candidate.session_id, candidate.seq)] = running_cost
                for candidate in admitted:
                    task_id = f"task-{uuid.uuid4().hex[:16]}"
                    intent_id = f"intent-{uuid.uuid4().hex[:16]}"
                    prior = connection.execute(
                        "SELECT status FROM parts WHERE session_id=? AND seq=?",
                        (candidate.session_id, candidate.seq),
                    ).fetchone()
                    connection.execute(
                        """
                        INSERT INTO round_tasks(id, round_id, session_id, seq, backend_id,
                            state, priority, order_index, detail_json, created_at, updated_at)
                        VALUES(?, ?, ?, ?, ?, 'QUEUED', ?, ?, ?, ?, ?)
                        """,
                        (
                            task_id,
                            round_id,
                            candidate.session_id,
                            candidate.seq,
                            candidate.backend_id,
                            candidate.priority,
                            candidate.order_index,
                            _json(
                                {
                                    "estimated_cost_ms": candidate.estimated_cost_ms,
                                    "budget_consumed_after_admission_ms": admitted_cost_after[
                                        (candidate.session_id, candidate.seq)
                                    ],
                                }
                            ),
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO schedule_intents(id, round_id, backend_id, session_id, seq,
                            bytes, prior_part_status, state, created_at, updated_at)
                        VALUES(?, ?, ?, ?, ?, ?, ?, 'PREPARED', ?, ?)
                        """,
                        (
                            intent_id,
                            round_id,
                            candidate.backend_id,
                            candidate.session_id,
                            candidate.seq,
                            candidate.size_bytes,
                            prior["status"] if prior else None,
                            now,
                            now,
                        ),
                    )
                    task_rows[(candidate.session_id, candidate.seq)] = {
                        "task_id": task_id,
                        "intent_id": intent_id,
                        "candidate": candidate,
                        "backend_url": backend_urls[candidate.backend_id],
                    }
                connection.execute(
                    """
                    INSERT INTO audit_events(round_id, event_type, detail_json, created_at)
                    VALUES(?, 'ROUND_PLANNED', ?, ?)
                    """,
                    (
                        round_id,
                        _json(
                            {
                                "admitted": len(admitted),
                                "budget_deferred": len(deferred),
                                "budget_ms": budget_ms,
                                "planned_cost_ms": consumed_ms,
                            }
                        ),
                        now,
                    ),
                )
            task_specs = list(task_rows.values())
            results = await asyncio.gather(
                *[
                    self._execute_task(
                        round_id,
                        spec,
                        write_timeout_ms,
                        read_timeout_ms,
                    )
                    for spec in task_specs
                ],
                return_exceptions=True,
            )
            unexpected_error = next(
                (item for item in results if isinstance(item, BaseException)), None
            )
            if unexpected_error is not None:
                raise unexpected_error
        finally:
            if admitted:
                await self.admission.release(len(admitted))

        return await self._finish_round(round_id)

    async def _execute_task(
        self,
        round_id: str,
        spec: dict[str, Any],
        write_timeout_ms: int,
        read_timeout_ms: int,
    ) -> dict[str, Any]:
        candidate: Candidate = spec["candidate"]
        self._queued_count += 1
        self._peak_queued = max(self._peak_queued, self._queued_count)
        acquired_global = False
        try:
            async with self.global_semaphore:
                self._queued_count -= 1
                acquired_global = True
                self._active_count += 1
                self._peak_active = max(self._peak_active, self._active_count)
                try:
                    async with self.backend_semaphores[candidate.backend_id]:
                        return await self._execute_admitted_task(
                            round_id,
                            spec,
                            write_timeout_ms,
                            read_timeout_ms,
                        )
                finally:
                    self._active_count -= 1
        finally:
            if not acquired_global:
                self._queued_count = max(0, self._queued_count - 1)

    async def _execute_admitted_task(
        self,
        round_id: str,
        spec: dict[str, Any],
        write_timeout_ms: int,
        read_timeout_ms: int,
    ) -> dict[str, Any]:
        candidate: Candidate = spec["candidate"]
        task_id: str = spec["task_id"]
        intent_id: str = spec["intent_id"]
        reservation_id = f"reservation-{uuid.uuid4().hex[:16]}"
        now = utc_now()
        quota_before: dict[str, Any] | None = None
        try:
            with self.database.transaction() as connection:
                intent = connection.execute(
                    "SELECT state FROM schedule_intents WHERE id=?", (intent_id,)
                ).fetchone()
                if intent is None or intent["state"] != "PREPARED":
                    raise business_error(
                        "E_SCHED_ALREADY_CLAIMED",
                        "schedule intent is no longer dispatchable",
                        round_id=round_id,
                        session_id=candidate.session_id,
                        seq=candidate.seq,
                    )
                part = connection.execute(
                    "SELECT status FROM parts WHERE session_id=? AND seq=?",
                    (candidate.session_id, candidate.seq),
                ).fetchone()
                if part is None or part["status"] not in {"PENDING", "DEFERRED"}:
                    raise business_error(
                        "E_SCHED_ALREADY_CLAIMED",
                        "part is no longer dispatchable",
                        round_id=round_id,
                        session_id=candidate.session_id,
                        seq=candidate.seq,
                        part_status=part["status"] if part else None,
                    )
                quota = connection.execute(
                    "SELECT * FROM backend_quota WHERE backend_id=?",
                    (candidate.backend_id,),
                ).fetchone()
                if quota is None:
                    raise business_error(
                        "E_NOT_FOUND",
                        "backend quota declaration not found",
                        backend_id=candidate.backend_id,
                    )
                quota_before = dict(quota)
                updated = connection.execute(
                    """
                    UPDATE backend_quota
                    SET reserved_bytes=reserved_bytes+?, version=version+1, updated_at=?
                    WHERE backend_id=?
                      AND capacity_bytes - committed_bytes - reserved_bytes >= ?
                    """,
                    (candidate.size_bytes, now, candidate.backend_id, candidate.size_bytes),
                ).rowcount
                if updated != 1:
                    remaining = max(
                        0,
                        quota["capacity_bytes"] - quota["committed_bytes"] - quota["reserved_bytes"],
                    )
                    raise business_error(
                        "E_QUOTA_EXCEEDED",
                        "backend quota cannot accommodate this part",
                        backend_id=candidate.backend_id,
                        seq=candidate.seq,
                        requested_bytes=candidate.size_bytes,
                        remaining_bytes=remaining,
                        quota_version=quota["version"],
                    )
                connection.execute(
                    """
                    INSERT INTO quota_reservations(id, intent_id, backend_id, session_id, seq,
                        bytes, status, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, 'RESERVED', ?, ?)
                    """,
                    (
                        reservation_id,
                        intent_id,
                        candidate.backend_id,
                        candidate.session_id,
                        candidate.seq,
                        candidate.size_bytes,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE schedule_intents SET state='UPLOADING', updated_at=? WHERE id=?",
                    (now, intent_id),
                )
                connection.execute(
                    """
                    UPDATE parts SET status='UPLOADING', round_id=?, attempt_count=attempt_count+1,
                        last_error_code=NULL, last_error_context_json='{}', updated_at=?
                    WHERE session_id=? AND seq=?
                    """,
                    (round_id, now, candidate.session_id, candidate.seq),
                )
                connection.execute(
                    """
                    UPDATE round_tasks SET state='DISPATCHING', updated_at=?
                    WHERE id=?
                    """,
                    (now, task_id),
                )
                connection.execute(
                    "UPDATE sessions SET version=version+1, updated_at=? WHERE id=?",
                    (now, candidate.session_id),
                )
                connection.execute(
                    """
                    INSERT INTO audit_events(round_id, session_id, event_type, detail_json, created_at)
                    VALUES(?, ?, 'QUOTA_RESERVED', ?, ?)
                    """,
                    (
                        round_id,
                        candidate.session_id,
                        _json(
                            {
                                "seq": candidate.seq,
                                "backend_id": candidate.backend_id,
                                "bytes": candidate.size_bytes,
                                "intent_id": intent_id,
                            }
                        ),
                        now,
                    ),
                )

            try:
                verification = await self._write_and_verify(
                    candidate,
                    spec["backend_url"],
                    write_timeout_ms,
                    read_timeout_ms,
                )
            except BusinessError as error:
                await self._mark_task_failure(
                    round_id=round_id,
                    task_id=task_id,
                    intent_id=intent_id,
                    candidate=candidate,
                    code=error.code,
                    context=error.context,
                    stage=error.context.get("stage"),
                )
                return {"state": "FAILED", "code": error.code}
            await self._commit_task_success(
                round_id=round_id,
                task_id=task_id,
                intent_id=intent_id,
                candidate=candidate,
                verification=verification,
                quota_before=quota_before,
            )
            return {"state": "COMMITTED", "code": None}
        except BusinessError as error:
            await self._mark_task_failure(
                round_id=round_id,
                task_id=task_id,
                intent_id=intent_id,
                candidate=candidate,
                code=error.code,
                context=error.context,
                stage=error.context.get("stage"),
            )
            return {"state": "FAILED", "code": error.code}
        except BaseException:
            raise

    async def _mark_task_failure(
        self,
        *,
        round_id: str,
        task_id: str,
        intent_id: str,
        candidate: Candidate,
        code: str,
        context: dict[str, Any],
        stage: str | None,
    ) -> None:
        now = utc_now()
        with self.database.transaction() as connection:
            reservation = connection.execute(
                "SELECT * FROM quota_reservations WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if reservation is not None and reservation["status"] == "RESERVED":
                connection.execute(
                    """
                    UPDATE backend_quota SET reserved_bytes=MAX(0, reserved_bytes-?),
                        version=version+1, updated_at=? WHERE backend_id=?
                    """,
                    (reservation["bytes"], now, reservation["backend_id"]),
                )
                connection.execute(
                    "UPDATE quota_reservations SET status='RELEASED', updated_at=? WHERE id=?",
                    (now, reservation["id"]),
                )
            connection.execute(
                """
                UPDATE schedule_intents SET state='FAILED', error_code=?, error_context_json=?,
                    updated_at=? WHERE id=?
                """,
                (code, _json(context), now, intent_id),
            )
            part = connection.execute(
                "SELECT status FROM parts WHERE session_id=? AND seq=?",
                (candidate.session_id, candidate.seq),
            ).fetchone()
            if part is not None and part["status"] in {"PENDING", "DEFERRED", "UPLOADING"}:
                connection.execute(
                    """
                    UPDATE parts SET status='DEFERRED', last_error_code=?, last_error_context_json=?,
                        updated_at=? WHERE session_id=? AND seq=?
                    """,
                    (code, _json(context), now, candidate.session_id, candidate.seq),
                )
            connection.execute(
                """
                UPDATE round_tasks SET state='FAILED', business_code=?, reason=?, detail_json=?,
                    updated_at=? WHERE id=?
                """,
                (
                    code,
                    context.get("message", code),
                    _json({"stage": stage, "context": context}),
                    now,
                    task_id,
                ),
            )
            connection.execute(
                "UPDATE sessions SET version=version+1, updated_at=? WHERE id=?",
                (now, candidate.session_id),
            )
            connection.execute(
                """
                INSERT INTO audit_events(round_id, session_id, event_type, detail_json, created_at)
                VALUES(?, ?, 'PART_FAILED', ?, ?)
                """,
                (
                    round_id,
                    candidate.session_id,
                    _json({"seq": candidate.seq, "code": code, "stage": stage, "context": context}),
                    now,
                ),
            )

    async def _commit_task_success(
        self,
        *,
        round_id: str,
        task_id: str,
        intent_id: str,
        candidate: Candidate,
        verification: dict[str, Any],
        quota_before: dict[str, Any] | None,
    ) -> None:
        now = utc_now()
        with self.database.transaction() as connection:
            reservation = connection.execute(
                "SELECT * FROM quota_reservations WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if reservation is None or reservation["status"] != "RESERVED":
                raise business_error(
                    "E_RECOVERY_INVARIANT",
                    "successful verification has no live quota reservation",
                    session_id=candidate.session_id,
                    seq=candidate.seq,
                )
            connection.execute(
                """
                UPDATE backend_quota SET reserved_bytes=reserved_bytes-?,
                    committed_bytes=committed_bytes+?, version=version+1, updated_at=?
                WHERE backend_id=?
                """,
                (reservation["bytes"], reservation["bytes"], now, candidate.backend_id),
            )
            quota_after = connection.execute(
                "SELECT * FROM backend_quota WHERE backend_id=?", (candidate.backend_id,)
            ).fetchone()
            connection.execute(
                "UPDATE quota_reservations SET status='COMMITTED', updated_at=? WHERE id=?",
                (now, reservation["id"]),
            )
            connection.execute(
                """
                UPDATE parts SET status='VERIFIED', verified_at=?, updated_at=?,
                    last_error_code=NULL, last_error_context_json='{}'
                WHERE session_id=? AND seq=? AND status='UPLOADING'
                """,
                (now, now, candidate.session_id, candidate.seq),
            )
            connection.execute(
                "UPDATE schedule_intents SET state='COMMITTED', updated_at=? WHERE id=?",
                (now, intent_id),
            )
            connection.execute(
                """
                UPDATE round_tasks SET state='COMMITTED', business_code=NULL,
                    reason='write and readback verification passed', detail_json=?, updated_at=?
                WHERE id=?
                """,
                (
                    _json(
                        {
                            "verification": verification,
                            "quota_before": quota_before,
                            "quota_after": dict(quota_after) if quota_after else None,
                        }
                    ),
                    now,
                    task_id,
                ),
            )
            all_parts = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM parts WHERE session_id=? ORDER BY seq", (candidate.session_id,)
                ).fetchall()
            ]
            verified_parts = [part for part in all_parts if part["status"] == "VERIFIED"]
            session = connection.execute(
                "SELECT * FROM sessions WHERE id=?", (candidate.session_id,)
            ).fetchone()
            complete = (
                {int(part["seq"]) for part in verified_parts}
                == set(range(1, session["total_parts"] + 1))
            )
            new_status = "COMPLETED" if complete else session["status"]
            object_digest = canonical_object_digest(verified_parts) if complete else None
            new_version = session["version"] + 1
            connection.execute(
                """
                UPDATE sessions SET status=?, completed_parts=?, object_digest=?,
                    version=?, updated_at=? WHERE id=?
                """,
                (
                    new_status,
                    len(verified_parts),
                    object_digest,
                    new_version,
                    now,
                    candidate.session_id,
                ),
            )
            manifest = manifest_from_parts(verified_parts)
            connection.execute(
                """
                UPDATE object_metadata SET status=?, manifest_json=?, object_digest=?,
                    version=?, updated_at=? WHERE session_id=?
                """,
                (
                    "READY" if complete else "DRAFT",
                    _json(manifest),
                    object_digest,
                    new_version,
                    now,
                    candidate.session_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO audit_events(round_id, session_id, event_type, detail_json, created_at)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    round_id,
                    candidate.session_id,
                    "PART_VERIFIED" if not complete else "OBJECT_READY",
                    _json(
                        {
                            "seq": candidate.seq,
                            "object_digest": object_digest,
                            "completed_parts": len(verified_parts),
                            "session_version": new_version,
                        }
                    ),
                    now,
                ),
            )

    async def _finish_round(self, round_id: str) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            round_row = connection.execute(
                "SELECT * FROM scheduling_rounds WHERE id=?", (round_id,)
            ).fetchone()
            tasks = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM round_tasks WHERE round_id=? ORDER BY order_index", (round_id,)
                ).fetchall()
            ]
            counts: dict[str, int] = {}
            for task in tasks:
                counts[task["state"]] = counts.get(task["state"], 0) + 1
            failed = counts.get("FAILED", 0)
            deferred_count = counts.get("DEFERRED", 0)
            if deferred_count and failed:
                status = "DEGRADED_WITH_ERRORS"
            elif failed:
                status = "COMPLETED_WITH_ERRORS"
            elif deferred_count:
                status = "DEGRADED"
            else:
                status = "COMPLETED"
            first_error = next((task for task in tasks if task["state"] == "FAILED"), None)
            summary = {
                "task_states": counts,
                "verified": counts.get("COMMITTED", 0),
                "failed": failed,
                "deferred": deferred_count,
                "business_codes": sorted(
                    {task["business_code"] for task in tasks if task["business_code"]}
                ),
                "scheduler_metrics": {
                    "global_peak_active": self._peak_active,
                    "global_peak_queued": self._peak_queued,
                    "admission_capacity": self.admission.capacity,
                    "admission_peak_used": self.admission.peak_used,
                },
            }
            connection.execute(
                """
                UPDATE scheduling_rounds SET status=?, finished_at=?, summary_json=? WHERE id=?
                """,
                (status, now, _json(summary), round_id),
            )
            connection.execute(
                """
                INSERT INTO audit_events(round_id, event_type, detail_json, created_at)
                VALUES(?, 'ROUND_FINISHED', ?, ?)
                """,
                (round_id, _json({"status": status, **summary}), now),
            )
            quotas = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT backend_id, capacity_bytes, committed_bytes, reserved_bytes,
                           capacity_bytes-committed_bytes-reserved_bytes AS remaining_bytes,
                           version
                    FROM backend_quota ORDER BY backend_id
                    """
                ).fetchall()
            ]
        trace = []
        for task in tasks:
            trace.append(
                {
                    "task_id": task["id"],
                    "session_id": task["session_id"],
                    "seq": task["seq"],
                    "backend_id": task["backend_id"],
                    "priority": task["priority"],
                    "order_index": task["order_index"],
                    "state": task["state"],
                    "business_code": task["business_code"],
                    "reason": task["reason"],
                    "detail": json.loads(task["detail_json"] or "{}"),
                    "updated_at": task["updated_at"],
                }
            )
        response = {
            "round_id": round_id,
            "status": status,
            "time_budget_ms": round_row["time_budget_ms"],
            "queue_limit": round_row["queue_limit"],
            "write_timeout_ms": round_row["write_timeout_ms"],
            "read_timeout_ms": round_row["read_timeout_ms"],
            "summary": summary,
            "trace": trace,
            "quota_after": quotas,
            "errors": [
                {
                    "task_id": task["id"],
                    "session_id": task["session_id"],
                    "seq": task["seq"],
                    "code": task["business_code"],
                    "context": json.loads(task["detail_json"] or "{}").get("context", {}),
                }
                for task in tasks
                if task["state"] == "FAILED"
            ],
        }
        if first_error is not None and counts.get("COMMITTED", 0) == 0 and failed:
            context = json.loads(first_error["detail_json"] or {}).get("context", {})
            raise BusinessError(
                str(first_error["business_code"]),
                first_error["reason"] or "all admitted scheduler tasks failed",
                {**context, "round": response},
                STATUS_BY_CODE.get(str(first_error["business_code"]), 502),
            )
        return response

    async def get_round(self, round_id: str) -> dict[str, Any]:
        with self.database.transaction(immediate=False) as connection:
            round_row = connection.execute(
                "SELECT * FROM scheduling_rounds WHERE id=?", (round_id,)
            ).fetchone()
            if round_row is None:
                raise business_error("E_NOT_FOUND", "scheduling round not found", round_id=round_id)
            tasks = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM round_tasks WHERE round_id=? ORDER BY order_index", (round_id,)
                ).fetchall()
            ]
        return {
            "round_id": round_row["id"],
            "status": round_row["status"],
            "time_budget_ms": round_row["time_budget_ms"],
            "queue_limit": round_row["queue_limit"],
            "summary": json.loads(round_row["summary_json"] or "{}"),
            "trace": [
                {
                    **task,
                    "detail": json.loads(task.pop("detail_json") or "{}"),
                }
                for task in tasks
            ],
            "created_at": round_row["created_at"],
            "finished_at": round_row["finished_at"],
            "recovered_at": round_row["recovered_at"],
        }
