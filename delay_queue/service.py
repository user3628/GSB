from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings
from .dispatch import DispatchResult, dispatch_once
from .errors import (
    E_CONFLICTING_ROUND,
    E_INTERNAL,
    E_INVALID_CONFIG,
    E_NOT_FOUND,
    E_QUEUE_FULL,
    E_ROUND_IN_PROGRESS,
    BusinessError,
)
from .model import canonical_json, validate_task_batch
from .storage import Storage, now_ms


@dataclass(frozen=True)
class RoundConfig:
    max_concurrency: int
    time_budget_ms: int
    max_task_resource: int
    waiting_queue_capacity: int
    dispatch_timeout_ms: int

    def as_dict(self) -> dict[str, int]:
        return {
            "max_concurrency": self.max_concurrency,
            "time_budget_ms": self.time_budget_ms,
            "max_task_resource": self.max_task_resource,
            "waiting_queue_capacity": self.waiting_queue_capacity,
            "dispatch_timeout_ms": self.dispatch_timeout_ms,
        }


def parse_round_config(body: Any, settings: Settings) -> RoundConfig:
    if body is None:
        body = {}
    if not isinstance(body, dict):
        raise BusinessError(
            E_INVALID_CONFIG, "round request body must be an object", {}, 422
        )

    def read(names: tuple[str, ...], default: int) -> int:
        for name in names:
            if name in body:
                return body[name]  # type: ignore[return-value]
        return default

    raw = {
        "max_concurrency": read(
            ("max_concurrency", "concurrency_limit", "concurrency"),
            settings.default_max_concurrency,
        ),
        "time_budget_ms": read(("time_budget_ms", "time_budget"), settings.default_time_budget_ms),
        "max_task_resource": read(
            ("max_task_resource", "per_task_resource_budget", "resource_budget"),
            settings.default_max_task_resource,
        ),
        "waiting_queue_capacity": read(
            ("waiting_queue_capacity", "queue_capacity"),
            settings.default_waiting_queue_capacity,
        ),
        "dispatch_timeout_ms": read(
            ("dispatch_timeout_ms", "request_timeout_ms", "per_request_timeout_ms"),
            settings.default_dispatch_timeout_ms,
        ),
    }
    errors = []
    for name, value in raw.items():
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append({"field": name, "reason": "must be an integer"})
    if not errors:
        if raw["max_concurrency"] < 1:
            errors.append({"field": "max_concurrency", "reason": "must be >= 1"})
        if raw["time_budget_ms"] < 0:
            errors.append({"field": "time_budget_ms", "reason": "must be >= 0"})
        if raw["max_task_resource"] < 1:
            errors.append({"field": "max_task_resource", "reason": "must be >= 1"})
        if raw["waiting_queue_capacity"] < 0:
            errors.append({"field": "waiting_queue_capacity", "reason": "must be >= 0"})
        if raw["dispatch_timeout_ms"] < 1:
            errors.append({"field": "dispatch_timeout_ms", "reason": "must be >= 1"})
    if errors:
        raise BusinessError(
            E_INVALID_CONFIG,
            "invalid round configuration",
            {"errors": errors},
            422,
        )
    return RoundConfig(**raw)


class DelayQueueService:
    MIN_DISPATCH_WINDOW_MS = 5

    def __init__(self, settings: Settings, storage: Storage | None = None):
        settings.validate()
        self.settings = settings
        self.storage = storage or Storage(settings.db_path)
        self.storage.initialize()
        Path(settings.intent_log_dir).mkdir(parents=True, exist_ok=True)
        self._queue_locks: dict[str, asyncio.Lock] = {}
        self._queue_locks_loop: asyncio.AbstractEventLoop | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._semaphore_loop: asyncio.AbstractEventLoop | None = None
        self.startup_recovery_report: dict[str, Any] | None = None

    def startup(self) -> dict[str, Any]:
        self.startup_recovery_report = self.storage.recover_incomplete_intents(now_ms())
        return self.startup_recovery_report

    def _get_semaphore(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        if self._semaphore is None or self._semaphore_loop is not loop:
            self._semaphore = asyncio.Semaphore(self.settings.global_concurrency)
            self._semaphore_loop = loop
        return self._semaphore

    def _queue_lock(self, queue_name: str) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._queue_locks_loop is not loop:
            self._queue_locks = {}
            self._queue_locks_loop = loop
        lock = self._queue_locks.get(queue_name)
        if lock is None:
            lock = asyncio.Lock()
            self._queue_locks[queue_name] = lock
        return lock

    def submit_tasks(
        self, queue_name: str, raw_tasks: Any, default_endpoint: str | None = None
    ) -> dict[str, Any]:
        specs, batch_duplicates = validate_task_batch(
            queue_name,
            raw_tasks,
            default_endpoint or self.settings.default_endpoint,
            self.settings.allow_non_local_endpoints,
        )
        result = self.storage.insert_tasks(
            specs, [entry["task_id"] for entry in batch_duplicates]
        )
        return {
            "ok": True,
            "queue": queue_name,
            "accepted": len(specs),
            "inserted": len(result["inserted"]),
            "duplicates": result["duplicates"],
            "batch_duplicates": batch_duplicates,
            "tasks": result["tasks"],
        }

    async def submit_tasks_locked(
        self, queue_name: str, raw_tasks: Any, default_endpoint: str | None = None
    ) -> dict[str, Any]:
        """HTTP-facing submission serialized with the queue's scheduler writer."""
        async with self._queue_lock(queue_name):
            return await asyncio.to_thread(
                self.submit_tasks, queue_name, raw_tasks, default_endpoint
            )

    async def manual_transition_locked(
        self, queue_name: str, task_id: str, to_state: str, reason: str
    ) -> dict[str, Any]:
        async with self._queue_lock(queue_name):
            return await asyncio.to_thread(
                self.storage.manual_transition,
                queue_name,
                task_id,
                to_state,
                reason,
            )

    def _round_fingerprint(
        self, queue_name: str, round_id: str, config: RoundConfig
    ) -> str:
        return canonical_json(
            {
                "schema": "delayed-round-v1",
                "queue": queue_name,
                "round_id": round_id,
                "requested_config": config.as_dict(),
                "execution_policy": {
                    "global_concurrency": self.settings.global_concurrency,
                    "retry_backoff_base_ms": self.settings.retry_backoff_base_ms,
                },
            }
        )

    @staticmethod
    def _attached_result(row: dict[str, Any], reused: bool) -> dict[str, Any]:
        result = dict(row.get("result") or {})
        result["round_id"] = row["round_id"]
        result["queue"] = row["queue_name"]
        result["status"] = row["status"]
        result["reused"] = reused
        result["started_at_ms"] = row["started_at_ms"]
        result["finished_at_ms"] = row["finished_at_ms"]
        return result

    @staticmethod
    def _raise_stored_error(row: dict[str, Any]) -> None:
        error = row.get("error") or {}
        status_map = {
            E_QUEUE_FULL: 429,
            E_CONFLICTING_ROUND: 409,
            E_ROUND_IN_PROGRESS: 409,
            "E_PROCESS_INTERRUPTED": 409,
        }
        code = error.get("code", E_INTERNAL)
        raise BusinessError(
            code,
            error.get("message", f"round ended with {code}"),
            error.get("context", {"round_id": row["round_id"]}),
            status_map.get(code, 409),
        )

    async def _reuse_existing_round(
        self, row: dict[str, Any], queue_name: str, fingerprint: str
    ) -> dict[str, Any]:
        if row["fingerprint"] != fingerprint:
            raise BusinessError(
                E_CONFLICTING_ROUND,
                "round id is already bound to a different queue or configuration",
                {
                    "round_id": row["round_id"],
                    "existing_fingerprint": row["fingerprint"],
                    "submitted_fingerprint": fingerprint,
                },
                409,
            )
        if row["status"] == "RUNNING":
            deadline = time.monotonic() + (self.settings.round_wait_timeout_ms / 1_000)
            while time.monotonic() < deadline:
                await asyncio.sleep(0.02)
                refreshed = self.storage.get_round(row["round_id"])
                if refreshed is None:
                    break
                row = refreshed
                if row["status"] != "RUNNING":
                    break
            if row["status"] == "RUNNING":
                raise BusinessError(
                    E_ROUND_IN_PROGRESS,
                    "the same round is still running; retry later to reuse its result",
                    {"round_id": row["round_id"], "queue": queue_name},
                    409,
                )
        if row["status"] in {"FAILED", "INTERRUPTED"}:
            self._raise_stored_error(row)
        return self._attached_result(row, reused=True)

    async def trigger_round(
        self, queue_name: str, round_id: str, body: Any
    ) -> dict[str, Any]:
        if not isinstance(round_id, str) or not round_id.strip():
            raise BusinessError(
                E_INVALID_CONFIG,
                "round_id must be a non-empty string",
                {"field": "round_id"},
                422,
            )
        config = parse_round_config(body, self.settings)
        fingerprint = self._round_fingerprint(queue_name, round_id, config)
        effective_config = config.as_dict()
        effective_config["effective_max_concurrency"] = min(
            config.max_concurrency, self.settings.global_concurrency
        )
        effective_config["global_concurrency"] = self.settings.global_concurrency
        effective_config["retry_backoff_base_ms"] = self.settings.retry_backoff_base_ms

        async with self._queue_lock(queue_name):
            existing = self.storage.get_round(round_id)
            if existing is not None:
                return await self._reuse_existing_round(
                    existing, queue_name, fingerprint
                )

            claimed, created = self.storage.claim_round(
                round_id,
                queue_name,
                fingerprint,
                config.as_dict(),
                effective_config,
                now_ms(),
            )
            if not created:
                return await self._reuse_existing_round(
                    claimed, queue_name, fingerprint
                )

            try:
                result = await self._run_round(
                    queue_name, round_id, config, effective_config, claimed["started_at_ms"]
                )
            except BusinessError as exc:
                self.storage.finish_round(
                    round_id,
                    "FAILED",
                    {
                        "round_id": round_id,
                        "queue": queue_name,
                        "status": "FAILED",
                        "error_code": exc.code,
                        "context": exc.context,
                        "started_at_ms": claimed["started_at_ms"],
                        "finished_at_ms": now_ms(),
                    },
                    exc,
                    now_ms(),
                )
                raise
            except BaseException as exc:
                internal = BusinessError(
                    E_INTERNAL,
                    f"round failed unexpectedly: {type(exc).__name__}",
                    {"round_id": round_id, "queue": queue_name},
                    500,
                )
                self.storage.finish_round(
                    round_id,
                    "FAILED",
                    {
                        "round_id": round_id,
                        "queue": queue_name,
                        "status": "FAILED",
                        "error_code": E_INTERNAL,
                        "started_at_ms": claimed["started_at_ms"],
                        "finished_at_ms": now_ms(),
                    },
                    internal,
                    now_ms(),
                )
                raise internal from exc

            finished = self.storage.finish_round(
                round_id, "COMPLETED", result, None, result["finished_at_ms"]
            )
            return self._attached_result(finished, reused=False)

    def _skip_entry(self, task: dict[str, Any], reason: str, **extra: Any) -> dict[str, Any]:
        return {
            "task_id": task["task_id"],
            "priority": task["priority"],
            "due_at_ms": task["due_at_ms"],
            "resource_requirement": task["resource_requirement"],
            "state_before": task["state"],
            "reason": reason,
            **extra,
        }

    async def _run_round(
        self,
        queue_name: str,
        round_id: str,
        config: RoundConfig,
        effective_config: dict[str, Any],
        started_at_ms: int,
    ) -> dict[str, Any]:
        semaphore = self._get_semaphore()
        deadline_monotonic = time.monotonic() + (config.time_budget_ms / 1_000)
        at_ms = now_ms()
        candidates = self.storage.peek_schedulable(queue_name, at_ms)
        skipped: list[dict[str, Any]] = []
        eligible: list[dict[str, Any]] = []
        for task in candidates:
            if task["resource_requirement"] > config.max_task_resource:
                skipped.append(
                    self._skip_entry(
                        task,
                        "RESOURCE_BUDGET_EXCEEDED",
                        max_task_resource=config.max_task_resource,
                    )
                )
            else:
                eligible.append(task)

        effective_concurrency = min(
            config.max_concurrency, self.settings.global_concurrency
        )
        selected = eligible[:effective_concurrency]
        waiting = eligible[effective_concurrency:]
        if len(waiting) > config.waiting_queue_capacity:
            raise BusinessError(
                E_QUEUE_FULL,
                "due task backlog exceeds the bounded waiting queue",
                {
                    "round_id": round_id,
                    "queue": queue_name,
                    "due_candidates": len(candidates),
                    "resource_skipped": len(skipped),
                    "dispatch_capacity": effective_concurrency,
                    "waiting_count": len(waiting),
                    "waiting_queue_capacity": config.waiting_queue_capacity,
                },
                429,
            )

        if selected or waiting:
            self.storage.promote_due_tasks(
                queue_name, [task["task_id"] for task in selected + waiting], now_ms()
            )

        acquired = 0
        selected_to_dispatch: list[dict[str, Any]] = []
        admission_failure = "TIME_BUDGET_EXHAUSTED"
        for task in selected:
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                admission_failure = "TIME_BUDGET_EXHAUSTED"
                break
            try:
                await asyncio.wait_for(
                    semaphore.acquire(), timeout=min(remaining, 0.01)
                )
            except asyncio.TimeoutError:
                admission_failure = (
                    "TIME_BUDGET_EXHAUSTED"
                    if time.monotonic() >= deadline_monotonic
                    else "GLOBAL_CONCURRENCY_LIMIT"
                )
                break
            acquired += 1
            selected_to_dispatch.append(task)

        for task in selected[len(selected_to_dispatch) :]:
            skipped.append(
                self._skip_entry(
                    task, admission_failure, consumed_by="global_admission"
                )
            )
        for task in waiting:
            skipped.append(
                self._skip_entry(
                    task,
                    "WAITING_QUEUE",
                    waiting_queue_capacity=config.waiting_queue_capacity,
                )
            )

        remaining_after_admission = deadline_monotonic - time.monotonic()
        if selected_to_dispatch and remaining_after_admission * 1_000 < self.MIN_DISPATCH_WINDOW_MS:
            for _ in range(acquired):
                semaphore.release()
            acquired = 0
            minimum_window_skips = selected_to_dispatch
            selected_to_dispatch = []
            for task in minimum_window_skips:
                skipped.append(
                    self._skip_entry(
                        task,
                        "TIME_BUDGET_EXHAUSTED",
                        consumed_by="minimum_send_window",
                        remaining_ms=max(0, int(remaining_after_admission * 1_000)),
                    )
                )

        dispatched_results: list[dict[str, Any]] = []
        prepared: list[dict[str, Any]] = []
        try:
            if selected_to_dispatch and acquired:
                prepared = self.storage.prepare_attempts(
                    queue_name,
                    round_id,
                    [task["task_id"] for task in selected_to_dispatch],
                    now_ms(),
                )
        except BaseException:
            for _ in range(acquired):
                semaphore.release()
            raise

        if len(prepared) < acquired:
            for _ in range(acquired - len(prepared)):
                semaphore.release()
        workers = [
            asyncio.create_task(
                self._execute_prepared_attempt(
                    attempt,
                    semaphore,
                    deadline_monotonic,
                    config.dispatch_timeout_ms,
                    round_id,
                )
            )
            for attempt in prepared
        ]
        if workers:
            raw_results = await asyncio.gather(*workers, return_exceptions=True)
            failures = [item for item in raw_results if isinstance(item, BaseException)]
            if failures:
                raise failures[0]
            dispatched_results = [item for item in raw_results if isinstance(item, dict)]

        dispatched_results.sort(key=lambda item: (item["priority"], item["due_at_ms"], item["task_id"]))
        state_counts: dict[str, int] = {}
        for item in dispatched_results:
            item["ok"] = item["outcome"] == "SUCCEEDED"
            item["code"] = item.get("error_code")
            state_counts[item["state_after"]] = state_counts.get(item["state_after"], 0) + 1
        attempt_errors = [
            {
                "code": item["error_code"],
                "task_id": item["task_id"],
                "attempt_id": item["attempt_id"],
                "context": {"outcome": item["outcome"], "http_status": item["http_status"]},
            }
            for item in dispatched_results
            if item.get("error_code")
        ]

        finished_at_ms = now_ms()
        skipped.sort(key=lambda item: (item["priority"], item["due_at_ms"], item["task_id"]))
        result = {
            "round_id": round_id,
            "queue": queue_name,
            "status": "COMPLETED",
            "started_at_ms": started_at_ms,
            "finished_at_ms": finished_at_ms,
            "config": effective_config,
            "budget": {
                "time_budget_ms": config.time_budget_ms,
                "elapsed_ms": max(0, finished_at_ms - started_at_ms),
                "deadline_reached": time.monotonic() >= deadline_monotonic,
            },
            "dispatched": dispatched_results,
            "attempt_errors": attempt_errors,
            "error_codes": sorted({item["code"] for item in attempt_errors}),
            "skipped": skipped,
            "waiting_queue": {
                "capacity": config.waiting_queue_capacity,
                "task_ids": [task["task_id"] for task in waiting],
            },
            "counts": {
                "due_candidates": len(candidates),
                "eligible": len(eligible),
                "dispatched": len(dispatched_results),
                "succeeded": sum(
                    1 for item in dispatched_results if item["outcome"] == "SUCCEEDED"
                ),
                "failed": sum(
                    1 for item in dispatched_results if item["outcome"] == "FAILED"
                ),
                "timed_out": sum(
                    1 for item in dispatched_results if item["outcome"] == "TIMEOUT"
                ),
                "retry_waiting": state_counts.get("RETRY_WAITING", 0),
                "dead_lettered": state_counts.get("DEAD_LETTERED", 0),
                "skipped": len(skipped),
                "waiting": len(waiting),
            },
        }
        return result

    async def _execute_prepared_attempt(
        self,
        attempt: dict[str, Any],
        semaphore: asyncio.Semaphore,
        deadline_monotonic: float,
        configured_timeout_ms: int,
        round_id: str,
    ) -> dict[str, Any]:
        try:
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                finalized = await asyncio.to_thread(
                    self.storage.finalize_attempt,
                    attempt["attempt_id"],
                    "ABORTED",
                    None,
                    None,
                    None,
                    "round time budget exhausted after intent commit and before send",
                    now_ms(),
                    self.settings.retry_backoff_base_ms,
                )
                return {**finalized, "priority": attempt["priority"], "due_at_ms": attempt["due_at_ms"]}

            effective_timeout_ms = max(
                1, min(configured_timeout_ms, int(math.floor(remaining * 1_000)))
            )
            timeout_seconds = effective_timeout_ms / 1_000
            payload = {
                "task_id": attempt["task_id"],
                "queue": attempt["queue_name"],
                "round_id": round_id,
                "attempt_no": attempt["attempt_no"],
                "due_at_ms": attempt["due_at_ms"],
                "priority": attempt["priority"],
                "payload": attempt["payload"],
            }
            dispatch_result: DispatchResult = await dispatch_once(
                attempt["endpoint"], payload, timeout_seconds
            )
            detail = dispatch_result.error_detail
            if dispatch_result.outcome == "TIMEOUT":
                detail = (
                    f"independent request timeout {effective_timeout_ms}ms; "
                    f"connection closed by client. {detail or ''}"
                ).strip()
            finalized = await asyncio.to_thread(
                self.storage.finalize_attempt,
                attempt["attempt_id"],
                dispatch_result.outcome,
                dispatch_result.http_status,
                dispatch_result.duration_ms,
                dispatch_result.error_code,
                detail,
                now_ms(),
                self.settings.retry_backoff_base_ms,
            )
            return {**finalized, "priority": attempt["priority"], "due_at_ms": attempt["due_at_ms"]}
        finally:
            semaphore.release()

    def get_round(self, round_id: str) -> dict[str, Any]:
        row = self.storage.get_round(round_id)
        if row is None:
            raise BusinessError(
                E_NOT_FOUND, "round not found", {"round_id": round_id}, 404
            )
        return {**row, "reused": True}

    def get_queue_status(self, queue_name: str) -> dict[str, Any]:
        snapshot = self.storage.queue_snapshot(queue_name)
        consistency = self.storage.verify_consistency()
        snapshot["consistency"] = consistency
        return {"ok": True, **snapshot}

    def get_task_trajectory(self, queue_name: str, task_id: str) -> dict[str, Any]:
        data = self.storage.task_trajectory(queue_name, task_id)
        return {"ok": True, **data}

    def get_recovery_report(self, queue_name: str | None = None) -> dict[str, Any]:
        return {
            "ok": True,
            "report": self.storage.latest_recovery_report(queue_name),
        }

    def get_round_for_queue(self, queue_name: str, round_id: str) -> dict[str, Any]:
        row = self.get_round(round_id)
        if row["queue_name"] != queue_name:
            raise BusinessError(
                E_NOT_FOUND,
                "round not found in queue",
                {"queue": queue_name, "round_id": round_id},
                404,
            )
        return row
