from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .backend_client import ActiveCallRegistry, BackendClient
from .config import GatewayConfig
from .domain import (
    InferenceRequestSpec,
    RequestStatus,
    RoundRequest,
    TERMINAL_STATES,
    normalize_route_document,
    validate_tenant,
)
from .errors import GatewayError
from .store import Store, utc_now


class Scheduler:
    def __init__(self, store: Store, config: GatewayConfig) -> None:
        self.store = store
        self.config = config
        self.active_calls = ActiveCallRegistry()
        self.backend_client = BackendClient(self.active_calls)
        self._decision_lock = threading.RLock()
        self._round_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=max(8, config.global_concurrency_limit * 4),
            thread_name_prefix="inference-dispatch",
        )
        self._closed = False

    def close(self) -> None:
        self._closed = True
        for request_id in self.active_calls.active_ids():
            self.active_calls.cancel(request_id)
        self._executor.shutdown(wait=False, cancel_futures=True)

    def submit_route(self, document: dict[str, Any]) -> dict[str, Any]:
        route, persisted, checksum = normalize_route_document(
            document, validate_loopback=self.config.validate_loopback_endpoints
        )
        return self.store.submit_route(route, persisted, checksum, source_document=document)

    def _current_route(self) -> tuple[Any, dict[str, Any]]:
        raw = self.store.get_current_route()
        if raw is None:
            raise GatewayError(
                "E_ROUTE_UNAVAILABLE",
                "No route table has been committed",
                http_status=409,
            )
        payload = {k: v for k, v in raw.items() if not k.startswith("_")}
        from .domain import RouteTable

        try:
            return RouteTable.model_validate(payload), payload
        except Exception as exc:
            raise GatewayError(
                "E_ROUTE_INVALID",
                f"Persisted current route cannot be loaded: {exc}",
                http_status=500,
            ) from exc

    def submit_request(
        self,
        spec: InferenceRequestSpec,
        *,
        strict_capacity: bool = False,
        wait: bool = False,
        wait_timeout_ms: int = 2_000,
    ) -> dict[str, Any]:
        validate_tenant(spec.tenant_id)
        last_error: GatewayError | None = None
        for _ in range(3):
            route, _payload = self._current_route()
            capacity = self.store.capacity_snapshot(route.version)
            try:
                request, duplicate = self.store.create_request(
                    spec,
                    route,
                    strict_capacity=strict_capacity,
                    capacity=capacity,
                )
            except GatewayError as exc:
                if exc.code == "E_ROUTE_VERSION_CHANGED":
                    last_error = exc
                    continue
                raise
            if duplicate and wait and request["status"] not in {s.value for s in TERMINAL_STATES}:
                request = self.wait_for_terminal(spec.request_id, wait_timeout_ms) or request
            request["duplicate"] = duplicate
            request["admission_capacity"] = capacity
            return request
        assert last_error is not None
        raise last_error

    def submit_batch(
        self,
        specs: list[InferenceRequestSpec],
        *,
        strict_capacity: bool = False,
        wait: bool = False,
        wait_timeout_ms: int = 2_000,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for spec in specs:
            try:
                item = self.submit_request(
                    spec,
                    strict_capacity=strict_capacity,
                    wait=wait,
                    wait_timeout_ms=wait_timeout_ms,
                )
                results.append({"ok": True, "request": item})
            except GatewayError as exc:
                results.append(
                    {
                        "ok": False,
                        "request_id": spec.request_id,
                        "error": exc.as_dict(),
                    }
                )
        accepted = sum(1 for item in results if item["ok"])
        return {
            "ok": True,
            "accepted": accepted,
            "rejected": len(results) - accepted,
            "results": results,
        }

    def wait_for_terminal(
        self, request_id: str, timeout_ms: int
    ) -> dict[str, Any] | None:
        deadline = time.monotonic() + max(0, timeout_ms) / 1000.0
        while True:
            request = self.store.get_request(request_id)
            if request is None:
                return None
            if request["status"] in {s.value for s in TERMINAL_STATES}:
                return request
            if time.monotonic() >= deadline:
                return request
            time.sleep(0.02)

    def _deterministic_candidate_order(
        self, requests: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        usage = self.store.tenant_usage()
        by_priority: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for request in requests:
            by_priority[int(request["priority"])].append(request)

        ordered: list[dict[str, Any]] = []
        for priority in sorted(by_priority, reverse=True):
            group = sorted(by_priority[priority], key=lambda item: int(item["seq"]))
            tenant_queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
            for item in group:
                tenant_queues[str(item["tenant_id"])].append(item)
            local_counts: dict[str, int] = {tenant: 0 for tenant in tenant_queues}
            local_ms: dict[str, int] = {tenant: 0 for tenant in tenant_queues}
            while any(tenant_queues.values()):
                available = [tenant for tenant, queue in tenant_queues.items() if queue]
                available.sort(
                    key=lambda tenant: (
                        int(usage.get(tenant, {}).get("dispatched_count", 0))
                        + local_counts[tenant],
                        int(usage.get(tenant, {}).get("estimated_ms_consumed", 0))
                        + local_ms[tenant],
                        int(usage.get(tenant, {}).get("last_dispatch_seq", 0)),
                        int(tenant_queues[tenant][0]["seq"]),
                        tenant,
                    )
                )
                for tenant in available:
                    if tenant_queues[tenant]:
                        selected = tenant_queues[tenant].popleft()
                        ordered.append(selected)
                        local_counts[tenant] += 1
                        local_ms[tenant] += int(selected["estimated_latency_ms"])
        return ordered

    def run_round(self, options: RoundRequest | None = None) -> dict[str, Any]:
        options = options or RoundRequest()
        if not self._round_lock.acquire(blocking=False):
            raise GatewayError(
                "E_ROUND_IN_PROGRESS",
                "Another scheduler round is already running",
                http_status=409,
            )
        try:
            with self._decision_lock:
                return self._run_round_locked(options)
        finally:
            self._round_lock.release()

    def _run_round_locked(self, options: RoundRequest) -> dict[str, Any]:
        started_wall = time.time()
        started_mono = time.monotonic()
        route, _payload = self._current_route()
        budget_ms = options.time_budget_ms or self.config.round_time_budget_ms
        round_row = self.store.create_round(
            budget_ms, route.version, requested_id=options.round_id
        )
        round_id = int(round_row["id"])
        route_global = route.global_concurrency_limit
        global_limit = self.config.global_concurrency_limit
        if route_global is not None:
            global_limit = min(global_limit, route_global)

        candidates = self._deterministic_candidate_order(self.store.queued_requests())
        rejection_events = self.store.attach_rejections(round_id)
        dispatch_order: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        estimated_used = 0
        dispatch_seq = 0

        for rejection in rejection_events:
            skipped.append(
                {
                    "request_id": rejection["request_id"],
                    "action": "REJECTED",
                    "reason": rejection["reason"],
                    "code": rejection["code"],
                    "detail": rejection["details"],
                }
            )

        for request in candidates:
            if options.max_dispatches is not None and len(dispatch_order) >= options.max_dispatches:
                skipped.append(
                    {
                        "request_id": request["request_id"],
                        "action": "QUEUED",
                        "reason": "MAX_DISPATCHES",
                        "detail": {"max_dispatches": options.max_dispatches},
                    }
                )
                continue
            remaining_budget = max(0, budget_ms - estimated_used)
            result = self.store.try_dispatch(
                request["request_id"],
                round_id=round_id,
                global_limit=global_limit,
                remaining_budget_ms=remaining_budget,
                dispatch_seq=dispatch_seq + 1,
            )
            if result["ok"]:
                dispatch_seq += 1
                dispatched = result["request"]
                estimated_used += int(dispatched["estimated_latency_ms"])
                dispatch_order.append(
                    {
                        "dispatch_seq": dispatch_seq,
                        "request_id": dispatched["request_id"],
                        "tenant_id": dispatched["tenant_id"],
                        "priority": dispatched["priority"],
                        "model_id": dispatched["model_id"],
                        "backend_id": dispatched["backend_id"],
                        "estimated_latency_ms": dispatched["estimated_latency_ms"],
                        "remaining_budget_before_ms": remaining_budget,
                        "vram_reserved_mb": dispatched["vram_reserved_mb"],
                    }
                )
                self._launch_worker(dispatched["request_id"])
                continue

            reason = str(result["reason"])
            detail = result.get("detail") or {}
            if reason == "BACKEND_DEGRADED":
                self.store.fail_backend_degraded(request["request_id"], round_id)
                skipped.append(
                    {
                        "request_id": request["request_id"],
                        "action": "FAILED",
                        "reason": "BACKEND_DEGRADED",
                        "code": "E_BACKEND_DEGRADED",
                        "detail": detail,
                    }
                )
                continue
            if reason == "NOT_QUEUED":
                skipped.append(
                    {
                        "request_id": request["request_id"],
                        "action": "SKIPPED",
                        "reason": reason,
                        "detail": detail,
                    }
                )
                continue
            if reason == "BACKEND_MISSING":
                shed = self.store.shed_request(
                    request["request_id"],
                    reason_code="E_ROUTE_INVALID",
                    details={"reason": reason, **detail},
                    round_id=round_id,
                )
                skipped.append(
                    {
                        "request_id": request["request_id"],
                        "action": "SHED",
                        "reason": reason,
                        "code": "E_ROUTE_INVALID",
                        "detail": detail,
                    }
                )
                continue

            if options.degrade_excess and reason in {
                "TIME_BUDGET",
                "GLOBAL_CONCURRENCY",
                "BACKEND_CONCURRENCY",
                "VRAM_BUDGET",
            }:
                code = (
                    "E_TIME_BUDGET_EXHAUSTED"
                    if reason == "TIME_BUDGET"
                    else "E_SHED_CAPACITY"
                )
                self.store.shed_request(
                    request["request_id"],
                    reason_code=code,
                    details={"reason": reason, **detail},
                    round_id=round_id,
                )
                skipped.append(
                    {
                        "request_id": request["request_id"],
                        "action": "SHED",
                        "reason": reason,
                        "code": code,
                        "detail": detail,
                    }
                )
            else:
                skipped.append(
                    {
                        "request_id": request["request_id"],
                        "action": "QUEUED",
                        "reason": reason,
                        "detail": detail,
                    }
                )

        decision_elapsed_ms = round((time.monotonic() - started_mono) * 1000.0, 3)
        backends = self.store.backend_status(route_version=route.version)
        trace = {
            "round_id": round_id,
            "route_version": route.version,
            "budget_ms": budget_ms,
            "policy": {
                "global_concurrency_limit": global_limit,
                "degrade_excess": options.degrade_excess,
                "max_dispatches": options.max_dispatches,
                "order": "priority_desc,tenant_usage_and_budget_round_robin,submission_seq",
            },
            "started_at": round_row["started_at"],
            "finished_at": utc_now(),
            "dispatch_order": dispatch_order,
            "skipped": skipped,
            "shed_order": [item["request_id"] for item in skipped if item["action"] == "SHED"],
            "failed_order": [item["request_id"] for item in skipped if item["action"] == "FAILED"],
            "backend_snapshot": backends,
            "summary": {
                "candidate_count": len(candidates),
                "dispatched_count": len(dispatch_order),
                "shed_count": sum(1 for item in skipped if item["action"] == "SHED"),
                "failed_count": sum(1 for item in skipped if item["action"] == "FAILED"),
                "rejected_count": sum(1 for item in skipped if item["action"] == "REJECTED"),
                "skipped_count": len(skipped),
                "estimated_used_ms": estimated_used,
                "remaining_budget_ms": max(0, budget_ms - estimated_used),
                "decision_elapsed_ms": decision_elapsed_ms,
                "wall_elapsed_ms": round((time.time() - started_wall) * 1000.0, 3),
            },
        }
        return self.store.finish_round(round_id, trace=trace)

    def _launch_worker(self, request_id: str) -> None:
        if self._closed:
            self.store.complete_dispatch(
                request_id,
                success=False,
                result=None,
                error_code="E_GATEWAY_SHUTDOWN",
                error_detail={"reason": "gateway_closed_before_launch"},
            )
            return
        try:
            self._executor.submit(self._worker, request_id)
        except RuntimeError:
            self.store.complete_dispatch(
                request_id,
                success=False,
                result=None,
                error_code="E_DISPATCH_LAUNCH_FAILED",
                error_detail={"reason": "executor_rejected_task"},
            )

    def _worker(self, request_id: str) -> None:
        try:
            request = self.store.get_request(request_id)
            if request is None or request["status"] != RequestStatus.DISPATCHED.value:
                return
            if request["cancel_requested"]:
                self._complete(
                    request_id,
                    success=False,
                    result=None,
                    error_code="E_REQUEST_CANCELLED",
                    error_detail={"reason": "cancel_requested_before_dispatch"},
                    timed_out=False,
                    cancelled=True,
                )
                return
            payload = {
                "request_id": request_id,
                "model_id": request["model_id"],
                "sample_count": request["sample_count"],
                "tenant_id": request["tenant_id"],
                "priority": request["priority"],
                "fault_mode": request["fault_mode"],
            }
            outcome = self.backend_client.infer(
                endpoint=request["endpoint"],
                timeout_ms=int(request["latency_cap_ms"]),
                request_id=request_id,
                tenant_id=str(request["tenant_id"]),
                payload=payload,
                fault_mode=request["fault_mode"],
                cancelled=bool(request["cancel_requested"]),
            )
            latest = self.store.get_request(request_id)
            cancelled = bool(latest and latest["cancel_requested"])
            if cancelled:
                self._complete(
                    request_id,
                    success=False,
                    result=None,
                    error_code="E_REQUEST_CANCELLED",
                    error_detail={"reason": "client_cancelled_during_dispatch"},
                    timed_out=False,
                    cancelled=True,
                )
            elif outcome.success:
                self._complete(
                    request_id,
                    success=True,
                    result=outcome.result,
                    error_code=None,
                    error_detail=None,
                    timed_out=False,
                    cancelled=False,
                )
            else:
                self._complete(
                    request_id,
                    success=False,
                    result=None,
                    error_code=outcome.error_code or "E_BACKEND_FAILURE",
                    error_detail=outcome.error_detail,
                    timed_out=outcome.timed_out,
                    cancelled=outcome.cancelled,
                )
        except Exception as exc:
            try:
                self._complete(
                    request_id,
                    success=False,
                    result=None,
                    error_code="E_INTERNAL",
                    error_detail={"reason": "worker_exception", "exception": type(exc).__name__},
                    timed_out=False,
                    cancelled=False,
                )
            except Exception:
                pass

    def _complete(
        self,
        request_id: str,
        *,
        success: bool,
        result: dict[str, Any] | None,
        error_code: str | None,
        error_detail: dict[str, Any] | None,
        timed_out: bool,
        cancelled: bool,
    ) -> None:
        with self._decision_lock:
            self.store.complete_dispatch(
                request_id,
                success=success,
                result=result,
                error_code=error_code,
                error_detail=error_detail,
                timed_out=timed_out,
            )

    def cancel_request(self, request_id: str, reason: str) -> dict[str, Any] | None:
        request, needs_close = self.store.request_cancel(request_id, reason)
        if request is None:
            return None
        if needs_close:
            self.active_calls.cancel(request_id)
            # Closing the socket is the real cancellation mechanism.  Allow the
            # owning worker to commit the release, but do not pretend a hung
            # backend thread was forcibly killed.
            request = self.wait_for_terminal(request_id, 1_500) or request
        return request

    def backend_status(self, *, include_retired: bool = False, route_version: int | None = None) -> dict[str, Any]:
        current = self.store.get_current_route()
        if route_version is None and not include_retired and current is not None:
            route_version = int(current["version"])
        return {
            "route_version": route_version,
            "include_retired": include_retired,
            "backends": self.store.backend_status(
                route_version=route_version, include_retired=include_retired
            ),
            "active_http_connections": self.active_calls.active_ids(),
        }

