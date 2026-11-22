from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Awaitable, Callable

import httpx
from fastapi import Request
from starlette.responses import Response

from app.compiler import CompiledSnapshot, RoutePlan
from app.db import SQLiteStore
from app.errors import WorkbenchError
from app.fixedpoint import SCALE
from app.http_headers import filter_request_headers, filter_response_headers, merge_headers
from app.models import is_loopback_host
from app.service import ConfigService


JSON_MEDIA_TYPE = "application/json"
ALL_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE"]


@dataclass(frozen=True)
class ProxyResponse:
    status_code: int
    headers: dict[str, str]
    body: bytes

    def as_response(self) -> Response:
        return Response(
            content=self.body,
            status_code=self.status_code,
            headers=self.headers,
        )

    @classmethod
    def from_error(cls, exc: WorkbenchError) -> "ProxyResponse":
        body = json.dumps(exc.payload(), ensure_ascii=False).encode("utf-8")
        headers = {
            "content-type": JSON_MEDIA_TYPE,
            "x-workbench-error": exc.business_code,
        }
        if exc.request_id:
            headers["x-workbench-request-id"] = exc.request_id
        return cls(
            status_code=exc.http_status or 500,
            headers=headers,
            body=body,
        )


class IdempotencyCoordinator:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self._locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()

    async def _lock_for(self, key: str) -> asyncio.Lock:
        async with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    async def execute(
        self,
        key: str,
        fingerprint: str,
        worker: Callable[[], Awaitable[ProxyResponse]],
    ) -> ProxyResponse:
        lock = await self._lock_for(key)
        async with lock:
            existing = await asyncio.to_thread(self.store.get_idempotency, key)
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise WorkbenchError(
                        "E_CONFLICTING_REQUEST",
                        "request identifier was previously used for a different request",
                        details={"request_id": key},
                        request_id=key,
                    )
                if existing["state"] != "COMPLETED":
                    raise WorkbenchError(
                        "E_CONFLICTING_REQUEST",
                        "request identifier has no reusable completed result",
                        details={"request_id": key, "state": existing["state"]},
                        request_id=key,
                    )
                return ProxyResponse(
                    status_code=int(existing["status_code"]),
                    headers=json.loads(existing["headers_json"] or "{}"),
                    body=bytes(existing["body"] or b""),
                )

            begun = await asyncio.to_thread(self.store.begin_idempotency, key, fingerprint)
            if begun.get("state") != "IN_PROGRESS" or begun.get("fingerprint") != fingerprint:
                raise WorkbenchError(
                    "E_CONFLICTING_REQUEST",
                    "request identifier is already in progress",
                    details={"request_id": key, "state": begun.get("state")},
                    request_id=key,
                )

            try:
                result = await worker()
            except WorkbenchError as exc:
                exc.request_id = exc.request_id or key
                result = ProxyResponse.from_error(exc)
                await asyncio.to_thread(
                    self.store.complete_idempotency,
                    key,
                    status_code=result.status_code,
                    headers=result.headers,
                    body=result.body,
                    error_json=json.dumps(exc.payload(), ensure_ascii=False),
                )
                return result
            except asyncio.CancelledError:
                await asyncio.to_thread(
                    self.store.abandon_idempotency, key, "request_cancelled"
                )
                raise
            except Exception as exc:
                await asyncio.to_thread(
                    self.store.abandon_idempotency, key, type(exc).__name__
                )
                raise

            await asyncio.to_thread(
                self.store.complete_idempotency,
                key,
                status_code=result.status_code,
                headers=result.headers,
                body=result.body,
            )
            return result


class ProxyEngine:
    def __init__(self, service: ConfigService, store: SQLiteStore) -> None:
        self.service = service
        self.idempotency = IdempotencyCoordinator(store)

    async def handle_reverse(self, request: Request) -> Response:
        snapshot = self._bind_snapshot()
        plan = snapshot.match_reverse(request.method, request.url.path)
        if plan is None:
            raise WorkbenchError(
                "E_ROUTE_NOT_FOUND",
                "no reverse route matches the request method and path",
                details={
                    "version": snapshot.version,
                    "method": request.method,
                    "path": request.url.path,
                },
            )
        body = await request.body()
        target_url = self._join_url(
            plan.route.upstream, request.url.path, request.url.query
        )
        headers = merge_headers(
            filter_request_headers(request.headers.items()), plan.route.headers
        )
        fingerprint = self._fingerprint(
            "reverse", request.method, plan.route.route_id, target_url, body, headers
        )
        return (
            await self._execute(
                request,
                plan,
                snapshot,
                fingerprint,
                lambda: self._forward(plan, snapshot, request.method, target_url, headers, body, via_egress=False),
            )
        ).as_response()

    async def handle_forward(self, request: Request, forwarded_path: str) -> Response:
        snapshot = self._bind_snapshot()
        host = (request.headers.get("x-forward-host") or "").strip().strip("[]").lower()
        port_text = request.headers.get("x-forward-port") or ""
        scheme = (request.headers.get("x-forward-scheme") or "http").lower()
        if not host or not port_text:
            raise WorkbenchError(
                "E_BAD_REQUEST",
                "forward requests require X-Forward-Host and X-Forward-Port headers",
            )
        try:
            port = int(port_text)
        except ValueError as exc:
            raise WorkbenchError(
                "E_BAD_REQUEST", "X-Forward-Port must be an integer"
            ) from exc
        if not is_loopback_host(host):
            raise WorkbenchError(
                "E_BAD_REQUEST", "forward target host must be loopback"
            )
        if scheme != "http":
            raise WorkbenchError(
                "E_BAD_REQUEST", "local egress supports http targets only"
            )
        plan = snapshot.match_forward(request.method, host, port)
        if plan is None:
            raise WorkbenchError(
                "E_ROUTE_NOT_FOUND",
                "no forward route matches the target host, port and method",
                details={
                    "version": snapshot.version,
                    "method": request.method,
                    "target_host": host,
                    "target_port": port,
                },
            )
        body = await request.body()
        path = "/" + forwarded_path if forwarded_path else "/"
        target_url = self._join_url(
            f"{scheme}://{host}:{port}", path, request.url.query
        )
        headers = merge_headers(
            filter_request_headers(request.headers.items()), plan.route.headers
        )
        fingerprint = self._fingerprint(
            "forward", request.method, plan.route.route_id, target_url, body, headers
        )
        return (
            await self._execute(
                request,
                plan,
                snapshot,
                fingerprint,
                lambda: self._forward(plan, snapshot, request.method, target_url, headers, body, via_egress=True),
            )
        ).as_response()

    def _bind_snapshot(self) -> CompiledSnapshot:
        snapshot = self.service.current
        if snapshot is None:
            raise WorkbenchError(
                "E_ROUTE_NOT_FOUND", "no committed configuration is available"
            )
        return snapshot

    @staticmethod
    def _join_url(origin: str, path: str, query: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        url = origin.rstrip("/") + path
        if query:
            url += "?" + query
        return url

    @staticmethod
    def _fingerprint(
        mode: str,
        method: str,
        route_id: str,
        target_url: str,
        body: bytes,
        headers: dict[str, str],
    ) -> str:
        material = {
            "mode": mode,
            "method": method.upper(),
            "route_id": route_id,
            "target_url": target_url,
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "headers": sorted((name.lower(), value) for name, value in headers.items()),
        }
        return hashlib.sha256(
            json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    async def _execute(
        self,
        request: Request,
        plan: RoutePlan,
        snapshot: CompiledSnapshot,
        fingerprint: str,
        worker: Callable[[], Awaitable[ProxyResponse]],
    ) -> ProxyResponse:
        request_key = request.headers.get("x-request-id") or request.headers.get(
            "idempotency-key"
        )
        if not request_key:
            try:
                return await worker()
            except WorkbenchError as exc:
                return ProxyResponse.from_error(exc)

        try:
            response = await self.idempotency.execute(request_key, fingerprint, worker)
        except WorkbenchError as exc:
            exc.request_id = exc.request_id or request_key
            return ProxyResponse.from_error(exc)
        response.headers.setdefault("x-workbench-request-id", request_key)
        response.headers.setdefault("x-workbench-version", str(snapshot.version))
        response.headers.setdefault("x-workbench-route", plan.route.route_id)
        return response

    async def _forward(
        self,
        plan: RoutePlan,
        snapshot: CompiledSnapshot,
        method: str,
        target_url: str,
        headers: dict[str, str],
        body: bytes,
        *,
        via_egress: bool,
    ) -> ProxyResponse:
        timeout_seconds = plan.route.timeout_ms / 1000.0
        connect_seconds = max(0.001, plan.budget.connect_budget_q6 / SCALE)
        total_seconds = max(0.001, plan.budget.total_wait_q6 / SCALE)
        attempts = plan.route.retries + 1
        timeout = httpx.Timeout(
            connect=connect_seconds,
            read=timeout_seconds,
            write=timeout_seconds,
            pool=connect_seconds,
        )
        client_options: dict[str, object] = {
            "timeout": timeout,
            "trust_env": False,
            "follow_redirects": False,
        }
        if via_egress:
            client_options["proxy"] = plan.route.upstream

        last_timeout: WorkbenchError | None = None
        last_unavailable: WorkbenchError | None = None
        last_response: httpx.Response | None = None
        try:
            async with asyncio.timeout(total_seconds):
                for attempt in range(1, attempts + 1):
                    try:
                        async with httpx.AsyncClient(**client_options) as client:
                            upstream = await client.request(
                                method,
                                target_url,
                                headers=headers,
                                content=body,
                            )
                    except httpx.ConnectTimeout as exc:
                        last_timeout = self._timeout_error(
                            plan,
                            snapshot,
                            target_url,
                            attempt,
                            "connect_timeout",
                            exc,
                        )
                        if attempt < attempts:
                            continue
                        raise last_timeout
                    except httpx.ReadTimeout as exc:
                        last_timeout = self._timeout_error(
                            plan,
                            snapshot,
                            target_url,
                            attempt,
                            "read_timeout",
                            exc,
                        )
                        if attempt < attempts:
                            continue
                        raise last_timeout
                    except httpx.ConnectError as exc:
                        last_unavailable = WorkbenchError(
                            "E_UPSTREAM_UNAVAILABLE",
                            "upstream connection was refused or could not be established",
                            details={
                                "route_id": plan.route.route_id,
                                "version": snapshot.version,
                                "target": target_url,
                                "attempt": attempt,
                                "stage": "connect",
                                "exception_type": type(exc).__name__,
                            },
                        )
                        if attempt < attempts:
                            continue
                        raise last_unavailable
                    except (
                        httpx.RemoteProtocolError,
                        httpx.ReadError,
                        httpx.WriteError,
                        httpx.PoolTimeout,
                    ) as exc:
                        last_unavailable = WorkbenchError(
                            "E_UPSTREAM_UNAVAILABLE",
                            "upstream transport failed",
                            details={
                                "route_id": plan.route.route_id,
                                "version": snapshot.version,
                                "target": target_url,
                                "attempt": attempt,
                                "stage": "transport",
                                "exception_type": type(exc).__name__,
                            },
                        )
                        if attempt < attempts:
                            continue
                        raise last_unavailable

                    last_response = upstream
                    if (
                        upstream.status_code >= 500
                        and method.upper() in {"GET", "HEAD", "OPTIONS"}
                        and attempt < attempts
                    ):
                        continue
                    return self._response_from_upstream(
                        upstream,
                        plan,
                        snapshot,
                        attempt,
                        upstream_5xx=upstream.status_code >= 500,
                    )
        except TimeoutError as exc:
            if last_timeout is not None:
                raise last_timeout from exc
            raise WorkbenchError(
                "E_UPSTREAM_TIMEOUT",
                "total wait budget expired and the connection was cancelled",
                details={
                    "route_id": plan.route.route_id,
                    "version": snapshot.version,
                    "target": target_url,
                    "stage": "total_wait",
                    "total_wait_q6": plan.budget.total_wait_q6,
                    "connect_budget_q6": plan.budget.connect_budget_q6,
                },
            ) from exc

        if last_timeout is not None:
            raise last_timeout
        if last_unavailable is not None:
            raise last_unavailable
        if last_response is not None:
            return self._response_from_upstream(
                last_response,
                plan,
                snapshot,
                attempts,
                upstream_5xx=last_response.status_code >= 500,
            )
        raise WorkbenchError(
            "E_UPSTREAM_UNAVAILABLE",
            "upstream request did not produce a response",
            details={"route_id": plan.route.route_id, "target": target_url},
        )

    @staticmethod
    def _timeout_error(
        plan: RoutePlan,
        snapshot: CompiledSnapshot,
        target_url: str,
        attempt: int,
        stage: str,
        exc: BaseException,
    ) -> WorkbenchError:
        return WorkbenchError(
            "E_UPSTREAM_TIMEOUT",
            "upstream operation exceeded its real timeout budget",
            details={
                "route_id": plan.route.route_id,
                "version": snapshot.version,
                "target": target_url,
                "attempt": attempt,
                "stage": stage,
                "connect_budget_q6": plan.budget.connect_budget_q6,
                "timeout_q6": plan.budget.timeout_q6,
                "exception_type": type(exc).__name__,
            },
        )

    @staticmethod
    def _response_from_upstream(
        upstream: httpx.Response,
        plan: RoutePlan,
        snapshot: CompiledSnapshot,
        attempt: int,
        *,
        upstream_5xx: bool,
    ) -> ProxyResponse:
        headers = filter_response_headers(upstream.headers.items())
        headers["x-workbench-version"] = str(snapshot.version)
        headers["x-workbench-route"] = plan.route.route_id
        headers["x-workbench-attempts"] = str(attempt)
        if upstream_5xx:
            headers["x-workbench-upstream-failure"] = "upstream_5xx"
        return ProxyResponse(
            status_code=upstream.status_code,
            headers=headers,
            body=upstream.content,
        )
