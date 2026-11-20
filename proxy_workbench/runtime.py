"""Concurrent runtime, snapshot publication, forwarding, and diagnostics."""

from __future__ import annotations

import asyncio
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlsplit

from .compiler import CompiledRoute, CompiledState, compile_state, state_summary, utc_now
from .errors import (
    E_CONFIG_CHANGED,
    E_CONFIG_INVALID,
    E_PERSISTENCE,
    E_STALE_BASE,
    E_UPSTREAM_5XX,
    E_UPSTREAM_UNAVAILABLE,
    E_VERSION_ROLLBACK,
    ProxyError,
    config_error,
)
from .health import HealthManager
from .models import (
    IncrementConfig,
    SnapshotConfig,
    apply_increment,
    content_hash,
    parse_config_payload,
    parse_snapshot,
    snapshot_payload,
)
from .store import RecoveryReport, Store
from .transport import UpstreamResponse, filter_hop_by_hop, perform_request


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _failpoint(name: str) -> None:
    if os.environ.get("PROXY_FAILPOINT") == name:
        os._exit(99)


@dataclass(frozen=True)
class ForwardOutcome:
    response: UpstreamResponse
    upstream: str
    route_version: int
    attempts: int


@dataclass(frozen=True)
class UpdateOutcome:
    version: int
    kind: str
    state: CompiledState

    def public(self) -> dict[str, Any]:
        return {
            "ok": True,
            "code": "OK",
            "version": self.version,
            "kind": self.kind,
            "state": state_summary(self.state),
            "invalidation": self.state.invalidation,
        }


class Runtime:
    def __init__(self, *, store: Store, recovery: RecoveryReport):
        self.store = store
        self.recovery = recovery
        self.health = HealthManager()
        self._current: CompiledState | None = None
        self._update_lock = asyncio.Lock()
        self._traces: deque[dict[str, Any]] = deque(maxlen=100)
        self._errors: deque[dict[str, Any]] = deque(maxlen=200)
        self._shutdown: dict[str, Any] | None = None

    @classmethod
    async def create(
        cls,
        db_path: str | os.PathLike[str],
        *,
        seed: dict[str, Any] | None = None,
    ) -> "Runtime":
        store = Store(db_path)
        recovery = store.recover()
        runtime = cls(store=store, recovery=recovery)
        loaded = store.load_complete()
        if loaded is None and seed is not None:
            snapshot = parse_snapshot(seed)
            payload = snapshot_payload(snapshot.version, snapshot.routes)
            intent_id = store.begin_intent(
                kind="snapshot",
                base_version=None,
                target_version=snapshot.version,
                payload=payload,
                detail={"source": "startup_seed", "routes": [route.id for route in snapshot.routes]},
            )
            store.persist_complete(
                intent_id=intent_id,
                version=snapshot.version,
                payload=payload,
                payload_sha256=content_hash(payload),
                detail={"source": "startup_seed", "routes": [route.id for route in snapshot.routes]},
            )
            store.mark_intent_committed(intent_id)
            loaded = store.load_complete()
        if loaded is not None:
            stored_version, payload = loaded
            snapshot = parse_snapshot(payload)
            if snapshot.version != stored_version:
                raise RuntimeError(
                    f"persisted snapshot version mismatch: envelope={snapshot.version}, row={stored_version}"
                )
            runtime._current = compile_state(version=snapshot.version, routes=snapshot.routes, old_state=None)
            for event in store.history(100):
                if event["event"] != "snapshot_committed":
                    continue
                details = event["details"]
                runtime._traces.append(
                    {
                        "event": "config_published_restored",
                        "at": event["created_at"],
                        "kind": details.get("source", "unknown"),
                        "from_version": details.get("from_version"),
                        "to_version": event["version"],
                        "generation": details.get("generation"),
                        "changed_route_ids": details.get("changed_route_ids", []),
                        "changed_fields": details.get("changed_fields", {}),
                        "invalidated_artifacts": details.get("invalidated_artifacts", []),
                        "rebuilt_artifacts": details.get("rebuilt_artifacts", []),
                        "atomic": True,
                        "restored_from_sqlite": True,
                    }
                )
            runtime._traces.appendleft(
                {
                    "event": "startup_snapshot_loaded",
                    "at": utc_now(),
                    **runtime._current.invalidation,
                }
            )

        recovery.previous_process_state = store.begin_process()
        if runtime._current is not None:
            generation = await runtime.health.prepare_update()
            runtime.health.start_plans(runtime._current, generation)
        return runtime

    @property
    def current(self) -> CompiledState | None:
        return self._current

    @property
    def update_lock(self) -> asyncio.Lock:
        return self._update_lock

    @property
    def shutdown_info(self) -> dict[str, Any] | None:
        return self._shutdown

    async def submit(self, payload: Any) -> UpdateOutcome:
        async with self._update_lock:
            current = self._current
            try:
                parsed = parse_config_payload(payload)
            except ProxyError as exc:
                self.record_error(exc, source="config")
                raise

            base_version = current.version if current else None
            if isinstance(parsed, SnapshotConfig):
                kind = "snapshot"
                target_version = parsed.version
                if current is not None and target_version <= current.version:
                    error = ProxyError(
                        E_VERSION_ROLLBACK,
                        "snapshot version must be greater than the current version",
                        409,
                        {"current_version": current.version, "submitted_version": target_version},
                    )
                    self.record_error(error, source="config")
                    raise error
                new_routes = parsed.routes
            elif isinstance(parsed, IncrementConfig):
                kind = "incremental"
                target_version = parsed.version
                if current is None or parsed.base_version != current.version:
                    error = ProxyError(
                        E_STALE_BASE,
                        "incremental base_version is not the current version",
                        409,
                        {
                            "base_version": parsed.base_version,
                            "current_version": current.version if current else None,
                        },
                    )
                    self.record_error(error, source="config")
                    raise error
                if target_version <= current.version:
                    error = ProxyError(
                        E_VERSION_ROLLBACK,
                        "incremental target version must be greater than the current version",
                        409,
                        {"current_version": current.version, "submitted_version": target_version},
                    )
                    self.record_error(error, source="config")
                    raise error
                try:
                    new_routes = apply_increment(
                        (compiled.route for compiled in current.routes.values()), parsed
                    )
                except ProxyError as exc:
                    self.record_error(exc, source="config")
                    raise
            else:  # pragma: no cover - parser invariant
                raise config_error("unsupported configuration object")

            try:
                compiled = compile_state(
                    version=target_version,
                    routes=new_routes,
                    old_state=current,
                )
            except ProxyError as exc:
                self.record_error(exc, source="config")
                raise
            except Exception as exc:
                error = ProxyError(
                    E_CONFIG_INVALID,
                    "configuration compilation failed",
                    400,
                    {"error": type(exc).__name__},
                )
                self.record_error(error, source="config")
                raise error from exc

            normalized = snapshot_payload(target_version, new_routes)
            config_hash = content_hash(normalized)
            intent_id = self.store.begin_intent(
                kind=kind,
                base_version=base_version,
                target_version=target_version,
                payload=normalized,
                detail={
                    "routes": [route.id for route in new_routes],
                    "changed_route_ids": compiled.invalidation["changed_route_ids"],
                    "generation": compiled.generation,
                },
            )
            _failpoint("after_intent")
            try:
                self.store.persist_complete(
                    intent_id=intent_id,
                    version=target_version,
                    payload=normalized,
                    payload_sha256=config_hash,
                    detail={
                        "source": kind,
                        "from_version": base_version,
                        "to_version": target_version,
                        "generation": compiled.generation,
                        "changed_fields": compiled.invalidation["changed_fields"],
                        "changed_route_ids": compiled.invalidation["changed_route_ids"],
                        "invalidated_artifacts": compiled.invalidation["invalidated_artifacts"],
                        "rebuilt_artifacts": compiled.invalidation["rebuilt_artifacts"],
                    },
                )
            except BaseException as exc:
                try:
                    self.store.abort_intent(intent_id, f"persistence failure: {type(exc).__name__}")
                except Exception:
                    pass
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                error = ProxyError(
                    E_PERSISTENCE,
                    "configuration could not be committed durably",
                    500,
                    {"error": type(exc).__name__},
                )
                self.record_error(error, source="config")
                raise error from exc

            # The snapshot is already the durable source of truth. Intent status is
            # bookkeeping and must not make the live process disagree with SQLite.
            try:
                self.store.mark_intent_committed(intent_id)
            except Exception as exc:
                self.record_error(
                    ProxyError(
                        E_PERSISTENCE,
                        "snapshot committed, but intent status update failed; recovery will reconcile it",
                        500,
                        {"intent_id": intent_id, "error": type(exc).__name__},
                    ),
                    source="config",
                )

            generation = await self.health.prepare_update()
            # This single assignment publishes all routes, artifacts, indexes and
            # health plans as one immutable object. No request can observe a mix.
            self._current = compiled
            self.health.start_plans(compiled, generation)
            trace = {
                "event": "config_published",
                "at": utc_now(),
                "kind": kind,
                "intent_id": intent_id,
                **compiled.invalidation,
            }
            self._traces.appendleft(trace)
            return UpdateOutcome(version=target_version, kind=kind, state=compiled)

    def _abort_if_changed(self, bound_state: CompiledState, compiled: CompiledRoute) -> None:
        current = self._current
        if (
            compiled.route.in_flight_policy == "abort"
            and current is not None
            and current.generation != bound_state.generation
        ):
            raise ProxyError(
                E_CONFIG_CHANGED,
                "in-flight request was bound to an older configuration and aborted by policy",
                409,
                {
                    "route_id": compiled.route.id,
                    "bound_version": bound_state.version,
                    "current_version": current.version,
                    "in_flight_policy": "abort",
                },
            )

    def _retry_key(self, error: ProxyError) -> str | None:
        mapping = {
            "E_CONNECT_TIMEOUT": "connect_timeout",
            "E_CONNECTION_REFUSED": "connection_refused",
            "E_READ_TIMEOUT": "read_timeout",
            "E_WRITE_TIMEOUT": "write_timeout",
            E_UPSTREAM_5XX: "5xx",
        }
        return mapping.get(error.code)

    @staticmethod
    def _forward_headers(
        incoming: Iterable[tuple[str, str]], configured: tuple[tuple[str, str], ...]
    ) -> list[tuple[str, str]]:
        filtered = filter_hop_by_hop(incoming, extra_remove={"host", "content-length"})
        configured_names = {name.lower() for name, _ in configured}
        result = [(name, value) for name, value in filtered if name.lower() not in configured_names]
        result.extend(configured)
        return result

    async def forward(
        self,
        *,
        bound_state: CompiledState,
        compiled: CompiledRoute,
        method: str,
        path_query: str,
        headers: Iterable[tuple[str, str]],
        body: bytes,
    ) -> ForwardOutcome:
        route = compiled.route
        request_headers = self._forward_headers(headers, route.headers)
        last_error: ProxyError | None = None
        attempts_used = 0
        for attempt in range(1, route.retries.attempts + 1):
            self._abort_if_changed(bound_state, compiled)
            attempts_used = attempt
            upstream = self.health.choose_candidate(compiled)
            if upstream is None:
                candidates = self.health.candidate_public(compiled)
                error = ProxyError(
                    E_UPSTREAM_UNAVAILABLE,
                    "all eligible upstreams are known to be unavailable",
                    503,
                    {
                        "route_id": route.id,
                        "failure_policy": route.failure_policy.public(),
                        "candidates": candidates,
                        "config_version": bound_state.version,
                    },
                )
                self.record_error(
                    error,
                    route_id=route.id,
                    version=bound_state.version,
                    source="forward",
                    detail={"attempt": attempt},
                )
                raise error
            try:
                response = await perform_request(
                    upstream=upstream,
                    method=method,
                    path_query=path_query,
                    headers=request_headers,
                    body=body,
                    connect_timeout=route.timeout.connect,
                    read_timeout=route.timeout.read,
                )
            except ProxyError as exc:
                self._abort_if_changed(bound_state, compiled)
                exc.details.setdefault("route_id", route.id)
                exc.details.setdefault("config_version", bound_state.version)
                self.health.record_forward_failure(
                    upstream,
                    route_id=route.id,
                    error=exc.code,
                    fail_threshold=route.health.fail_threshold,
                )
                last_error = exc
                retry_key = self._retry_key(exc)
                if attempt < route.retries.attempts and retry_key in route.retries.retry_on:
                    if route.retries.backoff_ms:
                        await asyncio.sleep(route.retries.backoff_ms / 1000.0)
                    continue
                self.record_error(
                    exc,
                    route_id=route.id,
                    version=bound_state.version,
                    source="forward",
                    detail={"upstream": upstream, "attempt": attempt},
                )
                raise

            self._abort_if_changed(bound_state, compiled)
            if response.status >= 500:
                self.health.record_forward_failure(
                    upstream,
                    route_id=route.id,
                    error=f"upstream status {response.status}",
                    fail_threshold=route.health.fail_threshold,
                )
                preview = response.body[:4096].decode("utf-8", errors="replace")
                error = ProxyError(
                    E_UPSTREAM_5XX,
                    "upstream returned a 5xx response",
                    response.status,
                    {
                        "route_id": route.id,
                        "upstream": upstream,
                        "upstream_status": response.status,
                        "upstream_reason": response.reason,
                        "upstream_body": preview,
                        "attempt": attempt,
                        "config_version": bound_state.version,
                    },
                )
                last_error = error
                if attempt < route.retries.attempts and "5xx" in route.retries.retry_on:
                    self.record_error(
                        error,
                        route_id=route.id,
                        version=bound_state.version,
                        source="forward",
                        detail={"upstream": upstream, "attempt": attempt, "retrying": True},
                    )
                    if route.retries.backoff_ms:
                        await asyncio.sleep(route.retries.backoff_ms / 1000.0)
                    continue
                self.record_error(
                    error,
                    route_id=route.id,
                    version=bound_state.version,
                    source="forward",
                    detail={"upstream": upstream, "attempt": attempt},
                )
                raise error

            self.health.record_forward_success(upstream, route_id=route.id)
            return ForwardOutcome(
                response=response,
                upstream=upstream,
                route_version=bound_state.version,
                attempts=attempts_used,
            )
        if last_error is None:
            last_error = ProxyError(E_UPSTREAM_UNAVAILABLE, "no upstream attempt succeeded", 503)
        raise last_error

    def record_error(
        self,
        error: ProxyError,
        *,
        route_id: str | None = None,
        version: int | None = None,
        source: str = "proxy",
        detail: dict[str, Any] | None = None,
    ) -> None:
        self._errors.appendleft(
            {
                "at": _now(),
                "code": error.code,
                "message": error.message,
                "http_status": error.http_status,
                "route_id": route_id or error.details.get("route_id"),
                "config_version": version,
                "source": source,
                "details": {**error.details, **(detail or {})},
            }
        )

    def match_target(
        self,
        state: CompiledState,
        *,
        method: str,
        target: str,
        host: str | None,
    ) -> CompiledRoute | None:
        method = method.upper()
        if target.lower().startswith(("http://", "https://")):
            parts = urlsplit(target)
            if parts.scheme.lower() != "http":
                raise ProxyError(
                    "E_UNSUPPORTED_TARGET",
                    "only absolute-form http forward requests are supported",
                    501,
                    {"target_scheme": parts.scheme},
                )
            if not parts.hostname:
                raise ProxyError(E_CONFIG_INVALID, "forward target has no host", 400)
            port = parts.port or 80
            path = parts.path or "/"
            return state.match_forward(parts.hostname, port, path, method)
        if target.startswith("/"):
            return state.match_reverse(urlsplit(target).path or "/", method)
        raise ProxyError(
            "E_UNSUPPORTED_TARGET",
            "request target must be origin-form or absolute-form http",
            400,
            {"target": target[:128]},
        )

    def diagnostics(self, state: CompiledState | None = None) -> dict[str, Any]:
        state = state or self._current
        current_version = state.version if state else None
        shutdown = self._shutdown or self.store.last_shutdown()
        result: dict[str, Any] = {
            "ok": True,
            "code": "OK",
            "process_state": self.store.current_process_state(),
            "version": current_version,
            "state": state_summary(state) if state else None,
            "recovery": self.recovery.public(),
            "shutdown": shutdown,
            "config": snapshot_payload(state.version, [item.route for item in state.routes.values()])
            if state
            else None,
            "routes": [],
            "artifacts": {},
            "aggregate_indexes": {},
            "health": self.health.public(state),
            "recent_errors": list(self._errors),
            "invalidation_trace": list(self._traces),
            "snapshots": self.store.snapshot_versions(40),
            "history": self.store.history(80),
        }
        if state is None:
            return result
        result["artifacts"] = {
            artifact_id: artifact.public()
            for artifact_id, artifact in sorted(state.artifacts.items())
        }
        result["aggregate_indexes"] = {
            artifact_id: artifact.public()
            for artifact_id, artifact in sorted(state.artifacts.items())
            if artifact.route_id is None
        }
        for route_id, compiled in sorted(state.routes.items()):
            result["routes"].append(
                {
                    **compiled.route.public(),
                    "fingerprint": compiled.fingerprint,
                    "artifacts": {
                        kind: artifact.public()
                        for kind, artifact in sorted(compiled.artifacts.items())
                    },
                    "upstream_health": self.health.candidate_public(compiled),
                }
            )
        return result

    async def shutdown(self, *, mode: str, completed: int, cancelled: int, detail: str) -> dict[str, Any]:
        await self.health.stop()
        self._shutdown = self.store.finish_process(
            mode=mode,
            completed_in_flight=completed,
            cancelled_in_flight=cancelled,
            detail=detail,
        )
        return self._shutdown

