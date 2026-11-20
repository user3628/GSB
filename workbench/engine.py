from __future__ import annotations

import asyncio
import base64
import copy
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .canonical import CacheKey, build_cache_key, normalize_path
from .config import WorkbenchConfig
from .errors import WorkbenchError, wb_error
from .injection import (
    InjectionPlan,
    build_injection_plan,
    normalize_injection_specs,
    query_control_values,
    record_plan,
    record_rejected_injection,
)
from .origin import OriginClient, OriginResponse
from .policy import CachePolicy
from .store import Store


@dataclass(slots=True)
class RequestSpec:
    request_id: str
    node_id: str
    method: str
    path: str
    query: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    reserved_controls: dict[str, list[str]] = field(default_factory=dict)
    injections: Any = None
    force_revalidate: bool = False
    request_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FetchResult:
    entry: dict[str, Any]
    version_changed: bool
    old_etag: str | None
    new_etag: str
    origin_fetches: int
    duration_ms: float
    intent_id: str


class Trace:
    def __init__(self, *, request_id: str, node_id: str, cache_key: str | None = None):
        self.request_id = request_id
        self.node_id = node_id
        self.cache_key = cache_key
        self.origin_fetches = 0
        self.events: list[dict[str, Any]] = []

    def add(self, phase: str, detail: str, **fields: Any) -> None:
        event = {"at": time.time(), "phase": phase, "detail": detail}
        event.update({key: value for key, value in fields.items() if value is not None})
        self.events.append(event)

    def as_dict(self, *, decision: str, origin_fetches: int) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "node_id": self.node_id,
            "cache_key": self.cache_key,
            "decision": decision,
            "origin_fetches": origin_fetches,
            "events": self.events,
        }


def _body_for_http(body: bytes) -> tuple[str, str]:
    try:
        return body.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return base64.b64encode(body).decode("ascii"), "base64"


class CacheEngine:
    def __init__(self, store: Store, config: WorkbenchConfig, origin: OriginClient | None = None):
        self.store = store
        self.config = config
        self.origin = origin or OriginClient(config)
        self._flights: dict[tuple[str, str], asyncio.Future[tuple[str, Any]]] = {}
        self._request_futures: dict[str, asyncio.Future[tuple[dict[str, Any], int, str | None]]] = {}
        self._background: dict[str, asyncio.Task[Any]] = {}
        self._background_status: dict[str, dict[str, Any]] = {}

    def validate_node(self, node_id: str) -> None:
        if node_id not in self.config.edge_nodes:
            raise wb_error("E_NODE_NOT_FOUND", "Unknown edge node", node_id=node_id)

    def _policy_for_node(self, node_id: str) -> CachePolicy:
        policy = self.store.get_current_policy()
        if policy is None:
            raise wb_error("E_POLICY_MISSING", "No cache policy snapshot has been committed")
        if not policy.applies_to(node_id):
            raise wb_error(
                "E_NODE_NOT_COVERED",
                "Current policy snapshot does not apply to this edge node",
                node_id=node_id,
                policy_nodes=list(policy.nodes),
            )
        return policy

    async def submit(
        self,
        spec: RequestSpec,
        *,
        fingerprint: str,
        request_payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], int]:
        self.validate_node(spec.node_id)
        claim, existing = self.store.claim_request(
            request_id=spec.request_id,
            node_id=spec.node_id,
            fingerprint=fingerprint,
            request_payload=request_payload,
        )
        if claim == "EXISTING":
            if existing.get("response") is not None:
                return self._replay_response(existing)
            if existing.get("status") == "IN_PROGRESS":
                future = self._request_futures.get(spec.request_id)
                if future is None:
                    raise wb_error(
                        "E_REQUEST_INCOMPLETE",
                        "An unfinished request from a previous process cannot be re-executed",
                        request_id=spec.request_id,
                    )
                response, status, _code = await asyncio.shield(future)
                self.store.record_trace(
                    request_id=spec.request_id,
                    node_id=spec.node_id,
                    cache_key=response.get("cache_key"),
                    decision="DEDUPLICATED_IN_FLIGHT",
                    origin_fetches=0,
                    events=[
                        {
                            "at": time.time(),
                            "phase": "DEDUPE",
                            "detail": "Identical request joined the original in-flight execution",
                        }
                    ],
                )
                return self._mark_replayed(response), status
            raise wb_error(
                "E_REQUEST_INCOMPLETE",
                "Request was interrupted before completion and will not be executed again",
                request_id=spec.request_id,
                status=existing.get("status"),
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[dict[str, Any], int, str | None]] = loop.create_future()
        self._request_futures[spec.request_id] = future
        try:
            response, status, code = await self._process_and_record(spec)
            future.set_result((response, status, code))
            return response, status
        except BaseException as exc:
            error = wb_error("E_INTERNAL", "Request processing failed", error_type=type(exc).__name__)
            response = error.to_dict()
            response.update({"request_id": spec.request_id, "node_id": spec.node_id, "http_status": error.http_status})
            trace_id = self.store.record_trace(
                request_id=spec.request_id,
                node_id=spec.node_id,
                cache_key=None,
                decision="INTERNAL_ERROR",
                origin_fetches=0,
                events=[
                    {
                        "at": time.time(),
                        "phase": "ERROR",
                        "detail": "Unhandled request-processing exception",
                        "business_code": error.code,
                        "error_type": type(exc).__name__,
                    }
                ],
            )
            response["trace"] = {
                "request_id": spec.request_id,
                "node_id": spec.node_id,
                "cache_key": None,
                "decision": "INTERNAL_ERROR",
                "origin_fetches": 0,
                "events": [{"at": time.time(), "phase": "ERROR", "detail": "Unhandled request-processing exception"}],
            }
            response["trace_id"] = trace_id
            response["origin_fetches"] = 0
            try:
                self.store.complete_request(
                    spec.request_id,
                    response=response,
                    http_status=error.http_status,
                    business_code=error.code,
                    status="FAILED",
                )
            finally:
                future.set_result((response, error.http_status, error.code))
            return response, error.http_status
        finally:
            self._request_futures.pop(spec.request_id, None)

    def _mark_replayed(self, response: Mapping[str, Any]) -> dict[str, Any]:
        replayed = copy.deepcopy(dict(response))
        replayed["replayed"] = True
        replayed["original_origin_fetches"] = int(replayed.get("origin_fetches", 0) or 0)
        replayed["origin_fetches"] = 0
        trace = replayed.get("trace")
        if isinstance(trace, dict):
            trace["reused_origin_fetches"] = int(trace.get("origin_fetches", 0) or 0)
            trace["origin_fetches"] = 0
            trace.setdefault("events", []).append(
                {
                    "at": time.time(),
                    "phase": "DEDUPE",
                    "detail": "Persisted result reused; cache engine was not invoked again",
                    "origin_fetches": 0,
                }
            )
        return replayed

    def _replay_response(self, existing: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
        response = self._mark_replayed(existing["response"])
        status = int(existing.get("http_status") or response.get("http_status") or 200)
        self.store.record_trace(
            request_id=str(existing["request_id"]),
            node_id=str(existing["node_id"]),
            cache_key=response.get("cache_key"),
            decision="DEDUPLICATED_REPLAY",
            origin_fetches=0,
            events=[
                {
                    "at": time.time(),
                    "phase": "DEDUPE",
                    "detail": "request_id already completed; persisted response reused",
                }
            ],
        )
        return response, status

    async def _process_and_record(
        self, spec: RequestSpec
    ) -> tuple[dict[str, Any], int, str | None]:
        trace = Trace(request_id=spec.request_id, node_id=spec.node_id)
        decision = "ERROR"
        origin_fetches = 0
        business_code: str | None = None
        response: dict[str, Any] | None = None
        http_status = 500
        try:
            response, decision, origin_fetches = await self._execute(spec, trace)
            http_status = int(response.get("http_status", 200))
            return response, http_status, None
        except WorkbenchError as exc:
            business_code = exc.code
            response = exc.to_dict()
            response.update(
                {
                    "request_id": spec.request_id,
                    "node_id": spec.node_id,
                    "http_status": exc.http_status,
                }
            )
            trace.add(
                "ERROR",
                exc.message,
                business_code=exc.code,
                details=exc.details,
            )
            http_status = exc.http_status
            return response, exc.http_status, business_code
        finally:
            origin_fetches = max(origin_fetches, trace.origin_fetches)
            trace_payload = trace.as_dict(decision=decision, origin_fetches=origin_fetches)
            trace_id = self.store.record_trace(
                request_id=spec.request_id,
                node_id=spec.node_id,
                cache_key=trace.cache_key,
                decision=decision,
                origin_fetches=origin_fetches,
                events=trace.events,
            )
            if response is not None:
                response["trace"] = trace_payload
                response["trace_id"] = trace_id
                response["origin_fetches"] = origin_fetches
                self.store.complete_request(
                    spec.request_id,
                    response=response,
                    http_status=http_status,
                    business_code=business_code,
                    status="FAILED" if business_code else "COMPLETED",
                )

    async def _execute(
        self, spec: RequestSpec, trace: Trace
    ) -> tuple[dict[str, Any], str, int]:
        if spec.method.upper() not in {"GET", "HEAD"}:
            raise wb_error("E_METHOD_NOT_ALLOWED", "Only GET and HEAD are cacheable in this workbench")
        try:
            controls = query_control_values(spec.reserved_controls)
        except WorkbenchError as exc:
            record_rejected_injection(
                self.store,
                exc,
                request_id=spec.request_id,
                node_id=spec.node_id,
                route=spec.path,
                raw_specs=[{"type": "query_control", **spec.reserved_controls}],
            )
            raise
        raw_specs: list[Mapping[str, Any]] = []
        try:
            raw_specs = normalize_injection_specs(controls=controls, injections=spec.injections)
            plan = build_injection_plan(
                raw_specs,
                node_id=spec.node_id,
                route=spec.path,
                context="request",
                max_body_pad_bytes=self.config.max_body_pad_bytes,
                default_timeout_ms=self.config.origin_timeout_ms,
            )
            record_plan(self.store, plan, request_id=spec.request_id, node_id=spec.node_id, route=spec.path)
        except WorkbenchError as exc:
            record_rejected_injection(
                self.store,
                exc,
                request_id=spec.request_id,
                node_id=spec.node_id,
                route=spec.path,
                raw_specs=raw_specs or [{"type": "invalid_injection"}],
            )
            raise
        if controls.get("request_id") and controls["request_id"] != spec.request_id:
            raise wb_error(
                "E_INJECT_INVALID",
                "Reserved request id conflicts with the request envelope",
                control_request_id=controls["request_id"],
                request_id=spec.request_id,
            )
        force_revalidate = bool(spec.force_revalidate or controls.get("force_revalidate"))
        if plan.any:
            trace.add("INJECTION", "Injection plan accepted", injections=plan.as_dict())

        policy = self._policy_for_node(spec.node_id)
        trace.add(
            "POLICY",
            "Policy snapshot selected",
            policy_version=policy.version,
            policy_digest=policy.digest,
        )
        try:
            key = build_cache_key(spec.path, spec.query, spec.headers, policy.vary)
        except WorkbenchError:
            trace.add("CACHE_KEY", "Canonicalization failed; no cache entry may be written")
            raise
        trace.cache_key = key.digest
        trace.add(
            "CACHE_KEY",
            "Canonical cache key built",
            cache_key=key.digest,
            key_canonical=key.canonical,
            vary=key.vary,
        )
        payload, body_encoding, decision, origin_fetches, entry, background = await self._handle_lookup(
            spec=spec,
            trace=trace,
            policy=policy,
            key=key,
            plan=plan,
            force_revalidate=force_revalidate,
        )
        response = self._response_envelope(
            spec=spec,
            policy=policy,
            key=key,
            entry=entry,
            payload=payload,
            body_encoding=body_encoding,
            cache_status=decision,
            origin_fetches=origin_fetches,
            now=time.time(),
            background=background,
        )
        return response, decision, origin_fetches

    async def _handle_lookup(
        self,
        *,
        spec: RequestSpec,
        trace: Trace,
        policy: CachePolicy,
        key: CacheKey,
        plan: InjectionPlan,
        force_revalidate: bool,
    ) -> tuple[bytes, str, str, int, dict[str, Any], dict[str, Any] | None]:
        now = time.time()
        entry = self.store.get_entry(spec.node_id, key.digest)
        if entry is not None and (entry["state"] == "PURGED" or entry["policy_version"] != policy.version):
            trace.add(
                "CACHE_LOOKUP",
                "Entry is not usable under the current policy",
                existing_state=entry["state"],
                entry_policy_version=entry["policy_version"],
                policy_version=policy.version,
            )
            entry = None
        if entry is None:
            trace.add("CACHE_LOOKUP", "No active cache entry", from_state="MISS")
            result, leader = await self._fetch_coalesced(
                spec=spec, trace=trace, policy=policy, key=key, plan=plan, old_entry=None
            )
            decision = "MISS" if leader else "REVALIDATING"
            fetches = result.origin_fetches if leader else 0
            return result.entry["body"], self._encoding(result.entry), decision, fetches, result.entry, None

        state = entry["state"]
        trace.add(
            "CACHE_LOOKUP",
            "Cache entry found",
            from_state=state,
            generation=entry["generation"],
            etag=entry["etag"],
            policy_version=entry["policy_version"],
        )
        if force_revalidate:
            trace.add("REVALIDATE", "Freshness bypass requested", from_state=state, to_state="REVALIDATING")
            result, leader = await self._fetch_coalesced(
                spec=spec, trace=trace, policy=policy, key=key, plan=plan, old_entry=entry
            )
            decision = "REVALIDATED" if leader else "REVALIDATING"
            fetches = result.origin_fetches if leader else 0
            return result.entry["body"], self._encoding(result.entry), decision, fetches, result.entry, None

        if state == "FRESH" and now < entry["fresh_until"]:
            trace.add("CACHE_HIT", "Entry is fresh", state="FRESH", age_seconds=now - entry["stored_at"])
            return entry["body"], self._encoding(entry), "FRESH", 0, entry, None

        if state == "FRESH" and now >= entry["fresh_until"]:
            entry = self._force_stale(entry, now)
            state = entry["state"]
            trace.add("STATE", "TTL elapsed; entry is stale", from_state="FRESH", to_state="STALE")

        if state == "STALE" and now < entry["stale_until"]:
            background = self._start_background_revalidation(
                spec=spec,
                policy=policy,
                key=key,
                plan=plan,
                old_entry=entry,
            )
            trace.add(
                "STALE_SERVED",
                "Within stale-while-revalidate window; request is decoupled from background origin fetch",
                state="STALE",
                background_task_id=background["task_id"],
            )
            return entry["body"], self._encoding(entry), "STALE", 0, entry, background

        if state == "REVALIDATING" and now < entry["stale_until"]:
            background = self._start_background_revalidation(
                spec=spec,
                policy=policy,
                key=key,
                plan=plan,
                old_entry=entry,
            )
            trace.add(
                "STALE_SERVED",
                "Revalidation is already in progress; stale bytes are served without waiting",
                state="REVALIDATING",
                background_task_id=background["task_id"],
            )
            return entry["body"], self._encoding(entry), "STALE", 0, entry, background

        if state == "REVALIDATING":
            flight = self._flights.get((spec.node_id, key.digest))
            if flight is not None:
                result = await self._await_flight(flight, trace)
                return result.entry["body"], self._encoding(result.entry), "REVALIDATING", 0, result.entry, None
            try:
                entry = self.store.transition_entry(
                    node_id=spec.node_id,
                    cache_key=key.digest,
                    from_state="REVALIDATING",
                    to_state="STALE",
                    reason="ORPHAN_REVALIDATION",
                )
                trace.add("STATE", "Recovered orphaned revalidation marker", from_state="REVALIDATING", to_state="STALE")
            except WorkbenchError:
                entry = self.store.get_entry(spec.node_id, key.digest)

        trace.add("REVALIDATE", "Entry cannot be served; synchronous origin validation required", from_state=state)
        result, leader = await self._fetch_coalesced(
            spec=spec, trace=trace, policy=policy, key=key, plan=plan, old_entry=entry
        )
        decision = "REVALIDATED" if leader else "REVALIDATING"
        fetches = result.origin_fetches if leader else 0
        return result.entry["body"], self._encoding(result.entry), decision, fetches, result.entry, None

    @staticmethod
    def _encoding(entry: Mapping[str, Any]) -> str:
        _, encoding = _body_for_http(bytes(entry["body"]))
        return encoding

    def _force_stale(self, entry: dict[str, Any], now: float) -> dict[str, Any]:
        try:
            return self.store.transition_entry(
                node_id=entry["node_id"],
                cache_key=entry["cache_key"],
                from_state="FRESH",
                to_state="STALE",
                reason="TTL_EXPIRED_READ",
                now=now,
            )
        except WorkbenchError:
            current = self.store.get_entry(entry["node_id"], entry["cache_key"])
            return current or entry

    async def _fetch_coalesced(
        self,
        *,
        spec: RequestSpec,
        trace: Trace,
        policy: CachePolicy,
        key: CacheKey,
        plan: InjectionPlan,
        old_entry: dict[str, Any] | None,
    ) -> tuple[FetchResult, bool]:
        flight_key = (spec.node_id, key.digest)
        existing = self._flights.get(flight_key)
        if existing is not None:
            trace.add(
                "COALESCE",
                "Joined an existing origin flight; this request will not contact origin",
                flight_key=f"{spec.node_id}:{key.digest}",
            )
            result = await self._await_flight(existing, trace)
            return result, False

        loop = asyncio.get_running_loop()
        flight: asyncio.Future[tuple[str, Any]] = loop.create_future()
        self._flights[flight_key] = flight
        try:
            try:
                result = await self._fetch_origin_and_commit(
                    spec=spec,
                    trace=trace,
                    policy=policy,
                    key=key,
                    plan=plan,
                    old_entry=old_entry,
                )
            except WorkbenchError as exc:
                flight.set_result(("error", exc))
                raise
            except BaseException as exc:
                flight.set_result(("error", exc))
                raise
            flight.set_result(("ok", result))
            return result, True
        finally:
            if self._flights.get(flight_key) is flight:
                self._flights.pop(flight_key, None)

    async def _await_flight(
        self, flight: asyncio.Future[tuple[str, Any]], trace: Trace
    ) -> FetchResult:
        outcome, value = await asyncio.shield(flight)
        if outcome == "error":
            if isinstance(value, WorkbenchError):
                raise value
            raise wb_error("E_INTERNAL", "The coalesced origin flight failed", error_type=type(value).__name__)
        trace.add("COALESCE", "Reused the leader's completed origin result", origin_fetches=0)
        assert isinstance(value, FetchResult)
        return value

    async def _fetch_origin_and_commit(
        self,
        *,
        spec: RequestSpec,
        trace: Trace,
        policy: CachePolicy,
        key: CacheKey,
        plan: InjectionPlan,
        old_entry: dict[str, Any] | None,
    ) -> FetchResult:
        restore_state: str | None = None
        if old_entry is not None:
            current = self.store.get_entry(spec.node_id, key.digest)
            if current is None or current["state"] == "PURGED":
                old_entry = None
            else:
                old_entry = current
                restore_state = current["state"]
                try:
                    self.store.transition_entry(
                        node_id=spec.node_id,
                        cache_key=key.digest,
                        from_state=current["state"],
                        to_state="REVALIDATING",
                        reason="ORIGIN_FETCH_STARTED",
                    )
                    trace.add(
                        "STATE",
                        "Entry marked revalidating before origin intent",
                        from_state=current["state"],
                        to_state="REVALIDATING",
                    )
                except WorkbenchError:
                    refreshed = self.store.get_entry(spec.node_id, key.digest)
                    old_entry = None if refreshed is None or refreshed["state"] == "PURGED" else refreshed
        else:
            trace.add("STATE", "New entry generation starts with a miss", from_state="MISS", to_state="REVALIDATING")

        epoch = self.store.node_epoch(spec.node_id)
        intent_payload = {
            "path": key.path,
            "query": key.query,
            "vary": key.vary,
            "policy_version": policy.version,
            "old_etag": old_entry["etag"] if old_entry else None,
            "observed_node_epoch": epoch,
            "injections": plan.as_dict(),
        }
        intent_id = self.store.create_intent(
            kind="ORIGIN_FETCH",
            request_id=spec.request_id,
            node_id=spec.node_id,
            cache_key=key.digest,
            payload=intent_payload,
        )
        trace.add("INTENT", "Origin fetch intent persisted before network access", intent_id=intent_id)
        attempt_id = str(uuid.uuid4())
        self.store.begin_origin_fetch(
            attempt_id=attempt_id,
            request_id=spec.request_id,
            node_id=spec.node_id,
            path=key.path,
        )
        trace.add("ORIGIN", "Loopback origin request started", attempt_id=attempt_id, origin_url=self.config.refused_origin_url if plan.connection_refused else self.config.effective_origin_base_url)
        trace.origin_fetches += 1
        try:
            origin_response = await self.origin.fetch(key.path, plan)
        except WorkbenchError as exc:
            status = {
                "E_ORIGIN_TIMEOUT": "TIMEOUT",
                "E_ORIGIN_5XX": "HTTP_5XX",
                "E_ORIGIN_CONNECT": "CONNECT_FAILED",
            }.get(exc.code, "FAILED")
            self.store.finish_origin_fetch(
                attempt_id,
                status=status,
                http_status=exc.details.get("origin_status"),
                error_code=exc.code,
            )
            self.store.rollback_intent(intent_id, error=exc.code)
            self._restore_after_failure(spec.node_id, key.digest, restore_state)
            trace.add(
                "ORIGIN_ERROR",
                exc.message,
                business_code=exc.code,
                intent_id=intent_id,
                attempt_id=attempt_id,
                details=exc.details,
            )
            raise
        trace.add(
            "ORIGIN",
            "Origin response received",
            attempt_id=attempt_id,
            etag=origin_response.etag,
            content_version=origin_response.version,
            body_length=len(origin_response.body),
            duration_ms=origin_response.duration_ms,
        )
        try:
            result = self._commit_origin_response(
                spec=spec,
                trace=trace,
                policy=policy,
                key=key,
                old_entry=old_entry,
                origin_response=origin_response,
                intent_id=intent_id,
                epoch=epoch,
            )
        except BaseException:
            self.store.finish_origin_fetch(
                attempt_id,
                status="RECEIVED_NOT_COMMITTED",
                http_status=origin_response.http_status,
                etag=origin_response.etag,
                body_length=len(origin_response.body),
            )
            raise
        self.store.finish_origin_fetch(
            attempt_id,
            status="COMMITTED",
            http_status=origin_response.http_status,
            etag=origin_response.etag,
            body_length=len(origin_response.body),
        )
        return result

    def _restore_after_failure(self, node_id: str, cache_key: str, restore_state: str | None) -> None:
        current = self.store.get_entry(node_id, cache_key)
        if current is None or current["state"] != "REVALIDATING":
            return
        now = time.time()
        target = "FRESH" if now < current["fresh_until"] else "STALE"
        try:
            self.store.transition_entry(
                node_id=node_id,
                cache_key=cache_key,
                from_state="REVALIDATING",
                to_state=target,
                reason="ORIGIN_FETCH_FAILED_KEPT_LAST_COMPLETE_VERSION",
            )
        except WorkbenchError:
            pass

    def _commit_origin_response(
        self,
        *,
        spec: RequestSpec,
        trace: Trace,
        policy: CachePolicy,
        key: CacheKey,
        old_entry: dict[str, Any] | None,
        origin_response: OriginResponse,
        intent_id: str,
        epoch: int,
    ) -> FetchResult:
        now = time.time()
        tags = list(dict.fromkeys([*origin_response.tags, *policy.default_tags]))
        entry = {
            "cache_key": key.digest,
            "key_canonical": key.canonical,
            "canonical_path": key.path,
            "canonical_query": key.query,
            "vary": key.vary,
            "body": origin_response.body,
            "content_type": origin_response.content_type,
            "etag": origin_response.etag,
            "origin_version": origin_response.version,
            "stored_at": now,
            "fresh_until": now + policy.ttl_seconds,
            "stale_until": now + policy.ttl_seconds + policy.stale_while_revalidate_seconds,
            "policy_version": policy.version,
            "tags": tags,
            "route_key": key.path,
        }
        committed = self.store.replace_entry(
            node_id=spec.node_id,
            entry=entry,
            intent_id=intent_id,
            expected_epoch=epoch,
            source_request_id=spec.request_id,
        )
        trace.add(
            "ENTRY_COMMIT",
            "Origin version committed by deleting and recreating the complete entry",
            intent_id=intent_id,
            generation=committed["generation"],
            version_changed=committed["version_changed"],
            old_etag=committed.get("old_etag"),
            new_etag=committed["new_etag"],
        )
        return FetchResult(
            entry=committed["entry"],
            version_changed=bool(committed["version_changed"]),
            old_etag=committed.get("old_etag"),
            new_etag=committed["new_etag"],
            origin_fetches=1,
            duration_ms=origin_response.duration_ms,
            intent_id=intent_id,
        )

    def _start_background_revalidation(
        self,
        *,
        spec: RequestSpec,
        policy: CachePolicy,
        key: CacheKey,
        plan: InjectionPlan,
        old_entry: dict[str, Any],
    ) -> dict[str, Any]:
        flight_key = (spec.node_id, key.digest)
        existing_task = next(
            (task_id for task_id, status in self._background_status.items() if status.get("flight_key") == flight_key and status.get("status") in {"RUNNING", "STARTING"}),
            None,
        )
        if existing_task is not None:
            return {"task_id": existing_task, "decoupled": True, "already_running": True}
        task_id = f"bg-{uuid.uuid4()}"
        self._background_status[task_id] = {
            "task_id": task_id,
            "flight_key": flight_key,
            "node_id": spec.node_id,
            "cache_key": key.digest,
            "status": "STARTING",
            "started_at": time.time(),
            "decoupled_from_response": True,
        }
        task = asyncio.create_task(
            self._background_revalidate(
                task_id=task_id,
                spec=spec,
                policy=policy,
                key=key,
                plan=plan,
                old_entry=old_entry,
            )
        )
        self._background[task_id] = task
        task.add_done_callback(lambda _task: self._background.pop(task_id, None))
        return {"task_id": task_id, "decoupled": True, "already_running": False}

    async def _background_revalidate(
        self,
        *,
        task_id: str,
        spec: RequestSpec,
        policy: CachePolicy,
        key: CacheKey,
        plan: InjectionPlan,
        old_entry: dict[str, Any],
    ) -> None:
        trace = Trace(request_id=f"{spec.request_id}:bg:{task_id[-8:]}", node_id=spec.node_id, cache_key=key.digest)
        trace.add("BACKGROUND", "Stale-while-revalidate background task started", task_id=task_id)
        self._background_status[task_id]["status"] = "RUNNING"
        decision = "BACKGROUND_REVALIDATED"
        origin_fetches = 0
        try:
            current = self.store.get_entry(spec.node_id, key.digest)
            if current is None or current["state"] == "PURGED":
                trace.add("BACKGROUND", "Entry disappeared or was purged before background commit")
                decision = "BACKGROUND_SKIPPED"
                return
            result, leader = await self._fetch_coalesced(
                spec=RequestSpec(
                    request_id=trace.request_id,
                    node_id=spec.node_id,
                    method="GET",
                    path=spec.path,
                    query=spec.query,
                    headers=spec.headers,
                ),
                trace=trace,
                policy=policy,
                key=key,
                plan=plan,
                old_entry=current,
            )
            origin_fetches = result.origin_fetches
            self._background_status[task_id].update(
                {
                    "status": "COMPLETED",
                    "origin_fetches": origin_fetches,
                    "new_etag": result.new_etag,
                    "version_changed": result.version_changed,
                    "completed_at": time.time(),
                }
            )
            trace.add("BACKGROUND", "Background revalidation completed", version_changed=result.version_changed)
        except WorkbenchError as exc:
            self._background_status[task_id].update(
                {
                    "status": "FAILED",
                    "business_code": exc.code,
                    "completed_at": time.time(),
                }
            )
            decision = "BACKGROUND_FAILED"
            trace.add("BACKGROUND", exc.message, business_code=exc.code)
        except BaseException as exc:
            self._background_status[task_id].update(
                {"status": "FAILED", "error_type": type(exc).__name__, "completed_at": time.time()}
            )
            decision = "BACKGROUND_FAILED"
            trace.add("BACKGROUND", "Background task failed", error_type=type(exc).__name__)
        finally:
            origin_fetches = max(origin_fetches, trace.origin_fetches)
            self.store.record_trace(
                request_id=trace.request_id,
                node_id=spec.node_id,
                cache_key=key.digest,
                decision=decision,
                origin_fetches=origin_fetches,
                events=trace.events,
            )

    def _response_envelope(
        self,
        *,
        spec: RequestSpec,
        policy: CachePolicy,
        key: CacheKey,
        entry: Mapping[str, Any],
        payload: bytes,
        body_encoding: str,
        cache_status: str,
        origin_fetches: int,
        now: float,
        background: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        age = max(0, int(now - float(entry["stored_at"])))
        headers: dict[str, str] = {
            "Age": str(age),
            "Cache-Control": policy.cache_control,
            "ETag": str(entry["etag"]),
            "X-Cache-Status": cache_status,
            "X-Cache": "STALE" if cache_status == "STALE" else ("MISS" if cache_status == "MISS" else "HIT"),
            "X-Policy-Version": str(policy.version),
            "X-Origin-Version": str(entry["origin_version"]),
        }
        if cache_status == "STALE":
            headers["Warning"] = '110 - "Response is stale"'
            headers["X-Stale"] = "true"
        body_text, detected_encoding = _body_for_http(payload)
        if spec.method.upper() == "HEAD":
            body_text = ""
        response = {
            "ok": True,
            "request_id": spec.request_id,
            "node_id": spec.node_id,
            "method": spec.method.upper(),
            "cache_key": key.digest,
            "key_canonical": key.canonical,
            "canonical_path": key.path,
            "canonical_query": key.query,
            "vary": key.vary,
            "cache_status": cache_status,
            "http_status": 200,
            "content_type": str(entry["content_type"]),
            "headers": headers,
            "body": body_text,
            "body_encoding": body_encoding if body_encoding == "base64" else detected_encoding,
            "body_length": len(payload),
            "etag": str(entry["etag"]),
            "origin_version": str(entry["origin_version"]),
            "policy_version": policy.version,
            "origin_fetches": origin_fetches,
            "age_seconds": age,
            "remaining_ttl_seconds": max(0.0, float(entry["fresh_until"]) - now),
            "stale_remaining_seconds": max(0.0, float(entry["stale_until"]) - now),
            "generation": entry.get("generation"),
            "purge_epoch": entry.get("purge_epoch"),
        }
        if background is not None:
            response["background_revalidation"] = dict(background)
        return response

    def background_status(self, task_id: str | None = None) -> Any:
        if task_id:
            return self._background_status.get(task_id)
        return sorted(self._background_status.values(), key=lambda item: item.get("started_at", 0), reverse=True)

    async def wait_background(self, task_id: str, timeout: float = 5.0) -> dict[str, Any] | None:
        task = self._background.get(task_id)
        if task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            except asyncio.TimeoutError:
                pass
        return self._background_status.get(task_id)

    async def purge(
        self,
        *,
        request_id: str,
        path: str | None,
        tag: str | None,
        injections: Any,
        request_payload: Mapping[str, Any],
        fingerprint: str,
        target_node: str | None = None,
    ) -> tuple[dict[str, Any], int]:
        claim, existing = self.store.claim_request(
            request_id=request_id,
            node_id="__purge__",
            fingerprint=fingerprint,
            request_payload=request_payload,
        )
        if claim == "EXISTING":
            if existing.get("response") is not None:
                return self._replay_response(existing)
            if existing.get("status") == "IN_PROGRESS":
                future = self._request_futures.get(request_id)
                if future is None:
                    raise wb_error("E_REQUEST_INCOMPLETE", "Unfinished purge will not be executed again")
                response, status, _code = await asyncio.shield(future)
                self.store.record_trace(
                    request_id=request_id,
                    node_id=existing.get("node_id", "__purge__"),
                    cache_key=None,
                    decision="DEDUPLICATED_IN_FLIGHT",
                    origin_fetches=0,
                    events=[
                        {
                            "at": time.time(),
                            "phase": "DEDUPE",
                            "detail": "Identical purge joined the original in-flight execution",
                        }
                    ],
                )
                return self._mark_replayed(response), status
            raise wb_error("E_REQUEST_INCOMPLETE", "Unfinished purge will not be executed again")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[dict[str, Any], int, str | None]] = loop.create_future()
        self._request_futures[request_id] = future
        trace = Trace(request_id=request_id, node_id=target_node or "all-applicable")
        decision = "PURGE"
        response: dict[str, Any] = {}
        status = 500
        code: str | None = None
        try:
            policy = self.store.get_current_policy()
            if policy is None:
                raise wb_error("E_POLICY_MISSING", "No cache policy snapshot has been committed")
            if target_node is not None and target_node not in policy.nodes:
                raise wb_error(
                    "E_INJECT_INVALID",
                    "Purge injection target is not covered by the current policy",
                    target_node=target_node,
                    policy_nodes=list(policy.nodes),
                )
            injection_targets: list[str] = []
            if isinstance(injections, Mapping):
                candidate_items = [injections]
            elif isinstance(injections, list):
                candidate_items = injections
            else:
                candidate_items = []
            for item in candidate_items:
                if isinstance(item, Mapping) and item.get("target_node") is not None:
                    injection_targets.append(str(item["target_node"]))
            if len(set(injection_targets)) > 1:
                raise wb_error(
                    "E_INJECT_INVALID",
                    "Purge injection target_node declarations conflict",
                    targets=sorted(set(injection_targets)),
                )
            if target_node is not None and injection_targets and injection_targets[0] != target_node:
                raise wb_error(
                    "E_INJECT_INVALID",
                    "Purge envelope target_node conflicts with the injection target_node",
                    envelope_target=target_node,
                    injection_target=injection_targets[0],
                )
            target_node = target_node or (injection_targets[0] if injection_targets else None)
            if target_node is not None and target_node not in policy.nodes:
                raise wb_error(
                    "E_INJECT_INVALID",
                    "Purge injection target is not covered by the current policy",
                    target_node=target_node,
                    policy_nodes=list(policy.nodes),
                )
            trace.node_id = target_node or "all-applicable"
            build_node = target_node or (policy.nodes[0] if policy.nodes else "__purge__")
            raw_specs: list[Mapping[str, Any]] = []
            try:
                raw_specs = normalize_injection_specs(injections=injections)
                plan = build_injection_plan(
                    raw_specs,
                    node_id=build_node,
                    route=path,
                    context="purge",
                    max_body_pad_bytes=self.config.max_body_pad_bytes,
                    default_timeout_ms=self.config.origin_timeout_ms,
                )
                injection_ids = record_plan(
                    self.store, plan, request_id=request_id, node_id=build_node, route=path
                )
            except WorkbenchError as exc:
                record_rejected_injection(
                    self.store,
                    exc,
                    request_id=request_id,
                    node_id=build_node,
                    route=path,
                    raw_specs=raw_specs or [{"type": "invalid_purge_injection"}],
                )
                raise
            if (path is None) == (tag is None):
                raise wb_error("E_INJECT_INVALID", "Purge requires exactly one of path or tag")
            canonical_path = normalize_path(path) if path is not None else None
            purge_id = str(uuid.uuid4())
            started_at = time.time()
            intent_id = self.store.create_intent(
                kind="PURGE_BROADCAST",
                request_id=request_id,
                node_id=target_node,
                cache_key=None,
                payload={
                    "path": canonical_path,
                    "tag": tag,
                    "nodes": list(policy.nodes),
                    "policy_version": policy.version,
                    "injection": plan.as_dict(),
                },
            )
            trace.add("INTENT", "Purge intent persisted before broadcast commit", intent_id=intent_id)
            if plan.purge_interrupt:
                details = {
                    "reason": "Injected purge broadcast interruption before atomic commit",
                    "target_node": build_node,
                    "intent_id": intent_id,
                }
                self.store.record_purge_failure(
                    purge_id=purge_id,
                    intent_id=intent_id,
                    request_id=request_id,
                    nodes=policy.nodes,
                    canonical_path=canonical_path,
                    tag=tag,
                    policy_version=policy.version,
                    broadcast_window_ms=policy.purge_propagation_window_ms,
                    started_at=started_at,
                    error_code="E_PURGE_BROADCAST_FAIL",
                    details=details,
                )
                self.store.update_injection_status(
                    injection_ids,
                    status="FAILED",
                    business_code="E_PURGE_BROADCAST_FAIL",
                    details=details,
                )
                raise wb_error(
                    "E_PURGE_BROADCAST_FAIL",
                    "Injected purge broadcast interruption; all node changes were rolled back",
                    **details,
                )
            try:
                purge_result = self.store.commit_purge(
                    purge_id=purge_id,
                    intent_id=intent_id,
                    request_id=request_id,
                    nodes=policy.nodes,
                    canonical_path=canonical_path,
                    tag=tag,
                    policy_version=policy.version,
                    broadcast_window_ms=policy.purge_propagation_window_ms,
                    started_at=started_at,
                )
            except WorkbenchError as exc:
                self.store.record_purge_failure(
                    purge_id=purge_id,
                    intent_id=intent_id,
                    request_id=request_id,
                    nodes=policy.nodes,
                    canonical_path=canonical_path,
                    tag=tag,
                    policy_version=policy.version,
                    broadcast_window_ms=policy.purge_propagation_window_ms,
                    started_at=started_at,
                    error_code=exc.code,
                    details=exc.details,
                )
                self.store.update_injection_status(
                    injection_ids,
                    status="FAILED",
                    business_code=exc.code,
                    details=exc.details,
                )
                raise
            trace.add(
                "PURGE_COMMIT",
                "All applicable node entries and derived indexes were changed in one transaction",
                **purge_result,
            )
            response = {
                "ok": True,
                "request_id": request_id,
                "http_status": 200,
                "purge": purge_result,
                "policy_version": policy.version,
            }
            status = 200
            code: str | None = None
        except WorkbenchError as exc:
            response = exc.to_dict()
            response.update({"request_id": request_id, "node_id": "all-applicable", "http_status": exc.http_status})
            status = exc.http_status
            code = exc.code
            decision = "PURGE_ERROR"
            trace.add("ERROR", exc.message, business_code=exc.code, details=exc.details)
        except Exception as exc:
            response = {
                "ok": False,
                "business_code": "E_INTERNAL",
                "message": "Unexpected purge failure",
                "details": {"error_type": type(exc).__name__},
                "request_id": request_id,
                "node_id": "all-applicable",
                "http_status": 500,
            }
            status = 500
            code = "E_INTERNAL"
            decision = "PURGE_ERROR"
            trace.add("ERROR", "Unexpected purge failure", business_code="E_INTERNAL", error_type=type(exc).__name__)
        finally:
            trace_payload = trace.as_dict(decision=decision, origin_fetches=0)
            self.store.record_trace(
                request_id=request_id,
                node_id=target_node or "all-applicable",
                cache_key=None,
                decision=decision,
                origin_fetches=0,
                events=trace.events,
            )
        response["trace"] = trace_payload
        self.store.complete_request(
            request_id,
            response=response,
            http_status=status,
            business_code=code,
            status="FAILED" if code else "COMPLETED",
        )
        self._request_futures.pop(request_id, None)
        future.set_result((response, status, code))
        return response, status

