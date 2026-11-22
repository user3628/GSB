from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from typing import Any, Iterable

from .config import AppConfig
from .errors import BusinessError, HTTP_STATUS_BY_CODE
from .models import BatchResult, ItemResult, QuerySpec, make_cache_key, new_round_id, now_ms
from .protocol import (
    NAME_BY_TYPE,
    QCLASS_IN,
    SUPPORTED_TYPES,
    TYPE_BY_NAME,
    RCODE_SERVFAIL,
    build_query,
    parse_message,
    parse_query_packet,
    parse_presentation_name,
    records_to_json,
)
from .resolver import ResolutionOutcome, Resolver
from .scheduler import ConcurrencyGate, Flight, FlightRegistry, GateTimeout
from .storage import CacheEntry, Storage, cache_entry_from_outcome
from .upstream import UpstreamClient


@dataclass(slots=True)
class SharedResolution:
    cache_key: str
    rcode: int = RCODE_SERVFAIL
    source_type: int | None = None
    answers: list[dict[str, Any]] = field(default_factory=list)
    authorities: list[dict[str, Any]] = field(default_factory=list)
    ttl_base_ms: int | None = None
    cache: dict[str, Any] | None = None
    cache_hit: bool = False
    business_code: str | None = None
    message: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    upstream_hops: int = 0
    chain: list[str] = field(default_factory=list)
    response_payload: dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.business_code is None

    def error_dict(self) -> dict[str, Any] | None:
        if self.business_code is None:
            return None
        return {"code": self.business_code, "message": self.message, "context": self.context}


@dataclass(slots=True)
class Claim:
    kind: str
    spec: QuerySpec
    future: asyncio.Future[ItemResult] | None = None
    replay: ItemResult | None = None
    conflict: dict[str, Any] | None = None


class DiagnosticService:
    def __init__(self, config: AppConfig, storage: Storage, upstream: UpstreamClient | None = None) -> None:
        self.config = config
        self.storage = storage
        self.upstream = upstream or UpstreamClient()
        self.gate = ConcurrencyGate(
            config.scheduler.global_upstream_limit,
            config.scheduler.per_authority_limit,
            config.scheduler.queue_limit,
        )
        self.registry = FlightRegistry(config.scheduler.max_splits_per_flight)
        self.resolver = Resolver(config, self.upstream, self.gate)
        self._id_lock = asyncio.Lock()
        self._id_futures: dict[str, asyncio.Future[ItemResult]] = {}
        self._hot_names = {
            parse_presentation_name(name).canonical for name in config.hot_names
        }

    def prepare_spec(
        self,
        *,
        name: str,
        qtype: int | str,
        qclass: int = QCLASS_IN,
        query_id: str | None = None,
        priority: int = 0,
        hot: bool = False,
        round_id: str | None = None,
        source_transport: str = "http",
        client: str = "127.0.0.1",
        arrival_seq: int = 0,
    ) -> QuerySpec:
        canonical = parse_presentation_name(name).canonical
        type_code = TYPE_BY_NAME[qtype.upper()] if isinstance(qtype, str) else int(qtype)
        if type_code not in SUPPORTED_TYPES:
            raise BusinessError(
                code="E_REQUEST_INVALID",
                message="unsupported query type",
                context={"type": qtype, "supported": sorted(NAME_BY_TYPE[item] for item in SUPPORTED_TYPES)},
                http_status=HTTP_STATUS_BY_CODE["E_REQUEST_INVALID"],
            )
        if int(qclass) != QCLASS_IN:
            raise BusinessError(
                code="E_REQUEST_INVALID",
                message="only IN class is supported",
                context={"qclass": qclass},
                http_status=HTTP_STATUS_BY_CODE["E_REQUEST_INVALID"],
            )
        if query_id is not None and (not isinstance(query_id, str) or not query_id.strip()):
            raise BusinessError(
                code="E_REQUEST_INVALID",
                message="query_id must be a non-empty string",
                context={"query_id": query_id},
                http_status=HTTP_STATUS_BY_CODE["E_REQUEST_INVALID"],
            )
        spec = QuerySpec(
            name=canonical,
            qtype=type_code,
            qclass=int(qclass),
            query_id=query_id.strip() if query_id else "",
            priority=max(0, int(priority)),
            hot=bool(hot) or canonical in self._hot_names,
            round_id=round_id,
            source_transport=source_transport,
            client=client,
            arrival_seq=arrival_seq,
        )
        return spec

    async def submit_batch(
        self,
        specs: list[QuerySpec],
        *,
        round_budget_ms: int | None = None,
        round_id: str | None = None,
    ) -> BatchResult:
        if not specs:
            raise BusinessError(
                code="E_REQUEST_INVALID",
                message="at least one query is required",
                context={},
                http_status=HTTP_STATUS_BY_CODE["E_REQUEST_INVALID"],
            )
        ordered = list(specs)
        for index, spec in enumerate(ordered):
            spec.arrival_seq = index
        chosen_round_id = round_id or next((spec.round_id for spec in ordered if spec.round_id), None) or new_round_id()
        for spec in ordered:
            spec.round_id = chosen_round_id
        budget = max(1, int(round_budget_ms or self.config.scheduler.round_budget_ms))
        started_at_ms = now_ms()

        claims: list[Claim] = []
        for spec in ordered:
            claims.append(await self._claim(spec))

        new_claims = [claim for claim in claims if claim.kind == "new"]
        results: dict[str, ItemResult] = {}
        trace: dict[str, Any] = {
            "round_id": chosen_round_id,
            "mode": "diagnostic_batch",
            "limits": {
                "global_upstream_limit": self.config.scheduler.global_upstream_limit,
                "per_authority_limit": self.config.scheduler.per_authority_limit,
                "queue_limit": self.config.scheduler.queue_limit,
                "admission_capacity_unique_upstream_tasks": self.config.scheduler.global_upstream_limit
                + self.config.scheduler.queue_limit,
                "round_budget_ms": budget,
                "upstream_timeout_ms": self.config.scheduler.upstream_timeout_ms,
            },
            "started_at_ms": started_at_ms,
            "deadline_at_ms": started_at_ms + budget,
            "dispatch": [],
            "waiting": [],
            "skipped": [],
            "merge_decisions": [],
            "cache_hits": [],
            "groups": [],
            "notes": [
                "The round budget governs cache lookups, admission, gate waiting, and merge waiting.",
                "Each active upstream exchange is capped by the remaining round budget. A round-budget "
                "cancellation is reported as E_BUDGET_EXHAUSTED with socket_closed=true, never as an "
                "upstream timeout.",
            ],
        }
        await self.storage.start_round(chosen_round_id, budget, started_at_ms)

        if new_claims:
            try:
                results.update(
                    await self._execute_round(
                        new_claims,
                        round_id=chosen_round_id,
                        started_at_ms=started_at_ms,
                        budget_ms=budget,
                        trace=trace,
                    )
                )
            except Exception as exc:
                for claim in new_claims:
                    item_result = await self._fail_claim(
                        claim.spec,
                        code="E_INTERNAL",
                        message=str(exc),
                        context={"exception_type": type(exc).__name__},
                        round_id=chosen_round_id,
                    )
                    results[claim.spec.query_id] = item_result
                    self._resolve_id_future(claim.spec.query_id, item_result)

        for claim in claims:
            if claim.kind == "new":
                if claim.spec.query_id not in results:
                    result = await self._await_id_future(claim.spec.query_id)
                    if result is not None:
                        results[claim.spec.query_id] = result
            elif claim.kind == "wait":
                if claim.future is not None:
                    try:
                        results[claim.spec.query_id] = await claim.future
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        results[claim.spec.query_id] = await self._fail_claim(
                            claim.spec,
                            code="E_INTERNAL",
                            message=str(exc),
                            context={"exception_type": type(exc).__name__},
                            round_id=chosen_round_id,
                        )
            elif claim.kind == "replay" and claim.replay is not None:
                results[claim.spec.query_id] = claim.replay
            elif claim.kind == "conflict" and claim.conflict is not None:
                results[claim.spec.query_id] = ItemResult(
                    query_id=claim.spec.query_id,
                    cache_key=claim.spec.cache_key,
                    name=claim.spec.name,
                    qtype=claim.spec.qtype,
                    qclass=claim.spec.qclass,
                    outcome="error",
                    rcode=RCODE_SERVFAIL,
                    business_code="E_CONFLICTING_QUERY",
                    message="query_id was previously used with different query content",
                    context=claim.conflict,
                )

        items = [results.get(spec.query_id) or self._fallback_error_item(spec) for spec in ordered]
        finished_at_ms = now_ms()
        trace["finished_at_ms"] = finished_at_ms
        trace["elapsed_ms"] = finished_at_ms - started_at_ms
        trace["dispatch_closed_at_ms"] = started_at_ms + budget
        trace["dispatch_after_deadline_count"] = sum(
            1 for event in trace["dispatch"] if int(event.get("dispatched_at_ms", 0)) > started_at_ms + budget
        )
        trace["gate"] = await self.gate.snapshot()
        trace["singleflight"] = await self.registry.snapshot()
        trace["upstream"] = self.upstream.stats.snapshot()
        trace["result_counts"] = _result_counts(items)
        await self.storage.finish_round(chosen_round_id, trace)
        return BatchResult(
            round_id=chosen_round_id,
            started_at_ms=started_at_ms,
            finished_at_ms=finished_at_ms,
            budget_ms=budget,
            items=items,
            trace=trace,
        )

    async def _claim(self, spec: QuerySpec) -> Claim:
        async with self._id_lock:
            existing = await self.storage.get_session(spec.query_id)
            if existing is not None:
                if existing["fingerprint"] != spec.fingerprint:
                    return Claim(
                        kind="conflict",
                        spec=spec,
                        conflict={
                            "query_id": spec.query_id,
                            "existing": {
                                "name": existing["name"],
                                "type": existing["type"],
                                "qclass": existing["qclass"],
                            },
                            "incoming": {"name": spec.name, "type": spec.qtype, "qclass": spec.qclass},
                        },
                    )
                state = existing["state"]
                if state in {"complete", "failed"}:
                    return Claim(kind="replay", spec=spec, replay=self._result_from_session(spec, existing))
                future = self._id_futures.get(spec.query_id)
                if future is not None:
                    return Claim(kind="wait", spec=spec, future=future)
                # A stale pending/resolving row with no in-memory owner (for example after a
                # previous process generation) is replayed from committed facts by recovery,
                # then safely re-executed when the caller explicitly submits it again.
            future = asyncio.get_running_loop().create_future()
            self._id_futures[spec.query_id] = future
            await self.storage.start_session(spec, spec.round_id or "")
            return Claim(kind="new", spec=spec, future=future)

    def _resolve_id_future(self, query_id: str, result: ItemResult) -> None:
        future = self._id_futures.pop(query_id, None)
        if future is not None and not future.done():
            future.set_result(result)

    async def _await_id_future(self, query_id: str) -> ItemResult | None:
        future = self._id_futures.get(query_id)
        if future is None:
            session = await self.storage.get_session(query_id)
            if session is None:
                return None
            spec = QuerySpec(
                name=session["name"],
                qtype=session["type"],
                qclass=session["qclass"],
                query_id=query_id,
                source_transport=session["transport"],
                client=session["client"],
            )
            return self._result_from_session(spec, session)
        return await future

    def _result_from_session(self, spec: QuerySpec, session: dict[str, Any]) -> ItemResult:
        response = session.get("response") or {}
        error = session.get("error") or {}
        business_code = session.get("business_code") or error.get("code")
        return ItemResult(
            query_id=spec.query_id,
            cache_key=spec.cache_key,
            name=spec.name,
            qtype=spec.qtype,
            qclass=spec.qclass,
            outcome="idempotent_replay",
            rcode=int(session.get("rcode") if session.get("rcode") is not None else response.get("rcode", RCODE_SERVFAIL)),
            source_type=session.get("source_record_type", response.get("source_record_type")),
            business_code=business_code,
            message=error.get("message"),
            context=dict(error.get("context") or {}),
            answers=list(response.get("answers") or []),
            authorities=list(response.get("authorities") or []),
            ttl_base_ms=response.get("ttl_base_ms"),
            cache=response.get("cache"),
            wait_ms=0,
        )

    async def _fail_claim(
        self,
        spec: QuerySpec,
        *,
        code: str,
        message: str,
        context: dict[str, Any],
        round_id: str,
        rcode: int = RCODE_SERVFAIL,
    ) -> ItemResult:
        error = {"code": code, "message": message, "context": context}
        await self.storage.complete_session(
            spec.query_id,
            state="failed",
            rcode=rcode,
            source_type=None,
            business_code=code,
            response=None,
            error=error,
        )
        result = ItemResult(
            query_id=spec.query_id,
            cache_key=spec.cache_key,
            name=spec.name,
            qtype=spec.qtype,
            qclass=spec.qclass,
            outcome="error",
            rcode=rcode,
            business_code=code,
            message=message,
            context=context,
        )
        return result

    def _fallback_error_item(self, spec: QuerySpec) -> ItemResult:
        return ItemResult(
            query_id=spec.query_id,
            cache_key=spec.cache_key,
            name=spec.name,
            qtype=spec.qtype,
            qclass=spec.qclass,
            outcome="error",
            rcode=RCODE_SERVFAIL,
            business_code="E_INTERNAL",
            message="query result was not produced",
            context={"query_id": spec.query_id},
        )

    async def _execute_round(
        self,
        claims: list[Claim],
        *,
        round_id: str,
        started_at_ms: int,
        budget_ms: int,
        trace: dict[str, Any],
    ) -> dict[str, ItemResult]:
        deadline_ms = started_at_ms + budget_ms
        results: dict[str, ItemResult] = {}

        entry_lookups = await asyncio.gather(
            *(self.storage.get_cache(claim.spec.cache_key) for claim in claims),
            return_exceptions=True,
        )
        miss_claims: list[Claim] = []
        for claim, entry_or_error in zip(claims, entry_lookups):
            if isinstance(entry_or_error, BaseException):
                result = await self._fail_claim(
                    claim.spec,
                    code="E_STORAGE",
                    message=str(entry_or_error),
                    context={"operation": "cache_lookup"},
                    round_id=round_id,
                )
                results[claim.spec.query_id] = result
                self._resolve_id_future(claim.spec.query_id, result)
                continue
            entry = entry_or_error
            if entry is not None and entry.expires_at_ms > now_ms():
                result = await self._cache_hit(claim.spec, entry, round_id, trace)
                results[claim.spec.query_id] = result
                self._resolve_id_future(claim.spec.query_id, result)
                continue
            if entry is not None:
                await self.storage.cleanup_expired()
            miss_claims.append(claim)

        if not miss_claims:
            return results

        for claim in miss_claims:
            await self.storage.mark_session_resolving(claim.spec.query_id, round_id)

        groups: dict[str, list[Claim]] = {}
        for claim in miss_claims:
            groups.setdefault(claim.spec.cache_key, []).append(claim)

        group_priority: dict[str, int] = {}
        for key, members in groups.items():
            group_priority[key] = max(member.spec.priority_score for member in members)

        existing_flights: dict[str, Flight] = {}
        for key in groups:
            flight = await self.registry.peek(key)
            if flight is not None and (
                flight.cache_expires_at_ms is None or flight.cache_expires_at_ms > now_ms()
            ):
                existing_flights[key] = flight

        new_group_keys = [key for key in groups if key not in existing_flights]
        new_group_keys.sort(
            key=lambda key: (-group_priority[key], min(member.spec.arrival_seq for member in groups[key]))
        )
        admission_capacity = self.config.scheduler.global_upstream_limit + self.config.scheduler.queue_limit
        admitted_keys = set(new_group_keys[:admission_capacity])
        rejected_keys = set(new_group_keys[admission_capacity:])

        trace["groups"] = [
            {
                "cache_key": key,
                "priority": group_priority[key],
                "leader_query_id": min(groups[key], key=lambda item: item.spec.arrival_seq).spec.query_id,
                "requests": len(groups[key]),
                "existing_flight": key in existing_flights,
                "admission": "existing_flight"
                if key in existing_flights
                else ("admitted" if key in admitted_keys else "queue_full"),
            }
            for key in groups
        ]

        for key in rejected_keys:
            for claim in groups[key]:
                context = {
                    "cache_key": key,
                    "admission_capacity": admission_capacity,
                    "global_upstream_limit": self.config.scheduler.global_upstream_limit,
                    "queue_limit": self.config.scheduler.queue_limit,
                    "active_or_admitted_unique_tasks": len(admitted_keys) + len(existing_flights),
                    "priority": claim.spec.priority_score,
                }
                result = await self._fail_claim(
                    claim.spec,
                    code="E_QUEUE_FULL",
                    message="bounded upstream waiting queue is full",
                    context=context,
                    round_id=round_id,
                )
                results[claim.spec.query_id] = result
                self._resolve_id_future(claim.spec.query_id, result)
                trace["skipped"].append(
                    {
                        "query_id": claim.spec.query_id,
                        "cache_key": key,
                        "code": "E_QUEUE_FULL",
                        "reason": "admission_capacity_exhausted",
                        "at_ms": now_ms(),
                        **context,
                    }
                )

        tasks: list[asyncio.Task[ItemResult]] = []
        task_meta: list[tuple[Claim, str]] = []
        group_order = sorted(
            groups,
            key=lambda key: (
                0 if key in existing_flights else 1,
                -group_priority[key],
                min(member.spec.arrival_seq for member in groups[key]),
            ),
        )
        for key in group_order:
            if key in rejected_keys:
                continue
            members = sorted(groups[key], key=lambda item: item.spec.arrival_seq)
            if key in existing_flights:
                flight = existing_flights[key]
                for claim in members:
                    task = asyncio.create_task(
                        self._execute_follower(flight, claim, deadline_ms, round_id, trace, source="existing_flight")
                    )
                    tasks.append(task)
                    task_meta.append((claim, "follower"))
                continue
            flight, created = await self.registry.get_or_create(
                key,
                members[0].spec.query_id,
                self.config.scheduler.upstream_timeout_ms,
                priority=group_priority[key],
                arrival_seq=members[0].spec.arrival_seq,
            )
            if not created:
                for claim in members:
                    task = asyncio.create_task(
                        self._execute_follower(flight, claim, deadline_ms, round_id, trace, source="cross_round")
                    )
                    tasks.append(task)
                    task_meta.append((claim, "follower"))
                continue
            leader_claim = members[0]
            leader_task = asyncio.create_task(
                self._execute_leader(flight, leader_claim, deadline_ms, round_id, trace)
            )
            tasks.append(leader_task)
            task_meta.append((leader_claim, "leader"))
            for claim in members[1:]:
                task = asyncio.create_task(
                    self._execute_follower(flight, claim, deadline_ms, round_id, trace, source="batch_merge")
                )
                tasks.append(task)
                task_meta.append((claim, "follower"))

        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for (claim, role), item in zip(task_meta, gathered):
            if isinstance(item, BaseException):
                result = await self._fail_claim(
                    claim.spec,
                    code="E_INTERNAL",
                    message=str(item),
                    context={"role": role, "exception_type": type(item).__name__},
                    round_id=round_id,
                )
            else:
                result = item
            results[claim.spec.query_id] = result
            self._resolve_id_future(claim.spec.query_id, result)
        return results

    async def _cache_hit(
        self,
        spec: QuerySpec,
        entry: CacheEntry,
        round_id: str,
        trace: dict[str, Any],
    ) -> ItemResult:
        instant = now_ms()
        view = entry.to_dict(instant)
        payload = {
            "cache_key": entry.cache_key,
            "rcode": entry.rcode,
            "source_record_type": entry.source_type,
            "answers": view["answers"],
            "authorities": view["authorities"],
            "ttl_base_ms": entry.written_at_ms,
            "cache": view,
            "cache_hit": True,
        }
        await self.storage.complete_session(
            spec.query_id,
            state="complete",
            rcode=entry.rcode,
            source_type=entry.source_type,
            business_code=None,
            response=payload,
            error=None,
            cache_hit=True,
        )
        trace["cache_hits"].append(
            {
                "query_id": spec.query_id,
                "cache_key": entry.cache_key,
                "kind": entry.kind,
                "remaining_ttl_ms": view["remaining_ttl_ms"],
                "at_ms": instant,
            }
        )
        return ItemResult(
            query_id=spec.query_id,
            cache_key=entry.cache_key,
            name=spec.name,
            qtype=spec.qtype,
            qclass=spec.qclass,
            outcome="cache_hit",
            rcode=entry.rcode,
            source_type=entry.source_type,
            answers=view["answers"],
            authorities=view["authorities"],
            ttl_base_ms=entry.written_at_ms,
            cache=view,
        )

    async def _execute_leader(
        self,
        flight: Flight,
        claim: Claim,
        deadline_ms: int,
        round_id: str,
        trace: dict[str, Any],
    ) -> ItemResult:
        spec = claim.spec

        async def on_dispatch(authority: Any, event: dict[str, Any]) -> None:
            timestamp = now_ms()
            if flight.started_ms is None:
                flight.started_ms = timestamp
            flight.dispatch_count += 1
            trace["dispatch"].append(
                {
                    **event,
                    "kind": "leader" if flight.dispatch_count == 1 else "leader_chain_hop",
                    "query_id": spec.query_id,
                    "cache_key": spec.cache_key,
                    "priority": spec.priority_score,
                    "dispatched_at_ms": timestamp,
                    "round_deadline_ms": deadline_ms,
                }
            )
            if int(event.get("queue_wait_ms", 0)) > 0:
                trace["waiting"].append(
                    {
                        "query_id": spec.query_id,
                        "cache_key": spec.cache_key,
                        "kind": "gate_wait",
                        "waited_ms": int(event["queue_wait_ms"]),
                        "authority_id": event["authority_id"],
                    }
                )

        try:
            outcome = await self.resolver.resolve(spec, deadline_ms=deadline_ms, on_dispatch=on_dispatch)
            shared = await self._commit_leader_outcome(claim, outcome, round_id)
        except asyncio.CancelledError:
            shared = SharedResolution(
                cache_key=spec.cache_key,
                business_code="E_INTERNAL",
                message="leader resolution was cancelled",
                context={"query_id": spec.query_id, "reason": "cancelled"},
            )
            if not flight.future.done():
                flight.future.set_result(shared)
            raise
        except Exception as exc:
            shared = SharedResolution(
                cache_key=spec.cache_key,
                business_code="E_INTERNAL",
                message=str(exc),
                context={"exception_type": type(exc).__name__},
            )
            await self._complete_failed_session(spec, shared)
        finally:
            if not flight.future.done():
                if "shared" not in locals():
                    shared = SharedResolution(
                        cache_key=spec.cache_key,
                        business_code="E_INTERNAL",
                        message="leader produced no result",
                        context={"query_id": spec.query_id},
                    )
                flight.future.set_result(shared)
            if "shared" in locals() and shared.cache is not None:
                flight.cache_expires_at_ms = int(shared.cache.get("expires_at_ms") or 0)
            if (
                "shared" in locals()
                and shared.business_code is None
                and flight.cache_expires_at_ms is not None
                and flight.cache_expires_at_ms > now_ms()
            ):
                self.registry.schedule_remove(flight)
            else:
                await self.registry.remove(flight)

        if not shared.success and shared.business_code in {"E_BUDGET_EXHAUSTED", "E_QUEUE_FULL"}:
            if shared.context.get("reason") == "round_budget_exhausted_while_waiting":
                trace["waiting"].append(
                    {
                        "query_id": spec.query_id,
                        "cache_key": spec.cache_key,
                        "kind": "gate_wait",
                        "authority_id": shared.context.get("authority_id"),
                        "waited_ms": shared.context.get("waited_ms", 0),
                        "outcome": "budget_exhausted",
                    }
                )
            trace["skipped"].append(
                {
                    "query_id": spec.query_id,
                    "cache_key": spec.cache_key,
                    "code": shared.business_code,
                    "reason": shared.context.get(
                        "reason",
                        "round_budget_exhausted"
                        if shared.business_code == "E_BUDGET_EXHAUSTED"
                        else "upstream_waiting_queue_full",
                    ),
                    "at_ms": now_ms(),
                    **shared.context,
                }
            )
        return self._item_from_shared(spec, shared, outcome="resolved")

    async def _commit_leader_outcome(
        self,
        claim: Claim,
        outcome: ResolutionOutcome,
        round_id: str,
    ) -> SharedResolution:
        spec = claim.spec
        if outcome.business_code is not None:
            shared = self._shared_from_outcome(spec, outcome, written_at_ms=None, cache=None)
            await self._complete_failed_session(spec, shared)
            return shared

        written_at = now_ms()
        entry = None
        cache_view = None
        if outcome.cacheable and outcome.ttl_ms > 0:
            answer_json = records_to_json(outcome.answers)
            authority_json = records_to_json(outcome.authorities)
            entry = cache_entry_from_outcome(
                spec=spec,
                written_at_ms=written_at,
                ttl_ms=outcome.ttl_ms,
                rcode=outcome.rcode,
                source_type=outcome.source_type,
                kind=outcome.kind,
                negative_kind=outcome.negative_kind,
                answers=answer_json,
                authorities=authority_json,
            )
            cache_view = entry.to_dict(written_at)
        shared = self._shared_from_outcome(spec, outcome, written_at_ms=written_at, cache=cache_view)
        if entry is None:
            await self.storage.complete_session(
                spec.query_id,
                state="complete",
                rcode=outcome.rcode,
                source_type=outcome.source_type,
                business_code=None,
                response=shared.response_payload,
                error=None,
            )
            return shared

        intent_id = await self.storage.write_intent(
            query_id=spec.query_id,
            cache_key=spec.cache_key,
            kind=entry.kind,
            payload={
                "cache_key": entry.cache_key,
                "ttl_ms": entry.original_ttl_ms,
                "written_at_ms": entry.written_at_ms,
                "rcode": entry.rcode,
                "source_type": entry.source_type,
                "negative_kind": entry.negative_kind,
            },
        )
        try:
            await self.storage.commit_cache_and_session(
                intent_id=intent_id,
                entry=entry,
                query_id=spec.query_id,
                response=shared.response_payload,
                rcode=outcome.rcode,
                source_type=outcome.source_type,
            )
        except Exception as exc:
            shared = SharedResolution(
                cache_key=spec.cache_key,
                rcode=RCODE_SERVFAIL,
                source_type=None,
                business_code="E_STORAGE",
                message=f"cache commit failed: {exc}",
                context={"query_id": spec.query_id, "intent_id": intent_id, "exception_type": type(exc).__name__},
            )
            await self._complete_failed_session(spec, shared)
        return shared

    async def _complete_failed_session(self, spec: QuerySpec, shared: SharedResolution) -> None:
        if shared.business_code is None:
            return
        await self.storage.complete_session(
            spec.query_id,
            state="failed",
            rcode=shared.rcode,
            source_type=shared.source_type,
            business_code=shared.business_code,
            response=None,
            error=shared.error_dict(),
        )

    def _shared_from_outcome(
        self,
        spec: QuerySpec,
        outcome: ResolutionOutcome,
        *,
        written_at_ms: int | None,
        cache: dict[str, Any] | None,
    ) -> SharedResolution:
        answers = records_to_json(outcome.answers)
        authorities = records_to_json(outcome.authorities)
        payload: dict[str, Any] = {
            "cache_key": spec.cache_key,
            "rcode": outcome.rcode,
            "source_record_type": outcome.source_type,
            "answers": answers,
            "authorities": authorities,
            "ttl_base_ms": written_at_ms,
            "cache": cache,
            "upstream_hops": outcome.upstream_hops,
            "chain": outcome.chain,
        }
        if outcome.business_code:
            payload["business_code"] = outcome.business_code
            payload["message"] = outcome.message
            payload["context"] = outcome.context
        return SharedResolution(
            cache_key=spec.cache_key,
            rcode=outcome.rcode,
            source_type=outcome.source_type,
            answers=answers,
            authorities=authorities,
            ttl_base_ms=written_at_ms,
            cache=cache,
            business_code=outcome.business_code,
            message=outcome.message,
            context=outcome.context,
            upstream_hops=outcome.upstream_hops,
            chain=outcome.chain,
            response_payload=payload,
        )

    def _item_from_shared(
        self,
        spec: QuerySpec,
        shared: SharedResolution,
        *,
        outcome: str,
        wait_ms: int = 0,
        merge_decision: dict[str, Any] | None = None,
    ) -> ItemResult:
        return ItemResult(
            query_id=spec.query_id,
            cache_key=spec.cache_key,
            name=spec.name,
            qtype=spec.qtype,
            qclass=spec.qclass,
            outcome=outcome,
            rcode=shared.rcode,
            source_type=shared.source_type,
            business_code=shared.business_code,
            message=shared.message,
            context=dict(shared.context),
            answers=copy.deepcopy(shared.answers),
            authorities=copy.deepcopy(shared.authorities),
            ttl_base_ms=shared.ttl_base_ms,
            cache=copy.deepcopy(shared.cache),
            wait_ms=wait_ms,
            merge_decision=merge_decision,
        )

    async def _execute_follower(
        self,
        flight: Flight,
        claim: Claim,
        deadline_ms: int,
        round_id: str,
        trace: dict[str, Any],
        *,
        source: str,
    ) -> ItemResult:
        spec = claim.spec
        started = now_ms()
        checks = 0
        while True:
            now = now_ms()
            remaining = deadline_ms - now
            if flight.future.done():
                shared = flight.future.result()
                wait_ms = max(0, now_ms() - started)
                decision = self._merge_decision(
                    flight,
                    decision="reused_shared_result",
                    reason="leader_completed_before_round_deadline",
                    remaining_ms=remaining,
                    projected_shared_finish_ms=None,
                    checks=checks,
                )
                decision["follower_wait_ms"] = max(0, now_ms() - started)
                await self._complete_follower_session(spec, shared)
                trace["waiting"].append(
                    {
                        "query_id": spec.query_id,
                        "cache_key": spec.cache_key,
                        "kind": "merge_wait",
                        "source": source,
                        "waited_ms": wait_ms,
                        "outcome": "shared_result",
                    }
                )
                trace["merge_decisions"].append({"query_id": spec.query_id, **decision})
                return self._item_from_shared(
                    spec,
                    shared,
                    outcome="merged",
                    wait_ms=wait_ms,
                    merge_decision=decision,
                )
            if remaining <= 0:
                shared = SharedResolution(
                    cache_key=spec.cache_key,
                    business_code="E_BUDGET_EXHAUSTED",
                    message="round budget expired while waiting for the shared upstream result",
                    context={
                        "reason": "merge_wait_exceeded_round_budget",
                        "wait_ms": max(0, now_ms() - started),
                        "remaining_budget_ms": 0,
                        "leader_query_id": flight.leader_query_id,
                    },
                )
                await self._complete_failed_session(spec, shared)
                decision = self._merge_decision(
                    flight,
                    decision="skipped",
                    reason="merge_wait_exceeded_round_budget",
                    remaining_ms=0,
                    projected_shared_finish_ms=None,
                    checks=checks,
                )
                trace["skipped"].append(
                    {
                        "query_id": spec.query_id,
                        "cache_key": spec.cache_key,
                        "code": "E_BUDGET_EXHAUSTED",
                        "reason": "merge_wait_exceeded_round_budget",
                        "at_ms": now_ms(),
                        **shared.context,
                    }
                )
                trace["waiting"].append(
                    {
                        "query_id": spec.query_id,
                        "cache_key": spec.cache_key,
                        "kind": "merge_wait",
                        "source": source,
                        "waited_ms": max(0, now_ms() - started),
                        "outcome": "budget_exhausted",
                    }
                )
                trace["merge_decisions"].append({"query_id": spec.query_id, **decision})
                return self._item_from_shared(
                    spec,
                    shared,
                    outcome="error",
                    wait_ms=max(0, now_ms() - started),
                    merge_decision=decision,
                )

            projected_finish = self._projected_leader_finish(flight)
            reserve = self.config.scheduler.merge_split_reserve_ms
            eligible = (
                self.config.scheduler.merge_split_enabled
                and flight.started_ms is not None
                and checks > 0
                and now - flight.created_ms >= self.config.scheduler.merge_split_after_ms
                and remaining >= self.config.scheduler.min_split_budget_ms
                and projected_finish is not None
                and projected_finish > deadline_ms - reserve
            )
            if eligible and await self.registry.claim_split(flight):
                decision = self._merge_decision(
                    flight,
                    decision="split",
                    reason="projected_shared_finish_would_miss_round_budget_reserve",
                    remaining_ms=remaining,
                    projected_shared_finish_ms=projected_finish,
                    checks=checks,
                )
                decision["follower_wait_ms"] = max(0, now_ms() - started)
                trace["merge_decisions"].append({"query_id": spec.query_id, **decision})
                trace["waiting"].append(
                    {
                        "query_id": spec.query_id,
                        "cache_key": spec.cache_key,
                        "kind": "merge_wait",
                        "source": source,
                        "waited_ms": max(0, now_ms() - started),
                        "outcome": "split_dispatch",
                    }
                )
                return await self._execute_split(flight, claim, deadline_ms, round_id, trace, decision)

            interval_ms = min(
                remaining,
                max(1, self.config.scheduler.merge_split_after_ms),
            )
            await asyncio.wait({flight.future}, timeout=interval_ms / 1000.0)
            checks += 1

    def _projected_leader_finish(self, flight: Flight) -> int | None:
        if flight.started_ms is None:
            return None
        return flight.started_ms + self.config.scheduler.upstream_timeout_ms

    @staticmethod
    def _merge_decision(
        flight: Flight,
        *,
        decision: str,
        reason: str,
        remaining_ms: int,
        projected_shared_finish_ms: int | None,
        checks: int,
    ) -> dict[str, Any]:
        now = now_ms()
        return {
            "decision": decision,
            "reason": reason,
            "rule": "wait unless conservative shared-finish ETA crosses round_deadline - reserve and a split token is available",
            "leader_query_id": flight.leader_query_id,
            "leader_started_ms": flight.started_ms,
            "leader_age_ms": max(0, now - flight.created_ms),
            "remaining_budget_ms": max(0, remaining_ms),
            "projected_shared_finish_ms": projected_shared_finish_ms,
            "projected_shared_wait_ms": None
            if projected_shared_finish_ms is None
            else max(0, projected_shared_finish_ms - now),
            "check_iterations": checks,
            "split_count": flight.split_count,
        }

    async def _execute_split(
        self,
        flight: Flight,
        claim: Claim,
        deadline_ms: int,
        round_id: str,
        trace: dict[str, Any],
        decision: dict[str, Any],
    ) -> ItemResult:
        spec = claim.spec

        async def on_dispatch(authority: Any, event: dict[str, Any]) -> None:
            timestamp = now_ms()
            trace["dispatch"].append(
                {
                    **event,
                    "kind": "split",
                    "query_id": spec.query_id,
                    "cache_key": spec.cache_key,
                    "priority": spec.priority_score,
                    "dispatched_at_ms": timestamp,
                    "round_deadline_ms": deadline_ms,
                }
            )

        outcome = await self.resolver.resolve(spec, deadline_ms=deadline_ms, on_dispatch=on_dispatch)
        written_at = now_ms() if outcome.business_code is None else None
        shared = self._shared_from_outcome(spec, outcome, written_at_ms=written_at, cache=None)
        if shared.business_code in {"E_BUDGET_EXHAUSTED", "E_QUEUE_FULL"}:
            trace["skipped"].append(
                {
                    "query_id": spec.query_id,
                    "cache_key": spec.cache_key,
                    "code": shared.business_code,
                    "reason": shared.context.get(
                        "reason",
                        "round_budget_exhausted"
                        if shared.business_code == "E_BUDGET_EXHAUSTED"
                        else "upstream_waiting_queue_full",
                    ),
                    "at_ms": now_ms(),
                    "dispatch_kind": "split",
                    **shared.context,
                }
            )
        if shared.business_code is not None:
            await self._complete_failed_session(spec, shared)
        else:
            # Deliberately do not write this split result: the canonical merged flight
            # remains the cache writer so cache visibility has one deterministic value.
            await self.storage.complete_session(
                spec.query_id,
                state="complete",
                rcode=shared.rcode,
                source_type=shared.source_type,
                business_code=None,
                response=shared.response_payload,
                error=None,
            )
        return self._item_from_shared(
            spec,
            shared,
            outcome="split",
            wait_ms=int(decision.get("follower_wait_ms", 0)),
            merge_decision=decision,
        )

    async def _complete_follower_session(self, spec: QuerySpec, shared: SharedResolution) -> None:
        if shared.business_code is not None:
            await self._complete_failed_session(spec, shared)
            return
        await self.storage.complete_session(
            spec.query_id,
            state="complete",
            rcode=shared.rcode,
            source_type=shared.source_type,
            business_code=None,
            response=shared.response_payload,
            error=None,
        )

    async def resolve_wire(self, packet: bytes, *, transport: str, client: str) -> tuple[bytes, str | None, bool]:
        """Resolve one listener packet. Invalid input never creates a session."""
        from .listener import build_wire_result  # local import avoids a circular module dependency

        query = parse_query_packet(packet)
        question = query.questions[0]
        spec = self.prepare_spec(
            name=question.name.canonical,
            qtype=question.qtype,
            qclass=question.qclass,
            source_transport=transport,
            client=client,
        )
        batch = await self.submit_batch([spec])
        item = batch.items[0]
        return build_wire_result(query, item, transport, self.config.max_udp_response_bytes), item.business_code, True

    async def get_cache_entries(
        self,
        qname: str,
        qtype: int | None = None,
        *,
        include_expired: bool = False,
    ) -> list[dict[str, Any]]:
        canonical = parse_presentation_name(qname).canonical
        entries = await self.storage.get_cache_by_name(canonical, qtype, include_expired=include_expired)
        instant = now_ms()
        if not include_expired:
            await self.storage.cleanup_expired(now=instant)
        return [entry.to_dict(instant) for entry in entries if include_expired or entry.expires_at_ms > instant]

    async def get_cache_key(self, cache_key: str) -> dict[str, Any] | None:
        entry = await self.storage.get_cache(cache_key)
        if entry is None:
            return None
        instant = now_ms()
        if entry.expires_at_ms <= instant:
            await self.storage.cleanup_expired(now=instant)
            return None
        return entry.to_dict(instant)

    async def session(self, query_id: str) -> dict[str, Any] | None:
        return await self.storage.session_with_round(query_id)

    async def round(self, round_id: str) -> dict[str, Any] | None:
        return await self.storage.get_round(round_id)

    async def recovery(self) -> dict[str, Any] | None:
        return await self.storage.latest_recovery()

    async def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "database": str(self.config.db_file),
            "gate": await self.gate.snapshot(),
            "singleflight": await self.registry.snapshot(),
            "upstream": self.upstream.stats.snapshot(),
            "limits": self.config.redacted()["scheduler"],
        }


def _result_counts(items: Iterable[ItemResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        key = item.business_code or item.outcome
        counts[key] = counts.get(key, 0) + 1
    return counts

