from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .config import AppConfig, AuthorityConfig
from .errors import BusinessError
from .models import QuerySpec, now_ms
from .protocol import (
    QTYPE_CNAME,
    QTYPE_SOA,
    RCODE_NXDOMAIN,
    RCODE_NOERROR,
    RCODE_SERVFAIL,
    ResourceRecord,
    parse_presentation_name,
)
from .scheduler import ConcurrencyGate, GateQueueFull, GateTimeout
from .upstream import UpstreamClient, UpstreamInvalid, UpstreamTimeout


@dataclass(slots=True)
class ResolutionOutcome:
    ok: bool
    rcode: int
    source_type: int | None
    answers: list[ResourceRecord] = field(default_factory=list)
    authorities: list[ResourceRecord] = field(default_factory=list)
    ttl_ms: int = 0
    negative_kind: str | None = None
    cacheable: bool = False
    business_code: str | None = None
    message: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    upstream_hops: int = 0
    chain: list[str] = field(default_factory=list)
    final_name: str | None = None

    @property
    def kind(self) -> str:
        return "negative" if self.negative_kind else "positive"

    def error_dict(self) -> dict[str, Any] | None:
        if not self.business_code:
            return None
        return {"code": self.business_code, "message": self.message, "context": self.context}


DispatchCallback = Callable[[AuthorityConfig, dict[str, Any]], Awaitable[None] | None]


class Resolver:
    def __init__(
        self,
        config: AppConfig,
        upstream: UpstreamClient,
        gate: ConcurrencyGate,
    ) -> None:
        self.config = config
        self.upstream = upstream
        self.gate = gate
        self._authorities = sorted(
            config.authorities,
            key=lambda item: max(len(parse_presentation_name(zone).labels) for zone in item.zones),
            reverse=True,
        )

    def route(self, name: str, qclass: int = 1) -> AuthorityConfig | None:
        dns_name = parse_presentation_name(name)
        for authority in self._authorities:
            for zone in authority.zones:
                if dns_name.is_subdomain_of(parse_presentation_name(zone)):
                    return authority
        return None

    async def resolve(
        self,
        spec: QuerySpec,
        *,
        deadline_ms: int,
        on_dispatch: DispatchCallback | None = None,
    ) -> ResolutionOutcome:
        current = parse_presentation_name(spec.name)
        visited = {current.canonical}
        path = [current.canonical]
        answers: list[ResourceRecord] = []
        hops = 0
        max_hops = self.config.max_cname_hops

        while True:
            authority = self.route(current.canonical, spec.qclass)
            if authority is None:
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_SERVFAIL,
                    source_type=None,
                    business_code="E_NO_AUTHORITY",
                    message="no configured authority owns the queried name",
                    context={"name": current.canonical, "configured_zones": [z for a in self._authorities for z in a.zones]},
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )

            wait_started = asyncio.get_running_loop().time()
            remaining_round_ms = deadline_ms - now_ms()
            if remaining_round_ms <= 0:
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_SERVFAIL,
                    source_type=None,
                    business_code="E_BUDGET_EXHAUSTED",
                    message="round budget expired before the upstream query could be dispatched",
                    context={
                        "reason": "round_budget_expired_before_dispatch",
                        "remaining_budget_ms": 0,
                        "authority_id": authority.id,
                        "socket_opened": False,
                    },
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )
            configured_timeout_ms = self.config.scheduler.upstream_timeout_ms
            effective_timeout_ms = min(configured_timeout_ms, max(1, remaining_round_ms))
            try:
                async with self.gate.lease(authority.id, deadline_ms=deadline_ms):
                    waited_ms = int((asyncio.get_running_loop().time() - wait_started) * 1000)
                    if on_dispatch is not None:
                        event = {
                            "authority_id": authority.id,
                            "authority": f"{authority.host}:{authority.port}",
                            "name": current.canonical,
                            "type": spec.qtype,
                            "hop": hops,
                            "queue_wait_ms": waited_ms,
                        }
                        maybe = on_dispatch(authority, event)
                        if asyncio.iscoroutine(maybe):
                            await maybe
                    response = await self.upstream.query(
                        authority,
                        current.canonical,
                        spec.qtype,
                        spec.qclass,
                        timeout_ms=effective_timeout_ms,
                        timeout_is_round_budget=effective_timeout_ms < configured_timeout_ms,
                    )
            except GateTimeout as exc:
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_SERVFAIL,
                    source_type=None,
                    business_code="E_BUDGET_EXHAUSTED",
                    message="round budget expired while waiting for an upstream concurrency slot",
                    context=exc.context(),
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )
            except GateQueueFull as exc:
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_SERVFAIL,
                    source_type=None,
                    business_code="E_QUEUE_FULL",
                    message="bounded upstream waiting queue is full",
                    context=exc.context(),
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )
            except UpstreamTimeout as exc:
                if exc.timeout_ms < configured_timeout_ms:
                    return ResolutionOutcome(
                        ok=False,
                        rcode=RCODE_SERVFAIL,
                        source_type=None,
                        business_code="E_BUDGET_EXHAUSTED",
                        message="round budget expired during an active upstream query; the socket was closed",
                        context={
                            "reason": "round_budget_expired_during_active_upstream_query",
                            "configured_upstream_timeout_ms": configured_timeout_ms,
                            "effective_timeout_ms": exc.timeout_ms,
                            "socket_closed": True,
                            "authority_id": authority.id,
                        },
                        upstream_hops=hops + 1,
                        chain=path,
                        final_name=current.canonical,
                    )
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_SERVFAIL,
                    source_type=None,
                    business_code="E_UPSTREAM_TIMEOUT",
                    message="upstream DNS query exceeded its per-hop timeout",
                    context=exc.context(),
                    upstream_hops=hops + 1,
                    chain=path,
                    final_name=current.canonical,
                )
            except UpstreamInvalid as exc:
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_SERVFAIL,
                    source_type=None,
                    business_code="E_UPSTREAM_INVALID",
                    message=exc.message,
                    context=exc.context,
                    upstream_hops=hops + 1,
                    chain=path,
                    final_name=current.canonical,
                )
            except BusinessError as exc:
                return ResolutionOutcome(
                    ok=False,
                    rcode=exc.dns_rcode,
                    source_type=None,
                    business_code=exc.code,
                    message=exc.message,
                    context=exc.context,
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # defensive conversion at the boundary
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_SERVFAIL,
                    source_type=None,
                    business_code="E_INTERNAL",
                    message=str(exc),
                    context={"exception_type": type(exc).__name__, "authority_id": authority.id},
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )

            hops += 1
            message = response.message
            if message.rcode == RCODE_NXDOMAIN:
                soa = self._find_soa(message.authorities) or self._find_soa(message.answers)
                negative_ttl = self._negative_ttl(soa)
                ttl = min([negative_ttl] + [rr.ttl_ms for rr in answers]) if answers else negative_ttl
                return ResolutionOutcome(
                    ok=False,
                    rcode=RCODE_NXDOMAIN,
                    source_type=QTYPE_SOA,
                    answers=answers,
                    authorities=[soa.with_ttl_ms(ttl)] if soa is not None else [],
                    ttl_ms=ttl,
                    negative_kind="nxdomain",
                    cacheable=soa is not None and ttl > 0,
                    business_code=None,
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )
            if message.rcode != RCODE_NOERROR:
                return ResolutionOutcome(
                    ok=False,
                    rcode=message.rcode,
                    source_type=None,
                    business_code="E_UPSTREAM_RCODE",
                    message=f"authoritative server returned DNS RCODE {message.rcode}",
                    context={"authority_id": authority.id, "rcode": message.rcode, "name": current.canonical},
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )

            cname = self._matching_record(message.answers, current, QTYPE_CNAME, spec.qclass)
            direct = [rr for rr in message.answers if self._owner_matches(rr, current) and rr.qclass == spec.qclass and rr.rtype == spec.qtype]

            if spec.qtype == QTYPE_CNAME and cname is not None:
                return ResolutionOutcome(
                    ok=True,
                    rcode=RCODE_NOERROR,
                    source_type=QTYPE_CNAME,
                    answers=answers + [cname],
                    ttl_ms=min((rr.ttl_ms for rr in answers + [cname]), default=0),
                    cacheable=True,
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )

            if cname is not None:
                target = cname.rdata.get("target")
                if target is None:
                    return ResolutionOutcome(
                        ok=False,
                        rcode=RCODE_SERVFAIL,
                        source_type=None,
                        business_code="E_UPSTREAM_INVALID",
                        message="CNAME record has no target",
                        context={"authority_id": authority.id, "name": current.canonical},
                        upstream_hops=hops,
                        chain=path,
                        final_name=current.canonical,
                    )
                target_name = parse_presentation_name(str(target))
                answers = answers + [cname]
                if target_name.canonical in visited:
                    return ResolutionOutcome(
                        ok=False,
                        rcode=RCODE_SERVFAIL,
                        source_type=None,
                        business_code="E_CNAME_LOOP",
                        message="CNAME chain contains a loop",
                        context={"chain": path + [target_name.canonical], "loop_at": target_name.canonical},
                        upstream_hops=hops,
                        chain=path + [target_name.canonical],
                        final_name=target_name.canonical,
                    )
                if len(answers) > max_hops:
                    return ResolutionOutcome(
                        ok=False,
                        rcode=RCODE_SERVFAIL,
                        source_type=None,
                        business_code="E_CHAIN_TOO_LONG",
                        message="CNAME chain exceeds configured maximum hops",
                        context={"cname_hops": len(answers), "max_cname_hops": max_hops, "chain": path + [target_name.canonical]},
                        upstream_hops=hops,
                        chain=path + [target_name.canonical],
                        final_name=target_name.canonical,
                    )
                visited.add(target_name.canonical)
                path.append(target_name.canonical)
                current = target_name
                continue

            if direct:
                final_answers = answers + direct
                ttl = min((rr.ttl_ms for rr in final_answers), default=0)
                return ResolutionOutcome(
                    ok=True,
                    rcode=RCODE_NOERROR,
                    source_type=spec.qtype,
                    answers=final_answers,
                    ttl_ms=ttl,
                    cacheable=ttl > 0,
                    upstream_hops=hops,
                    chain=path,
                    final_name=current.canonical,
                )

            soa = self._find_soa(message.authorities) or self._find_soa(message.answers)
            ttl = self._negative_ttl(soa)
            return ResolutionOutcome(
                ok=False,
                rcode=RCODE_NOERROR,
                source_type=QTYPE_SOA,
                answers=answers,
                authorities=[soa.with_ttl_ms(ttl)] if soa is not None else [],
                ttl_ms=min([ttl] + [rr.ttl_ms for rr in answers]) if answers else ttl,
                negative_kind="nodata",
                cacheable=soa is not None and ttl > 0,
                business_code=None,
                upstream_hops=hops,
                chain=path,
                final_name=current.canonical,
            )

    @staticmethod
    def _owner_matches(record: ResourceRecord, name: Any) -> bool:
        return record.name.canonical == name.canonical

    @classmethod
    def _matching_record(
        cls,
        records: list[ResourceRecord],
        name: Any,
        rtype: int,
        qclass: int,
    ) -> ResourceRecord | None:
        for record in records:
            if cls._owner_matches(record, name) and record.rtype == rtype and record.qclass == qclass:
                return record
        return None

    @staticmethod
    def _find_soa(records: list[ResourceRecord]) -> ResourceRecord | None:
        for record in records:
            if record.rtype == QTYPE_SOA:
                return record
        return None

    @staticmethod
    def _negative_ttl(soa: ResourceRecord | None) -> int:
        if soa is None:
            return 0
        minimum = int(soa.rdata.get("minimum", 0)) * 1000
        return max(0, min(soa.ttl_ms, minimum))

