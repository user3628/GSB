"""Recursive CNAME traversal, negative caching rules and in-flight suppression."""

from __future__ import annotations

import threading
from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import dataclass
from typing import Any

from dnslib import CLASS, QTYPE

from .cache import CacheStore
from .codec import (
    QueryKey,
    RCODE_NOERROR,
    RCODE_NXDOMAIN,
    canonical_name,
    labels_equal,
    labels_key,
)
from .errors import (
    BusinessError,
    E_CHAIN_TOO_LONG,
    E_CNAME_LOOP,
    E_INFLIGHT_TIMEOUT,
    E_NEGATIVE_WITHOUT_SOA,
    E_UPSTREAM_MALFORMED_MESSAGE,
    E_UPSTREAM_REFUSED,
    E_UPSTREAM_RCODE,
    E_UPSTREAM_SERVFAIL,
    rcode_for_error,
)
from .model import Record, Resolution
from .stats import Stats
from .upstream import UpstreamClient


MAX_NEGATIVE_TTL = 604800


@dataclass(frozen=True)
class ResolverConfig:
    max_cname_hops: int = 4
    inflight_timeout: float = 30.0


class RecursiveResolver:
    def __init__(
        self,
        cache: CacheStore,
        upstream: UpstreamClient,
        stats: Stats | None = None,
        config: ResolverConfig | None = None,
    ) -> None:
        self.cache = cache
        self.upstream = upstream
        self.stats = stats or Stats()
        self.config = config or ResolverConfig()
        self._inflight_lock = threading.RLock()
        self._inflight: dict[QueryKey, Future[Resolution]] = {}

    def inflight_count(self) -> int:
        with self._inflight_lock:
            return len(self._inflight)

    def resolve(
        self,
        key: QueryKey,
        labels: tuple[bytes, ...],
        qtype: int,
        qclass: int = int(CLASS.IN),
    ) -> Resolution:
        self.stats.inc("resolve_requests")
        cached = self.cache.get(key)
        if cached is not None:
            self.stats.inc("cache_hit")
            return cached

        with self._inflight_lock:
            future = self._inflight.get(key)
            leader = future is None
            if leader:
                future = Future()
                self._inflight[key] = future
        assert future is not None

        if not leader:
            self.stats.inc("inflight_waiters")
            try:
                return future.result(timeout=self.config.inflight_timeout)
            except FutureTimeout as exc:
                self.stats.error(E_INFLIGHT_TIMEOUT)
                raise BusinessError(E_INFLIGHT_TIMEOUT, "in-flight owner did not finish") from exc

        try:
            result = self._resolve_uncached(key, labels, qtype, qclass)
            future.set_result(result)
            return result
        except BusinessError as exc:
            result = error_resolution(exc.code, exc.detail)
            self.stats.error(exc.code)
            future.set_result(result)
            return result
        except Exception as exc:
            result = error_resolution("E_INTERNAL", str(exc))
            self.stats.error("E_INTERNAL")
            future.set_result(result)
            return result
        finally:
            with self._inflight_lock:
                self._inflight.pop(key, None)
            if not future.done():
                future.set_result(error_resolution("E_INTERNAL", "resolution ended without result"))

    def _resolve_uncached(
        self,
        key: QueryKey,
        labels: tuple[bytes, ...],
        qtype: int,
        qclass: int,
    ) -> Resolution:
        session_id = self.cache.start_session(key, canonical_name(labels))
        try:
            resolution = self._walk(key, labels, qtype, qclass, session_id)
            self.cache.commit_resolution(session_id, key, resolution)
            self.stats.inc("cache_commits")
            return resolution
        except BusinessError as exc:
            self.cache.fail_session(session_id, exc.code, exc.detail)
            raise
        except Exception as exc:
            self.cache.fail_session(session_id, "E_INTERNAL", str(exc))
            raise

    def _walk(
        self,
        key: QueryKey,
        labels: tuple[bytes, ...],
        qtype: int,
        qclass: int,
        session_id: int,
    ) -> Resolution:
        current = tuple(bytes(label) for label in labels)
        seen = {labels_key(current)}
        answers: list[Record] = []
        response = None
        chain_steps = 0
        upstream_queries = 0

        # A response can contain several CNAMEs. The +2 gives the loop enough
        # iterations to consume a fully delivered maximum-length chain.
        for _ in range(self.config.max_cname_hops + 2):
            if response is None:
                response = self.upstream.query(current, int(qtype), int(qclass))
                upstream_queries += 1
                self.stats.inc("upstream_queries")
                self.cache.update_session_progress(
                    session_id, chain_steps, canonical_name(current)
                )

            cname = next(
                (
                    record
                    for record in response.answers
                    if record.rtype == QTYPE.CNAME and labels_equal(record.owner, current)
                ),
                None,
            )
            final = [
                record
                for record in response.answers
                if record.rtype == int(qtype) and labels_equal(record.owner, current)
            ]

            if qtype == QTYPE.CNAME:
                if cname is not None:
                    return Resolution(
                        kind="positive",
                        rcode=RCODE_NOERROR,
                        answers=tuple(answers) + (cname,),
                        upstream_queries=upstream_queries,
                    )
            elif final:
                return Resolution(
                    kind="positive",
                    rcode=RCODE_NOERROR,
                    answers=tuple(answers) + tuple(final),
                    upstream_queries=upstream_queries,
                )

            if cname is not None:
                if chain_steps + 1 > self.config.max_cname_hops:
                    raise BusinessError(
                        E_CHAIN_TOO_LONG,
                        f"CNAME chain exceeds max_hops={self.config.max_cname_hops}",
                    )
                target = tuple(bytes(label) for label in _record_target(cname))
                answers.append(cname)
                chain_steps += 1
                self.stats.inc("cname_follows")
                if labels_key(target) in seen:
                    raise BusinessError(E_CNAME_LOOP, canonical_name(target))
                seen.add(labels_key(target))
                current = target
                if any(labels_equal(record.owner, current) for record in response.answers):
                    continue
                response = None
                continue

            if response.rcode == RCODE_NXDOMAIN:
                return self._make_negative(
                    "nxdomain", RCODE_NXDOMAIN, answers, response.authority, upstream_queries
                )
            if response.rcode == RCODE_NOERROR:
                return self._make_negative(
                    "nodata", RCODE_NOERROR, answers, response.authority, upstream_queries
                )
            raise BusinessError(_rcode_error(response.rcode), f"upstream rcode={response.rcode}")

        raise BusinessError(
            E_CHAIN_TOO_LONG,
            f"CNAME traversal exceeded max_hops={self.config.max_cname_hops}",
        )

    def _make_negative(
        self,
        kind: str,
        rcode: int,
        answers: list[Record],
        authority: tuple[Record, ...],
        upstream_queries: int,
    ) -> Resolution:
        soa = next((record for record in authority if record.rtype == QTYPE.SOA), None)
        if soa is None:
            raise BusinessError(E_NEGATIVE_WITHOUT_SOA, f"{kind} response has no SOA")
        minimum = int(soa.data["times"][4])
        if kind == "nxdomain":
            negative_ttl = minimum
            source = "soa.minimum"
        else:
            negative_ttl = min(int(soa.ttl), minimum)
            source = "min(soa.ttl,soa.minimum)"
        negative_ttl = max(1, min(int(negative_ttl), MAX_NEGATIVE_TTL))
        soa_for_response = soa.with_ttl(negative_ttl)
        return Resolution(
            kind="negative",
            rcode=rcode,
            answers=tuple(answers),
            authority=(soa_for_response,),
            negative_ttl=negative_ttl,
            negative_ttl_source=source,
            upstream_queries=upstream_queries,
        )


def _record_target(record: Record) -> tuple[bytes, ...]:
    from .model import labels_from_json

    return labels_from_json(record.data["target"])


def _rcode_error(rcode: int) -> str:
    if rcode == 5:
        return E_UPSTREAM_REFUSED
    if rcode == 2:
        return E_UPSTREAM_SERVFAIL
    if rcode == 1:
        return E_UPSTREAM_MALFORMED_MESSAGE
    return E_UPSTREAM_RCODE


def error_resolution(code: str, detail: str = "") -> Resolution:
    return Resolution(
        kind="error",
        rcode=rcode_for_error(code),
        business_code=code,
        error_detail=detail or None,
    )
