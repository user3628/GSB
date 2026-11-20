from __future__ import annotations

import threading
import time

import pytest
from dnslib import QTYPE

from dnsmw.authserver import ZONE
from dnsmw.cache import CacheStore
from dnsmw.codec import QueryKey
from dnsmw.errors import (
    E_CHAIN_TOO_LONG,
    E_CNAME_LOOP,
    E_UPSTREAM_MALFORMED_MESSAGE,
    E_UPSTREAM_REFUSED,
    E_UPSTREAM_TIMEOUT,
)
from dnsmw.resolver import RecursiveResolver, ResolverConfig
from dnsmw.stats import Stats
from dnsmw.upstream import UpstreamClient


def _labels(name: str) -> tuple[bytes, ...]:
    return tuple(part.encode() for part in name.rstrip(".").split("."))


def _resolver(tmp_path, auth, *, hops=4, timeout=0.35):
    cache = CacheStore(tmp_path / "resolver.sqlite3")
    upstream = UpstreamClient("127.0.0.1", auth.port, timeout=timeout)
    stats = Stats()
    resolver = RecursiveResolver(
        cache, upstream, stats, ResolverConfig(max_cname_hops=hops, inflight_timeout=5)
    )
    return cache, upstream, stats, resolver


def _resolve(resolver, name: str, qtype=QTYPE.A, labels=None):
    actual = labels or _labels(name)
    key = QueryKey.from_labels(actual, qtype, 1)
    return resolver.resolve(key, actual, qtype, 1)


def test_cname_chain_preserves_each_ttl_and_reuses_cache(tmp_path, stack):
    cache, upstream, stats, resolver = _resolver(tmp_path, stack.auth)
    try:
        first = _resolve(resolver, "cHaIn.ExAmPlE.TeSt.")
        assert first.kind == "positive"
        assert [record.type_name for record in first.answers] == ["CNAME", "CNAME", "A"]
        assert [record.ttl for record in first.answers] == [300, 120, 30]
        assert stats.snapshot()["counters"]["upstream_queries"] == 3

        time.sleep(1.05)
        second = _resolve(resolver, "chain.example.test.")
        assert second.cached is True
        assert [record.ttl for record in second.answers] == [299, 119, 29]
        assert stats.snapshot()["counters"]["upstream_queries"] == 3
    finally:
        cache.close()


def test_negative_cache_uses_distinct_sources_and_returns_rcode(tmp_path, stack):
    cache, _, stats, resolver = _resolver(tmp_path, stack.auth)
    try:
        nx1 = _resolve(resolver, "missing.example.test.")
        assert nx1.rcode == 3
        assert nx1.negative_ttl == 45
        assert nx1.negative_ttl_source == "soa.minimum"
        assert nx1.authority[0].ttl == 45

        nodata = _resolve(resolver, "www.example.test.", QTYPE.MX)
        assert nodata.rcode == 0
        assert nodata.kind == "negative"
        assert nodata.negative_ttl == 15
        assert nodata.negative_ttl_source == "min(soa.ttl,soa.minimum)"
        assert nodata.authority[0].ttl == 15

        nx2 = _resolve(resolver, "MiSsInG.Example.Test.")
        assert nx2.cached is True
        assert nx2.rcode == 3
        assert stack.auth.zone.query_count("missing.example.test.") == 1
    finally:
        cache.close()


@pytest.mark.parametrize(
    ("name", "code"),
    [
        ("loop1.example.test.", E_CNAME_LOOP),
        ("long0.example.test.", E_CHAIN_TOO_LONG),
        ("refused.example.test.", E_UPSTREAM_REFUSED),
        ("malformed.example.test.", E_UPSTREAM_MALFORMED_MESSAGE),
    ],
)
def test_failures_have_business_codes_and_are_not_cached(tmp_path, stack, name, code):
    cache, _, stats, resolver = _resolver(tmp_path, stack.auth)
    try:
        result = _resolve(resolver, name)
        assert result.business_code == code
        assert result.rcode == 2
        assert cache.stats()["entries"] == 0
        assert stats.snapshot()["errors"][code] == 1
    finally:
        cache.close()


def test_upstream_timeout_closes_sockets_and_blocks_cache(tmp_path, stack):
    cache, upstream, stats, resolver = _resolver(tmp_path, stack.auth, timeout=0.2)
    try:
        started = time.monotonic()
        result = _resolve(resolver, "timeout.example.test.")
        elapsed = time.monotonic() - started
        assert result.business_code == E_UPSTREAM_TIMEOUT
        assert result.rcode == 2
        assert elapsed < 1.2
        upstream_stats = upstream.stats()
        assert upstream_stats["active_sockets"] == 0
        assert upstream_stats["sockets_opened"] == upstream_stats["sockets_closed"]
        assert cache.stats()["entries"] == 0
    finally:
        cache.close()


def test_concurrent_mixed_case_queries_trigger_one_upstream_resolution(tmp_path, stack):
    cache, upstream, stats, resolver = _resolver(tmp_path, stack.auth, timeout=2.0)
    variants = [
        "concurrent.example.test.",
        "CONCURRENT.EXAMPLE.TEST.",
        "Concurrent.Example.Test.",
        "cOnCuRrEnT.eXaMpLe.tEsT.",
    ]
    barrier = threading.Barrier(12)
    results = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            barrier.wait(timeout=3)
            result = _resolve(resolver, variants[index % len(variants)])
            with lock:
                results.append(result)
        except BaseException as exc:  # pragma: no cover - surfaced below
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(12)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=6)
        assert not errors
        assert len(results) == 12
        assert all(result.answers[-1].data["address"] == "192.0.2.77" for result in results)
        assert stack.auth.zone.query_count("concurrent.example.test.") == 1
        snapshot = stats.snapshot()
        assert snapshot["counters"]["upstream_queries"] == 1
        assert snapshot["counters"]["inflight_waiters"] >= 9
        assert upstream.stats()["sockets_opened"] == 1
    finally:
        cache.close()
