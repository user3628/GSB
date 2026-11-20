from __future__ import annotations

import socket
import struct
import threading
import time

from dnsmw.client import ask, query_udp, make_query


def test_udp_truncation_tcp_retransmission_edns_and_one_cache_fill(stack):
    legacy = ask("127.0.0.1", stack.dns_port, "big.example.test.", "TXT", retry_tcp=False, timeout=3)
    assert legacy.transport == "udp"
    assert legacy.response.header.tc == 1
    assert len(legacy.response.rr) == 0
    assert len(legacy.wire) <= 512

    tcp = ask("127.0.0.1", stack.tcp_port, "big.example.test.", "TXT", tcp=True, timeout=3)
    assert tcp.response.header.tc == 0
    assert len(tcp.response.rr) == 14

    edns = ask(
        "127.0.0.1",
        stack.dns_port,
        "big.example.test.",
        "TXT",
        edns_payload=4096,
        retry_tcp=False,
        timeout=3,
    )
    assert edns.response.header.tc == 0
    assert len(edns.response.rr) == 14
    assert edns.response.ar[0].rclass == 4096
    assert stack.auth.zone.query_count("big.example.test.") == 1


def test_cname_chain_and_case_insensitive_cache_hit(stack):
    first = ask("127.0.0.1", stack.dns_port, "cHaIn.ExAmPlE.TeSt.", "A", timeout=3)
    assert [rr.rtype for rr in first.response.rr] == [5, 5, 1]
    assert [rr.ttl for rr in first.response.rr] == [300, 120, 30]
    upstream_before = stack.service.stats.snapshot()["counters"]["upstream_queries"]
    time.sleep(1.05)
    second = ask("127.0.0.1", stack.dns_port, "chain.example.test.", "A", timeout=3)
    assert [rr.ttl for rr in second.response.rr] == [299, 119, 29]
    assert stack.service.stats.snapshot()["counters"]["upstream_queries"] == upstream_before
    assert stack.auth.zone.query_count("chain.example.test.") == 1
    assert len([row for row in stack.service.cache.snapshot() if row["key"] == "chain.example.test."]) == 1


def test_authoritative_answer_contains_real_compression_pointer(stack):
    result = ask("127.0.0.1", stack.dns_port, "www.example.test.", "A", timeout=3)
    assert result.response.header.tc == 0
    assert result.response.rr[0].rname.label == (b"www", b"example", b"test")
    # Header 12 + question name 18 + QTYPE/QCLASS 4 => answer owner at offset 34.
    assert result.wire[34:36] == b"\xc0\x0c"


def test_nxdomain_and_nodata_cache_hits_with_soa_ttl(stack):
    nx = ask("127.0.0.1", stack.dns_port, "MiSsInG.Example.Test.", "A", timeout=3)
    assert nx.response.header.rcode == 3
    assert len(nx.response.auth) == 1
    assert nx.response.auth[0].rtype == 6
    assert nx.response.auth[0].ttl == 45
    time.sleep(1.05)
    nx_hit = ask("127.0.0.1", stack.dns_port, "missing.example.test.", "A", timeout=3)
    assert nx_hit.response.header.rcode == 3
    assert nx_hit.response.auth[0].ttl == 44
    assert stack.auth.zone.query_count("missing.example.test.") == 1

    nodata = ask("127.0.0.1", stack.dns_port, "WwW.Example.Test.", "MX", timeout=3)
    assert nodata.response.header.rcode == 0
    assert len(nodata.response.rr) == 0
    assert nodata.response.auth[0].rtype == 6
    assert nodata.response.auth[0].ttl == 15
    time.sleep(1.05)
    nodata_hit = ask("127.0.0.1", stack.dns_port, "www.example.test.", "MX", timeout=3)
    assert nodata_hit.response.auth[0].ttl == 14
    assert stack.auth.zone.query_count("www.example.test.") == 1


def test_malformed_udp_returns_no_answer_and_records_business_code(stack):
    header = struct.pack("!HHHHHH", 0x7777, 0x0100, 1, 0, 0, 0)
    malformed = header + b"\xc0\x0c\x00\x01\x00\x01"  # qname points to itself
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)
    try:
        sock.sendto(malformed, ("127.0.0.1", stack.dns_port))
        response, _ = sock.recvfrom(65535)
    finally:
        sock.close()
    assert struct.unpack_from("!H", response, 0)[0] == 0x7777
    assert struct.unpack_from("!H", response, 4)[0] == 0  # QDCOUNT
    assert struct.unpack_from("!H", response, 6)[0] == 0  # ANCOUNT
    assert response[3] & 0x0F == 1  # FORMERR
    assert stack.service.stats.snapshot()["errors"]["E_MALFORMED_MESSAGE"] == 1
    assert stack.service.cache.snapshot() == []


def test_upstream_timeout_uses_servfail_and_releases_sockets(stack):
    started = time.monotonic()
    result = ask(
        "127.0.0.1",
        stack.dns_port,
        "timeout.example.test.",
        "A",
        edns_payload=1232,
        timeout=3,
    )
    elapsed = time.monotonic() - started
    assert elapsed < 1.2
    assert result.response.header.rcode == 2
    assert result.business_code == "E_UPSTREAM_TIMEOUT"
    assert stack.service.upstream.stats()["active_sockets"] == 0
    assert stack.service.upstream.stats()["sockets_opened"] == stack.service.upstream.stats()["sockets_closed"]
    assert all("timeout.example.test." != row["key"] for row in stack.service.cache.snapshot())


def test_concurrent_repeated_queries_are_suppressed(stack):
    barrier = threading.Barrier(10)
    results = []
    failures: list[BaseException] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            barrier.wait(timeout=3)
            result = ask(
                "127.0.0.1",
                stack.dns_port,
                "CONCURRENT.example.test." if index % 2 else "concurrent.Example.Test.",
                "A",
                timeout=3,
            )
            with lock:
                results.append(result)
        except BaseException as exc:  # pragma: no cover - surfaced below
            with lock:
                failures.append(exc)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=6)
    assert not failures
    assert len(results) == 10
    assert all(result.response.rr[0].rdata.data == (192, 0, 2, 77) for result in results)
    assert stack.auth.zone.query_count("concurrent.example.test.") == 1
    assert stack.service.stats.snapshot()["counters"]["inflight_waiters"] >= 8
