#!/usr/bin/env python3
"""Fixed-port loopback demonstration, including a real forced-kill recovery."""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import sqlite3
import struct
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dnslib import QTYPE, DNSRecord  # noqa: E402

from dnsmw.client import ask  # noqa: E402


AUTH_PORT = 15353
RESOLVER_PORT = 15354
ADMIN_PORT = 18080
RUNTIME = ROOT / "runtime"
DB_PATH = RUNTIME / "demo.sqlite3"
RESULTS_PATH = ROOT / "artifacts" / "demo-results.json"


@dataclass
class Check:
    name: str
    passed: bool
    observed: Any
    expected: Any
    elapsed_seconds: float = 0.0


@dataclass
class Demo:
    checks: list[Check] = field(default_factory=list)
    processes: list[subprocess.Popen[bytes]] = field(default_factory=list)
    log_handles: list[Any] = field(default_factory=list)

    def check(self, name: str, passed: bool, observed: Any, expected: Any, elapsed: float = 0.0) -> None:
        item = Check(name, bool(passed), observed, expected, elapsed)
        self.checks.append(item)
        marker = "PASS" if item.passed else "FAIL"
        print(f"[{marker}] {name}: observed={observed!r}", flush=True)

    def run_case(self, name: str, fn: Callable[[], None]) -> None:
        started = time.monotonic()
        try:
            fn()
        except Exception as exc:
            self.check(
                f"{name} execution",
                False,
                {"error": repr(exc), "traceback": traceback.format_exc(limit=8)},
                "no exception",
                time.monotonic() - started,
            )


def port_is_free(host: str, port: int, kind: int) -> bool:
    sock = socket.socket(socket.AF_INET, kind)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def preflight(host: str = "127.0.0.1") -> None:
    checks = [
        ("authoritative UDP", AUTH_PORT, socket.SOCK_DGRAM),
        ("authoritative TCP", AUTH_PORT, socket.SOCK_STREAM),
        ("resolver UDP", RESOLVER_PORT, socket.SOCK_DGRAM),
        ("resolver TCP", RESOLVER_PORT, socket.SOCK_STREAM),
        ("admin TCP", ADMIN_PORT, socket.SOCK_STREAM),
    ]
    busy = [name for name, port, kind in checks if not port_is_free(host, port, kind)]
    if busy:
        raise RuntimeError(f"fixed demonstration ports are busy: {', '.join(busy)}")


def wait_tcp(host: str, port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    raise TimeoutError(f"TCP {host}:{port} did not become ready") from last_error


def http_json(path: str, timeout: float = 3.0) -> Any:
    url = f"http://127.0.0.1:{ADMIN_PORT}{path}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_admin(timeout: float = 8.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            health = http_json("/health", timeout=0.5)
            if health.get("status") == "ok":
                return health
        except Exception as exc:
            last_error = exc
        time.sleep(0.05)
    raise TimeoutError("admin endpoint did not become ready") from last_error


def start_process(
    demo: Demo,
    name: str,
    args: list[str],
    *,
    python: str,
) -> subprocess.Popen[bytes]:
    handle = open(RUNTIME / f"{name}.log", "ab", buffering=0)
    demo.log_handles.append(handle)
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    process = subprocess.Popen(
        [python, *args],
        cwd=str(ROOT),
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    demo.processes.append(process)
    return process


def stop_process(process: subprocess.Popen[bytes] | None, *, force: bool = False) -> None:
    if process is None or process.poll() is not None:
        return
    if force:
        process.kill()
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def resolver_args() -> list[str]:
    return [
        "-m",
        "dnsmw.server",
        "--udp-port",
        str(RESOLVER_PORT),
        "--tcp-port",
        str(RESOLVER_PORT),
        "--upstream-port",
        str(AUTH_PORT),
        "--upstream-timeout",
        "0.8",
        "--max-cname-hops",
        "4",
        "--db",
        str(DB_PATH),
        "--admin-port",
        str(ADMIN_PORT),
        "--purge-interval",
        "0.2",
    ]


def wait_running_crash_session(timeout: float = 4.0) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            connection = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=0.5)
            connection.row_factory = sqlite3.Row
            try:
                row = connection.execute(
                    """
                    SELECT id,cache_key,status,chain_steps,last_upstream_name,started_at
                      FROM resolution_sessions
                     WHERE cache_key='crash0.example.test.'
                       AND status='running'
                       AND chain_steps >= 1
                     ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()
            finally:
                connection.close()
            if row is not None:
                return dict(row)
        except sqlite3.Error:
            pass
        time.sleep(0.02)
    return None


def malformed_udp() -> tuple[bytes, float]:
    packet = struct.pack("!HHHHHH", 0x7777, 0x0100, 1, 0, 0, 0)
    packet += b"\xc0\x0c\x00\x01\x00\x01"  # query name points to itself
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)
    started = time.monotonic()
    try:
        sock.sendto(packet, ("127.0.0.1", RESOLVER_PORT))
        response, _ = sock.recvfrom(65535)
        return response, time.monotonic() - started
    finally:
        sock.close()


def cache_rows(key: str) -> list[dict[str, Any]]:
    return [row for row in http_json("/cache") if row["key"] == key]


def record_summary(response: DNSRecord) -> dict[str, Any]:
    return {
        "rcode": int(response.header.rcode),
        "tc": int(response.header.tc),
        "answers": [
            {"type": int(rr.rtype), "ttl": int(rr.ttl), "rdata": repr(rr.rdata)}
            for rr in response.rr
        ],
        "authority": [
            {"type": int(rr.rtype), "ttl": int(rr.ttl), "rdata": repr(rr.rdata)}
            for rr in response.auth
        ],
        "additional": [{"type": int(rr.rtype), "class": int(rr.rclass)} for rr in response.ar],
    }


def run(args: argparse.Namespace) -> int:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()
    for log_name in ("auth.log", "resolver.log", "resolver-restarted.log"):
        path = RUNTIME / log_name
        if path.exists():
            path.unlink()

    preflight()
    demo = Demo()
    auth: subprocess.Popen[bytes] | None = None
    resolver: subprocess.Popen[bytes] | None = None
    restarted: subprocess.Popen[bytes] | None = None
    crash_worker: threading.Thread | None = None

    try:
        auth = start_process(demo, "auth", ["-m", "dnsmw.authserver", "--port", str(AUTH_PORT)], python=args.python)
        wait_tcp("127.0.0.1", AUTH_PORT)
        resolver = start_process(demo, "resolver", resolver_args(), python=args.python)
        health = wait_admin()
        demo.check(
            "fixed-port startup",
            health["udp_port"] == RESOLVER_PORT
            and health["tcp_port"] == RESOLVER_PORT
            and health["admin_port"] == ADMIN_PORT,
            health,
            {"udp": RESOLVER_PORT, "tcp": RESOLVER_PORT, "admin": ADMIN_PORT},
        )

        def truncation_case() -> None:
            started = time.monotonic()
            upstream_before = int(http_json("/stats")["upstream"]["queries"])
            legacy = ask(
                "127.0.0.1",
                RESOLVER_PORT,
                "big.example.test.",
                "TXT",
                retry_tcp=False,
                timeout=3,
            )
            demo.check(
                "UDP legacy 512-byte truncation",
                legacy.response.header.tc == 1 and len(legacy.response.rr) == 0 and len(legacy.wire) <= 512,
                {"tc": int(legacy.response.header.tc), "answers": len(legacy.response.rr), "bytes": len(legacy.wire)},
                {"tc": 1, "answers": 0, "max_bytes": 512},
                time.monotonic() - started,
            )
            tcp = ask("127.0.0.1", RESOLVER_PORT, "big.example.test.", "TXT", tcp=True, timeout=3)
            demo.check(
                "TCP retransmission returns complete RRset",
                tcp.response.header.tc == 0 and len(tcp.response.rr) == 14 and len(tcp.wire) > 512,
                {"tc": int(tcp.response.header.tc), "answers": len(tcp.response.rr), "bytes": len(tcp.wire)},
                {"tc": 0, "answers": 14, "min_bytes": 513},
            )
            edns = ask(
                "127.0.0.1",
                RESOLVER_PORT,
                "big.example.test.",
                "TXT",
                edns_payload=4096,
                retry_tcp=False,
                timeout=3,
            )
            demo.check(
                "EDNS(0) 4096-byte UDP avoids truncation and echoes OPT",
                edns.response.header.tc == 0
                and len(edns.response.rr) == 14
                and len(edns.response.ar) == 1
                and int(edns.response.ar[0].rclass) == 4096,
                {**record_summary(edns.response), "wire_bytes": len(edns.wire)},
                {"tc": 0, "answers": 14, "opt_udp_size": 4096},
            )
            demo.check(
                "UDP/TCP/EDNS share one semantic cache fill",
                int(http_json("/stats")["upstream"]["queries"]) - upstream_before == 1,
                int(http_json("/stats")["upstream"]["queries"]) - upstream_before,
                1,
            )

        demo.run_case("UDP truncation / TCP / EDNS", truncation_case)

        def chain_case() -> None:
            first = ask("127.0.0.1", RESOLVER_PORT, "cHaIn.ExAmPlE.TeSt.", "A", timeout=3)
            first_ttls = [int(rr.ttl) for rr in first.response.rr]
            demo.check(
                "CNAME chain converges to A with per-hop TTLs",
                [int(rr.rtype) for rr in first.response.rr] == [5, 5, 1]
                and first_ttls == [300, 120, 30],
                {**record_summary(first.response), "ttls": first_ttls},
                {"types": [5, 5, 1], "ttls": [300, 120, 30]},
            )
            before = http_json("/stats")["stats"]["counters"].get("upstream_queries", 0)
            time.sleep(1.05)
            second = ask("127.0.0.1", RESOLVER_PORT, "chain.example.test.", "A", timeout=3)
            after = http_json("/stats")["stats"]["counters"].get("upstream_queries", 0)
            second_ttls = [int(rr.ttl) for rr in second.response.rr]
            demo.check(
                "CNAME cache hit decrements each record independently",
                second_ttls[0] < 300
                and second_ttls[1] < 120
                and second_ttls[2] < 30
                and after == before,
                {"ttls": second_ttls, "upstream_delta": after - before},
                {"ttls": "each < original", "upstream_delta": 0},
            )
            demo.check(
                "mixed-case CNAME query uses one normalized cache row",
                len(cache_rows("chain.example.test.")) == 1,
                cache_rows("chain.example.test."),
                "one row",
            )

        demo.run_case("CNAME chain and TTL cache", chain_case)

        def negative_case() -> None:
            nx = ask("127.0.0.1", RESOLVER_PORT, "MiSsInG.Example.Test.", "A", timeout=3)
            demo.check(
                "NXDOMAIN carries SOA and NX negative TTL",
                nx.response.header.rcode == 3
                and len(nx.response.auth) == 1
                and int(nx.response.auth[0].rtype) == 6
                and int(nx.response.auth[0].ttl) == 45,
                record_summary(nx.response),
                {"rcode": 3, "soa_ttl": 45},
            )
            time.sleep(1.05)
            nx_hit = ask("127.0.0.1", RESOLVER_PORT, "missing.example.test.", "A", timeout=3)
            demo.check(
                "NXDOMAIN cache hit keeps rcode and decrements SOA TTL",
                nx_hit.response.header.rcode == 3
                and int(nx_hit.response.auth[0].ttl) < 45
                and len(cache_rows("missing.example.test.")) == 1,
                record_summary(nx_hit.response),
                {"rcode": 3, "soa_ttl": "<45", "cache_rows": 1},
            )
            nodata = ask("127.0.0.1", RESOLVER_PORT, "WwW.Example.Test.", "MX", timeout=3)
            demo.check(
                "NODATA is NOERROR with zero answers and NODATA TTL source",
                nodata.response.header.rcode == 0
                and len(nodata.response.rr) == 0
                and len(nodata.response.auth) == 1
                and int(nodata.response.auth[0].ttl) == 15,
                record_summary(nodata.response),
                {"rcode": 0, "answers": 0, "soa_ttl": 15},
            )
            time.sleep(1.05)
            nodata_hit = ask("127.0.0.1", RESOLVER_PORT, "www.example.test.", "MX", timeout=3)
            demo.check(
                "NODATA cache hit keeps rcode and decrements SOA TTL",
                nodata_hit.response.header.rcode == 0
                and int(nodata_hit.response.auth[0].ttl) < 15
                and len(cache_rows("www.example.test.")) == 1,
                {"response": record_summary(nodata_hit.response), "cache_rows": len(cache_rows("www.example.test."))},
                {"rcode": 0, "soa_ttl": "<15", "cache_rows": 1},
            )

        demo.run_case("negative cache", negative_case)

        def errors_case() -> None:
            loop = ask(
                "127.0.0.1", RESOLVER_PORT, "loop1.example.test.", "A", edns_payload=1232, timeout=3
            )
            demo.check(
                "CNAME loop business code and OPT echo",
                loop.business_code == "E_CNAME_LOOP"
                and loop.response.header.rcode == 2
                and len(loop.response.ar) == 1
                and int(loop.response.ar[0].rclass) == 1232,
                {"code": loop.business_code, "rcode": int(loop.response.header.rcode), "summary": record_summary(loop.response)},
                {"code": "E_CNAME_LOOP", "rcode": 2, "opt_udp_size": 1232},
            )
            long_chain = ask(
                "127.0.0.1", RESOLVER_PORT, "long0.example.test.", "A", edns_payload=1232, timeout=3
            )
            demo.check(
                "CNAME max-hop business code",
                long_chain.business_code == "E_CHAIN_TOO_LONG" and long_chain.response.header.rcode == 2,
                {"code": long_chain.business_code, "rcode": int(long_chain.response.header.rcode)},
                {"code": "E_CHAIN_TOO_LONG", "rcode": 2},
            )
            refused = ask(
                "127.0.0.1", RESOLVER_PORT, "refused.example.test.", "A", edns_payload=1232, timeout=3
            )
            demo.check(
                "upstream REFUSED is distinguished from SERVFAIL transport behavior",
                refused.business_code == "E_UPSTREAM_REFUSED" and refused.response.header.rcode == 2,
                {"code": refused.business_code, "rcode": int(refused.response.header.rcode)},
                {"code": "E_UPSTREAM_REFUSED", "rcode": 2},
            )
            started = time.monotonic()
            timed_out = ask(
                "127.0.0.1", RESOLVER_PORT, "timeout.example.test.", "A", edns_payload=1232, timeout=3
            )
            elapsed = time.monotonic() - started
            upstream_stats = http_json("/stats")["upstream"]
            demo.check(
                "per-hop timeout returns SERVFAIL and closes every socket",
                timed_out.business_code == "E_UPSTREAM_TIMEOUT"
                and timed_out.response.header.rcode == 2
                and elapsed < 1.5
                and upstream_stats["active_sockets"] == 0
                and upstream_stats["sockets_opened"] == upstream_stats["sockets_closed"],
                {"code": timed_out.business_code, "elapsed": round(elapsed, 3), "sockets": upstream_stats},
                {"code": "E_UPSTREAM_TIMEOUT", "max_elapsed": 1.5, "active_sockets": 0},
                elapsed,
            )
            malformed, malformed_elapsed = malformed_udp()
            stats = http_json("/stats")["stats"]
            demo.check(
                "malformed self-pointer yields FORMERR/no answer and business counter",
                struct.unpack_from("!H", malformed, 6)[0] == 0
                and (malformed[3] & 0x0F) == 1
                and stats["errors"].get("E_MALFORMED_MESSAGE", 0) >= 1,
                {
                    "ancount": struct.unpack_from("!H", malformed, 6)[0],
                    "rcode": malformed[3] & 0x0F,
                    "errors": stats["errors"],
                    "elapsed": round(malformed_elapsed, 3),
                },
                {"ancount": 0, "rcode": 1, "business_code": "E_MALFORMED_MESSAGE"},
            )
            demo.check(
                "failed resolutions are absent from positive cache",
                all(
                    not cache_rows(key)
                    for key in (
                        "loop1.example.test.",
                        "long0.example.test.",
                        "refused.example.test.",
                        "timeout.example.test.",
                    )
                ),
                "no rows for failed keys",
                "no rows for failed keys",
            )

        demo.run_case("business errors and upstream timeout", errors_case)

        def concurrency_case() -> None:
            barrier = threading.Barrier(10)
            results: list[Any] = []
            failures: list[BaseException] = []
            lock = threading.Lock()

            def worker(index: int) -> None:
                try:
                    barrier.wait(timeout=3)
                    result = ask(
                        "127.0.0.1",
                        RESOLVER_PORT,
                        "CONCURRENT.Example.Test." if index % 2 else "concurrent.example.test.",
                        "A",
                        timeout=4,
                    )
                    with lock:
                        results.append(result)
                except BaseException as exc:  # pragma: no cover - diagnostic output
                    with lock:
                        failures.append(exc)

            before = http_json("/stats")
            threads = [threading.Thread(target=worker, args=(index,)) for index in range(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=8)
            after = http_json("/stats")
            upstream_delta = after["upstream"]["queries"] - before["upstream"]["queries"]
            waiter_delta = after["stats"]["counters"].get("inflight_waiters", 0) - before["stats"]["counters"].get("inflight_waiters", 0)
            addresses = [
                repr(result.response.rr[0].rdata) if result.response.rr else "NO_ANSWER"
                for result in results
            ]
            demo.check(
                "10 concurrent mixed-case duplicates trigger one upstream resolution",
                not failures
                and len(results) == 10
                and all(address == "192.0.2.77" for address in addresses)
                and upstream_delta == 1
                and waiter_delta >= 8,
                {
                    "results": len(results),
                    "failures": [repr(item) for item in failures],
                    "addresses": addresses,
                    "upstream_delta": upstream_delta,
                    "inflight_waiter_delta": waiter_delta,
                },
                {"results": 10, "upstream_delta": 1, "inflight_waiter_delta": ">=8"},
            )

        demo.run_case("concurrent duplicate suppression", concurrency_case)

        def recovery_case() -> None:
            nonlocal resolver, restarted, crash_worker
            crash_errors: list[BaseException] = []

            def crash_query() -> None:
                try:
                    ask("127.0.0.1", RESOLVER_PORT, "crash0.example.test.", "A", timeout=10)
                except BaseException as exc:
                    crash_errors.append(exc)

            crash_worker = threading.Thread(target=crash_query, daemon=True)
            crash_worker.start()
            running = wait_running_crash_session(timeout=4)
            demo.check(
                "forced-kill point reached mid-CNAME-chain and persisted as running",
                running is not None
                and running["status"] == "running"
                and int(running["chain_steps"]) >= 1,
                running,
                {"status": "running", "chain_steps": ">=1"},
            )
            if running is None:
                raise AssertionError("crash session never appeared; refusing to fake recovery")
            assert resolver is not None
            resolver.kill()
            resolver.wait(timeout=5)
            crash_worker.join(timeout=2)
            demo.check(
                "middleware process was force-killed (not gracefully shut down)",
                resolver.returncode is not None and resolver.returncode != 0,
                {"returncode": resolver.returncode, "worker_errors": [repr(item) for item in crash_errors]},
                {"returncode": "non-zero"},
            )

            connection = sqlite3.connect(DB_PATH, timeout=2)
            try:
                persisted = connection.execute(
                    "SELECT status,chain_steps FROM resolution_sessions WHERE id=?",
                    (running["id"],),
                ).fetchone()
            finally:
                connection.close()
            demo.check(
                "pre-kill incomplete session survives in SQLite",
                persisted == ("running", int(running["chain_steps"])),
                persisted,
                ("running", int(running["chain_steps"])),
            )

            restarted = start_process(
                demo, "resolver-restarted", resolver_args(), python=args.python
            )
            wait_admin()
            recovery = http_json("/recovery")
            interrupted = [
                item
                for item in recovery.get("interrupted_sessions", [])
                if item.get("cache_key") == "crash0.example.test."
            ]
            demo.check(
                "restart scans persisted facts and reports interrupted session",
                recovery.get("source") == "sqlite"
                and len(interrupted) == 1
                and interrupted[0]["state"] == "incomplete",
                recovery,
                {"source": "sqlite", "interrupted_cache_key": "crash0.example.test.", "state": "incomplete"},
            )
            sessions = http_json("/sessions")
            crash_session = next(
                item for item in sessions if item["cache_key"] == "crash0.example.test."
            )
            demo.check(
                "unfinished query is marked interrupted and has no uncommitted cache row",
                crash_session["status"] == "interrupted" and not cache_rows("crash0.example.test."),
                {"session": crash_session, "cache_rows": cache_rows("crash0.example.test.")},
                {"status": "interrupted", "cache_rows": []},
            )
            before = http_json("/stats")["upstream"]["queries"]
            chain_after = ask("127.0.0.1", RESOLVER_PORT, "CHAIN.example.test.", "A", timeout=3)
            after = http_json("/stats")["upstream"]["queries"]
            demo.check(
                "committed cache survives restart and is reused without upstream traffic",
                len(chain_after.response.rr) == 3 and after == before,
                {"answers": len(chain_after.response.rr), "upstream_delta": after - before},
                {"answers": 3, "upstream_delta": 0},
            )

        demo.run_case("forced termination and recovery", recovery_case)

    finally:
        stop_process(restarted)
        stop_process(resolver)
        stop_process(auth)
        for handle in demo.log_handles:
            try:
                handle.close()
            except OSError:
                pass

    passed = all(item.passed for item in demo.checks)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "passed": passed,
        "environment": {
            "platform": platform.platform(),
            "python": sys.version,
            "executable": args.python,
            "loopback": "127.0.0.1",
            "fixed_ports": {
                "authoritative": AUTH_PORT,
                "resolver": RESOLVER_PORT,
                "admin": ADMIN_PORT,
            },
            "database": str(DB_PATH),
        },
        "checks": [
            {
                "name": item.name,
                "passed": item.passed,
                "observed": item.observed,
                "expected": item.expected,
                "elapsed_seconds": round(item.elapsed_seconds, 6),
            }
            for item in demo.checks
        ],
        "summary": {
            "total": len(demo.checks),
            "passed": sum(item.passed for item in demo.checks),
            "failed": sum(not item.passed for item in demo.checks),
        },
    }
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"\n{'PASS' if passed else 'FAIL'}: {payload['summary']['passed']}/{payload['summary']['total']} checks; "
        f"results={RESULTS_PATH}",
        flush=True,
    )
    return 0 if passed else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the fixed-port loopback DNS middleware demo")
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
