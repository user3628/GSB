from __future__ import annotations

import asyncio
import copy
import http.client
import json
import os
import tempfile
import time
import unittest
from dataclasses import dataclass
from typing import Any
from unittest import mock

from proxy_workbench.errors import ProxyError
from proxy_workbench.runtime import Runtime
from proxy_workbench.server import WorkbenchServer
from proxy_workbench.testing import LoopbackBackend, reserve_unused_port
from proxy_workbench.transport import perform_request


def reverse_route(
    route_id: str,
    *,
    prefix: str,
    upstream: str,
    methods: list[str] | None = None,
    headers: dict[str, str] | None = None,
    connect_timeout: float = 1.0,
    read_timeout: float = 1.0,
    attempts: int = 1,
    health_enabled: bool = False,
    health_interval: float = 0.05,
    fail_threshold: int = 1,
    failure_mode: str = "fail",
    backups: list[str] | None = None,
    in_flight_policy: str = "continue",
) -> dict[str, Any]:
    return {
        "id": route_id,
        "kind": "reverse",
        "match": {"methods": methods or ["GET", "POST"], "path_prefix": prefix},
        "upstream": upstream,
        "timeout": {"connect": connect_timeout, "read": read_timeout},
        "retries": {
            "attempts": attempts,
            "backoff_ms": 0,
            "retry_on": ["connection_refused", "connect_timeout", "read_timeout", "5xx"],
        },
        "headers": headers or {},
        "health": {
            "enabled": health_enabled,
            "path": "/health",
            "interval": health_interval,
            "timeout": 0.1,
            "fail_threshold": fail_threshold,
            "success_threshold": 1,
        },
        "failure_policy": {
            "mode": failure_mode,
            "backup_upstreams": backups or [],
            "group": f"{route_id}-group",
        },
        "in_flight_policy": in_flight_policy,
    }


def forward_route(
    route_id: str,
    *,
    target_port: int,
    upstream: str,
    prefix: str = "/forward",
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "id": route_id,
        "kind": "forward",
        "match": {
            "methods": ["GET", "POST"],
            "path_prefix": prefix,
            "target_host": "127.0.0.1",
            "target_port": target_port,
        },
        "upstream": upstream,
        "timeout": {"connect": 1.0, "read": 1.0},
        "retries": {"attempts": 1},
        "headers": headers or {},
        "health": {"enabled": False},
        "failure_policy": {"mode": "fail", "group": f"{route_id}-group"},
        "in_flight_policy": "continue",
    }


def snapshot(version: int, routes: list[dict[str, Any]]) -> dict[str, Any]:
    return {"kind": "snapshot", "version": version, "routes": routes}


def increment(base: int, version: int, routes: list[dict[str, Any]]) -> dict[str, Any]:
    return {"kind": "incremental", "base_version": base, "version": version, "routes": routes}


def _sync_http(
    host: str,
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: list[tuple[str, str]] | None = None,
    timeout: float = 4.0,
) -> dict[str, Any]:
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        connection.request(method, path, body=body, headers=dict(headers or []))
        response = connection.getresponse()
        raw = response.read()
        response_headers = {name.lower(): value for name, value in response.getheaders()}
        parsed: Any = None
        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            parsed = None
        return {
            "status": response.status,
            "headers": response_headers,
            "body": raw,
            "json": parsed,
            "elapsed": None,
        }
    finally:
        connection.close()


@dataclass
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes
    json: Any
    elapsed: float | None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


async def http_call(
    host: str,
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: list[tuple[str, str]] | None = None,
    timeout: float = 4.0,
) -> dict[str, Any]:
    started = time.monotonic()
    result = await asyncio.to_thread(_sync_http, host, port, method, path, body, headers, timeout)
    result["elapsed"] = time.monotonic() - started
    return HttpResult(**result)


async def submit(host: str, port: int, payload: dict[str, Any]) -> dict[str, Any]:
    return await http_call(
        host,
        port,
        "POST",
        "/__workbench/config",
        body=json.dumps(payload).encode("utf-8"),
        headers=[("Content-Type", "application/json")],
    )


async def raw_http(host: str, port: int, request: bytes, timeout: float = 4.0) -> dict[str, Any]:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(request)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=timeout)
    finally:
        writer.close()
        await writer.wait_closed()
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.lower()] = value.strip()
    parsed = None
    try:
        parsed = json.loads(body.decode("utf-8")) if body else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    return {"status": status, "headers": headers, "body": body, "json": parsed}


async def wait_until(predicate, *, timeout: float = 3.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was not met before timeout")


class WorkbenchIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tempdir.name, "workbench.db")
        self.runtime = await Runtime.create(self.db_path)
        self.server = WorkbenchServer(runtime=self.runtime, host="127.0.0.1", port=0, graceful_timeout=2)
        await self.server.start()
        self.host = "127.0.0.1"
        self.port = self.server.port
        self.backends: list[LoopbackBackend] = []

    async def asyncTearDown(self) -> None:
        self.server.request_shutdown()
        await self.server.wait_closed()
        for backend in reversed(self.backends):
            await backend.stop()
        self.tempdir.cleanup()

    async def backend(self, **kwargs: Any) -> LoopbackBackend:
        backend = LoopbackBackend(port=0, **kwargs)
        await backend.start()
        self.backends.append(backend)
        return backend

    async def test_reverse_and_forward_forwarding_with_header_boundary(self) -> None:
        backend = await self.backend()
        reverse = reverse_route(
            "echo",
            prefix="/api/echo",
            upstream=backend.upstream,
            headers={"X-Demo-Configured": "yes", "X-Override": "configured"},
        )
        forward = forward_route(
            "forward",
            target_port=backend.port,
            upstream=backend.upstream,
            headers={"X-Forward-Route": "yes"},
        )
        response = await submit(self.host, self.port, snapshot(1, [reverse, forward]))
        self.assertEqual(response.status, 200, response.body)

        response = await http_call(
            self.host,
            self.port,
            "POST",
            "/api/echo/a?q=1&x=two",
            body=b"hello-body",
            headers=[
                ("Content-Type", "text/plain"),
                ("X-Override", "client"),
                ("Connection", "keep-alive, X-Remove-Me"),
                ("X-Remove-Me", "secret"),
            ],
        )
        self.assertEqual(response.status, 200, response.body)
        self.assertEqual(response.headers["x-workbench-route"], "echo")
        self.assertEqual(response.headers["x-workbench-version"], "1")
        record = backend.requests[-1]
        self.assertEqual(record["method"], "POST")
        self.assertEqual(record["path"], "/api/echo/a")
        self.assertEqual(record["query"], "q=1&x=two")
        self.assertEqual(record["body"], "hello-body")
        self.assertEqual(record["headers"]["x-demo-configured"], ["yes"])
        self.assertEqual(record["headers"]["x-override"], ["configured"])
        self.assertNotIn("x-remove-me", record["headers"])
        self.assertEqual(record["headers"]["connection"], ["close"])
        self.assertIn("x-forwarded-for", record["headers"])

        target = f"http://127.0.0.1:{backend.port}/forward/f?z=9"
        raw_request = (
            f"POST {target} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{backend.port}\r\n"
            "Content-Length: 5\r\n"
            "Connection: close\r\n\r\n"
            "world"
        ).encode("latin-1")
        forward_response = await raw_http(self.host, self.port, raw_request)
        self.assertEqual(forward_response["status"], 200, forward_response["body"])
        self.assertEqual(forward_response["headers"]["x-workbench-route"], "forward")
        forward_record = backend.requests[-1]
        self.assertEqual(forward_record["method"], "POST")
        self.assertEqual(forward_record["path"], "/forward/f")
        self.assertEqual(forward_record["query"], "z=9")
        self.assertEqual(forward_record["body"], "world")
        self.assertEqual(forward_record["headers"]["x-forward-route"], ["yes"])

        dashboard = await http_call(self.host, self.port, "GET", "/dashboard")
        self.assertEqual(dashboard.status, 200)
        self.assertIn(b"Loopback Proxy Workbench", dashboard.body)
        self.assertIn(b"echo", dashboard.body)
        state = await http_call(self.host, self.port, "GET", "/__workbench/state")
        self.assertEqual(state.json["version"], 1)
        self.assertEqual(len(state.json["routes"]), 2)

    async def test_incremental_cascade_stale_base_and_rollback(self) -> None:
        backend_a = await self.backend(label="a")
        backend_b = await self.backend(label="b")
        echo_v1 = reverse_route("echo", prefix="/api/echo", upstream=backend_a.upstream)
        stable = reverse_route("stable", prefix="/api/stable", upstream=backend_a.upstream)
        accepted = await submit(self.host, self.port, snapshot(1, [echo_v1, stable]))
        self.assertEqual(accepted.status, 200, accepted.body)

        echo_v2 = copy.deepcopy(echo_v1)
        echo_v2["upstream"] = backend_b.upstream
        updated = await submit(self.host, self.port, increment(1, 2, [echo_v2]))
        self.assertEqual(updated.status, 200, updated.body)
        body = updated.json
        self.assertEqual(body["version"], 2)
        self.assertEqual(body["invalidation"]["changed_route_ids"], ["echo"])
        self.assertIn("upstream", body["invalidation"]["changed_fields"]["echo"])
        for artifact in ["match:echo", "pool:echo-group:echo", "health:echo", "rules:echo", "index:reverse-match"]:
            self.assertIn(artifact, body["invalidation"]["invalidated_artifacts"])
            self.assertIn(artifact, body["invalidation"]["rebuilt_artifacts"])
        self.assertNotIn("match:stable", body["invalidation"]["invalidated_artifacts"])

        echo_response = await http_call(self.host, self.port, "GET", "/api/echo/x")
        self.assertEqual(echo_response.status, 200)
        self.assertEqual(echo_response.json["backend"], backend_b.port)
        stable_response = await http_call(self.host, self.port, "GET", "/api/stable/x")
        self.assertEqual(stable_response.json["backend"], backend_a.port)

        state = await http_call(self.host, self.port, "GET", "/__workbench/state")
        routes = {route["id"]: route for route in state.json["routes"]}
        self.assertEqual(routes["echo"]["artifacts"]["match"]["built_version"], 2)
        self.assertEqual(routes["stable"]["artifacts"]["match"]["built_version"], 1)
        self.assertTrue(all(route["artifacts"]["match"]["status"] == "fresh" for route in routes.values()))

        stale = await submit(
            self.host,
            self.port,
            increment(1, 3, [reverse_route("echo", prefix="/api/echo", upstream=backend_a.upstream)]),
        )
        self.assertEqual(stale.status, 409)
        self.assertEqual(stale.json["code"], "E_STALE_BASE")
        self.assertEqual(stale.json["details"]["current_version"], 2)
        current = await http_call(self.host, self.port, "GET", "/__workbench/state")
        self.assertEqual(current.json["version"], 2)
        still_new = await http_call(self.host, self.port, "GET", "/api/echo/x")
        self.assertEqual(still_new.json["backend"], backend_b.port)

        replaced = await submit(self.host, self.port, snapshot(3, [stable]))
        self.assertEqual(replaced.status, 200, replaced.body)
        self.assertEqual(replaced.json["invalidation"]["removed_route_ids"], ["echo"])
        echo_gone = await http_call(self.host, self.port, "GET", "/api/echo/x")
        self.assertEqual(echo_gone.status, 404)
        self.assertEqual(echo_gone.json["code"], "E_ROUTE_NOT_FOUND")

        rollback = await submit(self.host, self.port, snapshot(1, [echo_v1, stable]))
        self.assertEqual(rollback.status, 409)
        self.assertEqual(rollback.json["code"], "E_VERSION_ROLLBACK")

    async def test_concurrent_increments_only_one_wins(self) -> None:
        backend_a = await self.backend(label="a")
        backend_b = await self.backend(label="b")
        backend_c = await self.backend(label="c")
        base_route = reverse_route("echo", prefix="/api/echo", upstream=backend_a.upstream)
        initial = await submit(self.host, self.port, snapshot(1, [base_route]))
        self.assertEqual(initial.status, 200)

        route_b = copy.deepcopy(base_route)
        route_b["upstream"] = backend_b.upstream
        route_c = copy.deepcopy(base_route)
        route_c["upstream"] = backend_c.upstream
        result_b, result_c = await asyncio.gather(
            submit(self.host, self.port, increment(1, 2, [route_b])),
            submit(self.host, self.port, increment(1, 2, [route_c])),
        )
        codes = sorted([result_b.json["code"], result_c.json["code"]])
        self.assertEqual(codes, ["E_STALE_BASE", "OK"])
        state = await http_call(self.host, self.port, "GET", "/__workbench/state")
        self.assertEqual(state.json["version"], 2)
        self.assertEqual(len(state.json["routes"]), 1)

    async def test_timeout_refused_and_5xx_are_distinct(self) -> None:
        backend = await self.backend(slow_delay=0.4)
        refused_port = reserve_unused_port()
        routes = [
            reverse_route(
                "slow",
                prefix="/api/slow",
                upstream=f"{backend.upstream}/slow",
                read_timeout=0.05,
            ),
            reverse_route(
                "fail5xx",
                prefix="/api/fail",
                upstream=f"{backend.upstream}/status/500",
            ),
            reverse_route(
                "refused",
                prefix="/api/refused",
                upstream=f"http://127.0.0.1:{refused_port}",
                connect_timeout=0.2,
            ),
        ]
        submitted = await submit(self.host, self.port, snapshot(1, routes))
        self.assertEqual(submitted.status, 200)

        slow = await http_call(self.host, self.port, "GET", "/api/slow/x")
        self.assertEqual(slow.status, 504)
        self.assertEqual(slow.json["code"], "E_READ_TIMEOUT")
        self.assertLess(slow.elapsed, 1.0)

        refused = await http_call(self.host, self.port, "GET", "/api/refused/x")
        self.assertEqual(refused.status, 502)
        self.assertEqual(refused.json["code"], "E_CONNECTION_REFUSED")

        failed = await http_call(self.host, self.port, "GET", "/api/fail/x")
        self.assertEqual(failed.status, 500)
        self.assertEqual(failed.json["code"], "E_UPSTREAM_5XX")
        self.assertEqual(failed.json["details"]["upstream_status"], 500)
        self.assertIn("scripted upstream status", failed.json["details"]["upstream_body"])
        before_blocked = len(backend.matching_requests("/status/500"))
        blocked = await http_call(self.host, self.port, "GET", "/api/fail/x")
        self.assertEqual(blocked.status, 503)
        self.assertEqual(blocked.json["code"], "E_UPSTREAM_UNAVAILABLE")
        self.assertEqual(len(backend.matching_requests("/status/500")), before_blocked)

    async def test_health_blocks_known_bad_upstream_and_failover_uses_backup(self) -> None:
        unhealthy_primary = await self.backend(health_status=503)
        backup = await self.backend(label="backup")
        refused_port = reserve_unused_port()
        health_route = reverse_route(
            "health-guard",
            prefix="/api/guarded",
            upstream=unhealthy_primary.upstream,
            health_enabled=True,
            health_interval=0.03,
            fail_threshold=1,
        )
        failover_route = reverse_route(
            "failover",
            prefix="/api/failover",
            upstream=f"http://127.0.0.1:{refused_port}",
            connect_timeout=0.2,
            attempts=2,
            failure_mode="failover",
            backups=[backup.upstream],
        )
        submitted = await submit(self.host, self.port, snapshot(1, [health_route, failover_route]))
        self.assertEqual(submitted.status, 200)

        await wait_until(
            lambda: (
                self.runtime.health.states.get(unhealthy_primary.upstream) is not None
                and self.runtime.health.states[unhealthy_primary.upstream].status == "unhealthy"
            )
        )
        guarded = await http_call(self.host, self.port, "GET", "/api/guarded/x")
        self.assertEqual(guarded.status, 503)
        self.assertEqual(guarded.json["code"], "E_UPSTREAM_UNAVAILABLE")
        self.assertEqual(unhealthy_primary.matching_requests("/api/guarded"), [])

        before_backup = len(backup.requests)
        failover = await http_call(self.host, self.port, "GET", "/api/failover/x")
        self.assertEqual(failover.status, 200, failover.body)
        self.assertEqual(failover.json["backend"], backup.port)
        self.assertEqual(failover.headers["x-workbench-upstream"], backup.upstream)
        self.assertGreater(len(backup.requests), before_backup)

    async def test_in_flight_continue_and_abort_policies(self) -> None:
        backend = await self.backend(slow_delay=0.3)
        continue_route = reverse_route(
            "continue",
            prefix="/api/continue",
            upstream=f"{backend.upstream}/slow",
            headers={"X-Config-Marker": "v1"},
            in_flight_policy="continue",
        )
        abort_route = reverse_route(
            "abort",
            prefix="/api/abort",
            upstream=f"{backend.upstream}/slow",
            headers={"X-Config-Marker": "v1"},
            in_flight_policy="abort",
        )
        submitted = await submit(self.host, self.port, snapshot(1, [continue_route, abort_route]))
        self.assertEqual(submitted.status, 200)

        continue_future = asyncio.create_task(
            http_call(self.host, self.port, "GET", "/api/continue/x", timeout=5)
        )
        abort_future = asyncio.create_task(
            http_call(self.host, self.port, "GET", "/api/abort/x", timeout=5)
        )
        await wait_until(lambda: len(backend.requests) >= 2)

        continue_v2 = copy.deepcopy(continue_route)
        abort_v2 = copy.deepcopy(abort_route)
        continue_v2["headers"]["X-Config-Marker"] = "v2"
        abort_v2["headers"]["X-Config-Marker"] = "v2"
        updated = await submit(self.host, self.port, increment(1, 2, [continue_v2, abort_v2]))
        self.assertEqual(updated.status, 200, updated.body)

        continued, aborted = await asyncio.gather(continue_future, abort_future)
        self.assertEqual(continued.status, 200, continued.body)
        self.assertEqual(continued.headers["x-workbench-version"], "1")
        self.assertEqual(aborted.status, 409)
        self.assertEqual(aborted.json["code"], "E_CONFIG_CHANGED")
        self.assertEqual(aborted.json["details"]["bound_version"], 1)
        self.assertEqual(aborted.json["details"]["current_version"], 2)

    async def test_invalid_config_is_rejected_without_state_change(self) -> None:
        backend = await self.backend()
        route = reverse_route("echo", prefix="/api/echo", upstream=backend.upstream)
        accepted = await submit(self.host, self.port, snapshot(1, [route]))
        self.assertEqual(accepted.status, 200)
        invalid = copy.deepcopy(route)
        invalid["upstream"] = "http://example.com:80"
        rejected = await submit(self.host, self.port, snapshot(2, [invalid]))
        self.assertEqual(rejected.status, 400)
        self.assertEqual(rejected.json["code"], "E_CONFIG_INVALID")
        state = await http_call(self.host, self.port, "GET", "/__workbench/state")
        self.assertEqual(state.json["version"], 1)

    async def test_route_change_replaces_old_health_probe_plan(self) -> None:
        healthy_backend = await self.backend(health_status=200, label="healthy")
        unhealthy_backend = await self.backend(health_status=503, label="unhealthy")
        route_v1 = reverse_route(
            "health-plan",
            prefix="/api/health-plan",
            upstream=healthy_backend.upstream,
            health_enabled=True,
            health_interval=0.03,
        )
        submitted = await submit(self.host, self.port, snapshot(1, [route_v1]))
        self.assertEqual(submitted.status, 200)
        await wait_until(
            lambda: (
                self.runtime.health.states.get(healthy_backend.upstream) is not None
                and self.runtime.health.states[healthy_backend.upstream].status == "healthy"
            )
        )

        route_v2 = copy.deepcopy(route_v1)
        route_v2["upstream"] = unhealthy_backend.upstream
        updated = await submit(self.host, self.port, increment(1, 2, [route_v2]))
        self.assertEqual(updated.status, 200, updated.body)
        await wait_until(
            lambda: (
                self.runtime.health.states.get(unhealthy_backend.upstream) is not None
                and self.runtime.health.states[unhealthy_backend.upstream].status == "unhealthy"
            )
        )
        self.assertNotIn(healthy_backend.upstream, self.runtime.health.states)
        state = await http_call(self.host, self.port, "GET", "/__workbench/state")
        route = state.json["routes"][0]
        self.assertEqual(route["artifacts"]["health_plan"]["built_version"], 2)
        self.assertEqual(route["upstream_health"][0]["upstream"], unhealthy_backend.upstream)
        self.assertEqual(route["upstream_health"][0]["status"], "unhealthy")

    async def test_connect_timeout_has_stable_business_code(self) -> None:
        async def slow_open(*args: Any, **kwargs: Any):
            await asyncio.sleep(1.0)

        with mock.patch("proxy_workbench.transport.asyncio.open_connection", slow_open):
            started = time.monotonic()
            with self.assertRaises(ProxyError) as raised:
                await perform_request(
                    upstream="http://127.0.0.1:9",
                    method="GET",
                    path_query="/",
                    headers=(),
                    body=b"",
                    connect_timeout=0.03,
                    read_timeout=0.03,
                )
        self.assertEqual(raised.exception.code, "E_CONNECT_TIMEOUT")
        self.assertLess(time.monotonic() - started, 0.5)


if __name__ == "__main__":
    unittest.main()

