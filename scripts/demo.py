#!/usr/bin/env python3
"""Deterministic end-to-end demo over fixed 127.0.0.1 ports."""

from __future__ import annotations

import asyncio
import http.client
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from proxy_workbench.runtime import Runtime  # noqa: E402
from proxy_workbench.server import WorkbenchServer  # noqa: E402
from proxy_workbench.testing import LoopbackBackend, port_is_free  # noqa: E402


WORKBENCH_PORT = 19080
BACKEND_A_PORT = 19101
BACKEND_B_PORT = 19102
REFUSED_PORT = 19109
REPORT_PATH = ROOT / "artifacts" / "demo" / "report.json"
DB_PATH = ROOT / "artifacts" / "demo" / "workbench.db"


def _load_sample(name: str) -> dict[str, Any]:
    with open(ROOT / "samples" / name, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _sync_request(
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        raw = response.read()
        parsed = None
        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        return {
            "http_status": response.status,
            "headers": {name.lower(): value for name, value in response.getheaders()},
            "body": raw.decode("utf-8", errors="replace"),
            "json": parsed,
        }
    finally:
        connection.close()


async def request(
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    return await asyncio.to_thread(_sync_request, port, method, path, body, headers)


async def submit(payload: dict[str, Any]) -> dict[str, Any]:
    return await request(
        WORKBENCH_PORT,
        "POST",
        "/__workbench/config",
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )


async def raw_forward() -> dict[str, Any]:
    target = f"http://127.0.0.1:{BACKEND_B_PORT}/forward/demo?source=forward"
    outgoing = (
        f"POST {target} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{BACKEND_B_PORT}\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 11\r\n"
        "Connection: close\r\n\r\n"
        "forward-body"
    ).encode("latin-1")
    reader, writer = await asyncio.open_connection("127.0.0.1", WORKBENCH_PORT)
    try:
        writer.write(outgoing)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=5)
    finally:
        writer.close()
        await writer.wait_closed()
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.lower()] = value.strip()
    parsed = json.loads(body.decode("utf-8")) if body else None
    return {
        "http_status": int(lines[0].split(" ", 2)[1]),
        "headers": headers,
        "json": parsed,
        "body": body.decode("utf-8", errors="replace"),
    }


def cleanup_demo_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()


def print_step(name: str, value: dict[str, Any]) -> None:
    payload = value.get("json") or {}
    if payload.get("ok"):
        invalidation = payload.get("invalidation", {})
        print(
            json.dumps(
                {
                    "step": name,
                    "code": payload.get("code"),
                    "version": payload.get("version"),
                    "changed_route_ids": invalidation.get("changed_route_ids"),
                    "invalidated_artifacts": invalidation.get("invalidated_artifacts"),
                    "rebuilt_artifacts": invalidation.get("rebuilt_artifacts"),
                    "reused_artifact_count": len(invalidation.get("reused_artifacts", [])),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(
            json.dumps(
                {
                    "step": name,
                    "http_status": value.get("http_status"),
                    "code": payload.get("code"),
                    "details": payload.get("details"),
                },
                ensure_ascii=False,
                indent=2,
            )
        )


async def run_demo() -> int:
    required = [WORKBENCH_PORT, BACKEND_A_PORT, BACKEND_B_PORT, REFUSED_PORT]
    busy = [port for port in required if not port_is_free(port)]
    if busy:
        print(
            json.dumps(
                {
                    "ok": False,
                    "code": "E_PORT_BUSY",
                    "message": "preflight failed; all demo ports must be free",
                    "busy_ports": busy,
                    "ports": required,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2

    cleanup_demo_db()
    backend_a = LoopbackBackend(port=BACKEND_A_PORT, label="A", slow_delay=0.5)
    backend_b = LoopbackBackend(port=BACKEND_B_PORT, label="B", slow_delay=0.5)
    await backend_a.start()
    await backend_b.start()
    runtime = await Runtime.create(DB_PATH)
    server = WorkbenchServer(
        runtime=runtime,
        host="127.0.0.1",
        port=WORKBENCH_PORT,
        graceful_timeout=3,
    )
    await server.start()
    report: dict[str, Any] = {
        "ok": False,
        "ports": {
            "workbench": WORKBENCH_PORT,
            "backend_a": BACKEND_A_PORT,
            "backend_b": BACKEND_B_PORT,
            "refused": REFUSED_PORT,
        },
        "samples": {
            "full": "samples/full_snapshot.v3.json",
            "incremental": "samples/incremental.v4.json",
            "stale": "samples/stale_incremental.v5.json",
        },
        "steps": {},
    }
    try:
        full = await submit(_load_sample("full_snapshot.v3.json"))
        report["steps"]["full_commit"] = full
        print_step("full_commit", full)
        if full["http_status"] != 200:
            raise AssertionError(f"full commit failed: {full['body']}")

        echo_before_reverse = await request(WORKBENCH_PORT, "POST", "/api/echo/demo?phase=before", b"v3-body")
        echo_before_forward = await raw_forward()
        before_backend = (echo_before_reverse.get("json") or {}).get("backend")
        if before_backend != BACKEND_A_PORT:
            raise AssertionError(f"v3 reverse reached unexpected backend: {before_backend}")

        incremental = await submit(_load_sample("incremental.v4.json"))
        report["steps"]["incremental_commit"] = incremental
        print_step("incremental_commit", incremental)
        if incremental["http_status"] != 200:
            raise AssertionError(f"incremental commit failed: {incremental['body']}")

        echo_after_reverse = await request(WORKBENCH_PORT, "POST", "/api/echo/demo?phase=after", b"v4-body")
        after_backend = (echo_after_reverse.get("json") or {}).get("backend")
        if after_backend != BACKEND_B_PORT:
            raise AssertionError(f"v4 reverse reached unexpected backend: {after_backend}")

        stale = await submit(_load_sample("stale_incremental.v5.json"))
        report["steps"]["stale_commit"] = stale
        print_step("stale_commit", stale)
        if stale["http_status"] != 409 or (stale.get("json") or {}).get("code") != "E_STALE_BASE":
            raise AssertionError("stale base was not rejected with E_STALE_BASE")

        slow = await request(WORKBENCH_PORT, "GET", "/api/slow/demo")
        five_xx = await request(WORKBENCH_PORT, "GET", "/api/fail/demo")
        refused = await request(WORKBENCH_PORT, "GET", "/api/refused/demo")
        failover = await request(WORKBENCH_PORT, "GET", "/api/failover/demo")
        error_codes = {
            "read_timeout": (slow.get("json") or {}).get("code"),
            "upstream_5xx": (five_xx.get("json") or {}).get("code"),
            "connection_refused": (refused.get("json") or {}).get("code"),
            "failover_result": "OK" if failover["http_status"] == 200 else (failover.get("json") or {}).get("code"),
        }
        report["steps"]["error_chain"] = {
            "read_timeout": slow,
            "upstream_5xx": five_xx,
            "connection_refused": refused,
            "failover": failover,
        }

        dashboard = await request(WORKBENCH_PORT, "GET", "/dashboard")
        state = await request(WORKBENCH_PORT, "GET", "/__workbench/state")
        state_json = state.get("json") or {}
        report["verification"] = {
            "reverse_v3_backend": before_backend,
            "reverse_v4_backend": after_backend,
            "forward_v3_status": echo_before_forward["http_status"],
            "forward_v3_backend": (echo_before_forward.get("json") or {}).get("backend"),
            "forward_v3_route": echo_before_forward["headers"].get("x-workbench-route"),
            "stale_rejected": True,
            "error_codes": error_codes,
            "final_version": state_json.get("version"),
            "final_config_hash": (state_json.get("state") or {}).get("config_hash"),
            "dashboard_http_status": dashboard["http_status"],
            "health": state_json.get("health", {}).get("upstreams"),
        }
        if error_codes != {
            "read_timeout": "E_READ_TIMEOUT",
            "upstream_5xx": "E_UPSTREAM_5XX",
            "connection_refused": "E_CONNECTION_REFUSED",
            "failover_result": "OK",
        }:
            raise AssertionError(f"unexpected error chain: {error_codes}")
        if state_json.get("version") != 4:
            raise AssertionError(f"final version should be 4, got {state_json.get('version')}")
        if dashboard["http_status"] != 200:
            raise AssertionError("dashboard did not return HTTP 200")
        report["ok"] = True
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        server.request_shutdown()
        shutdown = await server.wait_closed()
        await backend_a.stop()
        await backend_b.stop()
        report["graceful_shutdown"] = shutdown
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    print("\n=== FINAL VERIFICATION ===")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    print(f"\nReport written to: {REPORT_PATH}")
    print(f"Dashboard was served at: http://127.0.0.1:{WORKBENCH_PORT}/dashboard")
    return 0 if report.get("ok") else 1


def main() -> int:
    return asyncio.run(run_demo())


if __name__ == "__main__":
    raise SystemExit(main())

