#!/usr/bin/env python3
"""Start the app on loopback, exercise HTTP endpoints, and emit a report."""

from __future__ import annotations

import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import uvicorn

from rope_diag.api import create_app
from rope_diag.digest import atomic_write_json
from rope_diag.session import DiagnosticSession


def main() -> int:
    workspace = ROOT / "artifacts" / "demo_workspace"
    session = DiagnosticSession.load(workspace)
    if session is None:
        raise SystemExit("Run scripts/run_demo.py first or initialize a workspace.")
    if session.status == "stopped":
        session.resume()
    elif session.status != "completed":
        session.run()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    app = create_app(workspace)
    config = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="warning", access_log=False
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    health: dict[str, Any] | None = None
    for _ in range(100):
        try:
            health = _request("GET", base_url + "/api/health")
            break
        except Exception:
            time.sleep(0.05)
    if health is None:
        raise SystemExit("HTTP server did not become ready")

    index = _request("GET", base_url + "/", raw=True)
    status = _request("GET", base_url + "/api/status")
    query = _request(
        "POST",
        base_url + "/api/query",
        {
            "shard_id": "odd",
            "query_type": "angles",
            "options": {"position": 3},
        },
    )
    cached_query = _request(
        "POST",
        base_url + "/api/query",
        {
            "shard_id": "odd",
            "query_type": "angles",
            "options": {"position": 3},
        },
    )
    latency_error = _request(
        "POST",
        base_url + "/api/query",
        {
            "shard_id": "long",
            "query_type": "score_summary",
            "options": {"query_position": 8},
            "latency_budget_ms": 0,
            "bypass_cache": True,
        },
        expected_status=408,
    )
    export = _request("POST", base_url + "/api/export", {})
    download = urllib.request.urlopen(
        base_url + export["download_url"], timeout=10
    ).read()

    server.should_exit = True
    thread.join(timeout=10)
    health_ok = health.get("service") == "rope-diagnostics" and bool(
        health.get("session_loaded")
    )
    report = {
        "loopback_url": base_url,
        "health_ok": health_ok,
        "index_contains_title": b"RoPE Diagnostics" in index,
        "status": status["status"],
        "angle_query_first_cache_hit": query["cache_hit"],
        "angle_query_second_cache_hit": cached_query["cache_hit"],
        "angle_query_payloads_identical": (
            query["payload_digest"] == cached_query["payload_digest"]
        ),
        "angle_query_payload_digest": query["payload_digest"],
        "latency_error_code": latency_error["error"]["code"],
        "export_filename": export["filename"],
        "export_download_size_matches": len(download) == export["size_bytes"],
        "all_checks_passed": all(
            [
                health_ok,
                b"RoPE Diagnostics" in index,
                status["status"] == "completed",
                cached_query["cache_hit"] is True,
                query["payload_digest"] == cached_query["payload_digest"],
                latency_error["error"]["code"] == "E_LATENCY_BUDGET",
                len(download) == export["size_bytes"],
            ]
        ),
    }
    output = ROOT / "artifacts" / "http_smoke.json"
    atomic_write_json(output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["all_checks_passed"] else 1


def _request(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    *,
    expected_status: int = 200,
    raw: bool = False,
) -> Any:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        response = urllib.request.urlopen(request, timeout=30)
        status = response.status
        payload = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        payload = exc.read()
    if status != expected_status:
        raise RuntimeError(f"HTTP {status} != {expected_status}: {payload[:500]!r}")
    if raw:
        return payload
    value = json.loads(payload.decode("utf-8"))
    return value.get("data", value) if status == 200 else value


if __name__ == "__main__":
    raise SystemExit(main())
