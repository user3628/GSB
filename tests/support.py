from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parent.parent


def free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def wait_http(url: str, timeout: float = 12.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=1.0)
            if response.status_code == 200:
                return response.json()
        except Exception as exc:
            last_error = exc
        time.sleep(0.06)
    raise RuntimeError(f"service not ready {url}: {last_error}")


def wait_tcp(host: str, port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.06)
    raise RuntimeError(f"tcp service not ready {host}:{port}")


def wait_port_free(host: str, port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
                return
            except OSError:
                time.sleep(0.06)
    raise RuntimeError(f"port did not free {host}:{port}")


def stop_process(process: subprocess.Popen | None, *, force: bool = False) -> None:
    if process is None or process.poll() is not None:
        return
    if force:
        process.kill()
    else:
        process.terminate()
    try:
        process.wait(timeout=4.0)
    except subprocess.TimeoutExpired:
        process.kill()
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=3.0)


def start_upstreams(
    *,
    host: str,
    backend_port: int,
    egress_port: int,
    alt_backend_port: int,
    log_path: Path,
) -> subprocess.Popen:
    handle = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tools.local_upstreams",
            "--mode",
            "all",
            "--host",
            host,
            "--backend-port",
            str(backend_port),
            "--egress-port",
            str(egress_port),
            "--alt-backend-port",
            str(alt_backend_port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    process._test_log_handle = handle  # type: ignore[attr-defined]
    wait_http(f"http://{host}:{backend_port}/health")
    wait_http(f"http://{host}:{alt_backend_port}/health")
    wait_tcp(host, egress_port)
    return process


def start_workbench(
    *,
    host: str,
    port: int,
    db_path: Path,
    enable_failpoints: bool,
    log_path: Path,
) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(
        {
            "WORKBENCH_DB": str(db_path),
            "WORKBENCH_PORT": str(port),
            "WORKBENCH_ENABLE_FAILPOINTS": "1" if enable_failpoints else "0",
        }
    )
    handle = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            host,
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    process._test_log_handle = handle  # type: ignore[attr-defined]
    wait_http(f"http://{host}:{port}/workbench/api/health")
    return process


def reverse_route(
    route_id: str,
    prefix: str,
    upstream_port: int,
    *,
    methods: list[str] | None = None,
    timeout_ms: int = 600,
    retries: int = 0,
    health_status: str = "healthy",
    priority: int = 100,
) -> dict[str, Any]:
    return {
        "route_id": route_id,
        "kind": "reverse",
        "methods": methods or ["GET", "POST"],
        "path_prefix": prefix,
        "upstream": f"http://127.0.0.1:{upstream_port}",
        "timeout_ms": timeout_ms,
        "retries": retries,
        "headers": {"X-Test-Route": route_id},
        "health_status": health_status,
        "priority": priority,
    }


def forward_route(egress_port: int, target_port: int) -> dict[str, Any]:
    return {
        "route_id": "forward-main",
        "kind": "forward",
        "methods": ["GET", "POST"],
        "target_host": "127.0.0.1",
        "target_port": target_port,
        "upstream": f"http://127.0.0.1:{egress_port}",
        "timeout_ms": 800,
        "retries": 1,
        "headers": {"X-Test-Route": "forward-main"},
        "health_status": "healthy",
        "priority": 100,
    }


def install_full_snapshot(base_url: str, routes: list[dict[str, Any]]) -> dict[str, Any]:
    current = httpx.get(base_url + "/workbench/api/health", timeout=3.0).json()[
        "current_config_version"
    ]
    response = httpx.post(
        base_url + "/workbench/api/config/snapshot",
        json={"version": current + 1, "routes": routes},
        timeout=8.0,
    )
    assert response.status_code == 200, response.text
    return response.json()


def current_version(base_url: str) -> int:
    return int(
        httpx.get(base_url + "/workbench/api/health", timeout=3.0).json()[
            "current_config_version"
        ]
    )
