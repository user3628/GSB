from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn

from capacity_workbench.test_service import create_app as create_target_app


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def target_url() -> Iterator[str]:
    port = free_tcp_port()
    config = uvicorn.Config(
        create_target_app(), host="127.0.0.1", port=port, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(200):
        try:
            response = httpx.get(f"{base}/health", timeout=0.2)
            if response.status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.025)
    else:  # pragma: no cover - startup failure diagnostic
        raise RuntimeError("deterministic target service did not start")
    yield f"{base}/sample"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def base_plan(target_url: str) -> dict:
    return {
        "name": "test-plan",
        "target_url": target_url,
        "stages": [
            {"concurrency": 1, "duration_seconds": 1},
            {"concurrency": 2, "duration_seconds": 1},
        ],
        "thresholds": {
            "p99_ms": 100.0,
            "error_rate": 0.05,
            "min_throughput_rps": 1.0,
        },
        "decision_rule": {
            "on_reject": "fallback",
            "max_retries": 1,
            "step_down_factor": 0.5,
        },
        "seed": 12345,
        "profile": "stable",
        "request_timeout_ms": 200,
        "sampling": {
            "requests_per_second_per_worker": 100,
            "min_samples_for_percentiles": 100,
        },
    }


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "workbench.db"

