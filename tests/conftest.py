from __future__ import annotations

import contextlib
import socket
import threading
import time
from pathlib import Path
from typing import Iterator

import pytest
import requests
import uvicorn
from fastapi.testclient import TestClient

from gateway.api import create_app
from gateway.config import GatewayConfig
from scripts.mock_backend import create_app as create_backend_app


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def backend_url() -> Iterator[str]:
    port = _free_port()
    app = create_backend_app(str(Path(__file__).resolve().parents[1] / "samples" / "backend_behaviors.json"))
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        timeout_graceful_shutdown=0.2,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="mock-backend-test", daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            if requests.get(url + "/health", timeout=0.2).status_code == 200:
                break
        except requests.RequestException:
            time.sleep(0.02)
    else:
        raise RuntimeError("mock backend did not start")
    yield url
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def backend(backend_url: str) -> Iterator[str]:
    requests.post(backend_url + "/admin/reset", timeout=1)
    yield backend_url
    requests.post(backend_url + "/admin/reset", timeout=1)


@pytest.fixture
def gateway_factory(tmp_path: Path):
    with contextlib.ExitStack() as stack:
        counter = 0

        def make(
            *,
            global_concurrency_limit: int = 2,
            queue_limit: int = 20,
            round_time_budget_ms: int = 500,
            recovery_requeue: bool = True,
            max_recovery_attempts: int = 3,
        ) -> tuple[TestClient, GatewayConfig]:
            nonlocal counter
            counter += 1
            config = GatewayConfig(
                db_path=str(tmp_path / f"gateway-{counter}.db"),
                global_concurrency_limit=global_concurrency_limit,
                queue_limit=queue_limit,
                round_time_budget_ms=round_time_budget_ms,
                recovery_requeue=recovery_requeue,
                max_recovery_attempts=max_recovery_attempts,
            )
            app = create_app(config)
            client = stack.enter_context(TestClient(app))
            return client, config

        yield make

