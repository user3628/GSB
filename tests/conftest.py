from __future__ import annotations

import json
import socket
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
import requests
import uvicorn
from fastapi.testclient import TestClient

from seqtxn.api import create_app
from seqtxn.coordinator import create_coordinator_app


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class ThreadedUvicorn:
    def __init__(self, app, port: int) -> None:
        self.server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
        )
        self.port = port
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.server.started:
                return
            time.sleep(0.01)
        raise RuntimeError("server did not start")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


@contextmanager
def running_coordinator(config: dict):
    import tempfile

    tmp = tempfile.TemporaryDirectory()
    config_path = Path(tmp.name) / "coordinator.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    port = free_port()
    server = ThreadedUvicorn(create_coordinator_app(config_path), port)
    server.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.stop()
        tmp.cleanup()


@pytest.fixture
def coordinator_config() -> dict:
    return {
        "hang_seconds": 1.0,
        "default_response": {"outcome": "UNKNOWN", "source": "default"},
        "transactions": {
            "txn-commit": {"responses": [{"outcome": "COMMIT", "source": "commit"}]},
            "txn-rollback": {"responses": [{"outcome": "ROLLBACK", "source": "rollback"}]},
            "txn-hang": {
                "responses": [
                    {"behavior": "HANG", "sleep_ms": 1000, "source": "hang"},
                    {"outcome": "COMMIT", "source": "retry-commit"},
                ]
            },
            "txn-unknown": {"responses": [{"outcome": "UNKNOWN", "source": "unknown"}]},
            "txn-conflict": {
                "responses": [
                    {"outcome": "COMMIT", "source": "first-commit"},
                    {"outcome": "ROLLBACK", "source": "conflict-rollback"},
                ]
            },
            "txn-refused": {"responses": [{"outcome": "COMMIT", "source": "commit"}]},
        },
    }


@pytest.fixture
def coordinator(coordinator_config):
    with running_coordinator(coordinator_config) as url:
        yield url


@pytest.fixture
def closed_port() -> int:
    return free_port()


@pytest.fixture
def component(tmp_path, coordinator, closed_port):
    db_path = tmp_path / "middleware.db"
    app = create_app(
        db_path=db_path,
        coordinator_url=coordinator,
        unreachable_url=f"http://127.0.0.1:{closed_port}",
        query_timeout_seconds=0.15,
        auto_recover=True,
    )
    with TestClient(app) as client:
        yield client, db_path, coordinator


def api(response: requests.Response) -> dict:
    return response.json()

