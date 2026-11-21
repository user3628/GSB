from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import create_app


ROOT = Path(__file__).resolve().parent.parent


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def backend_fixture(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    temp_dir = tmp_path_factory.mktemp("backend")
    port = _free_port()
    config = {
        "global_concurrency": 2,
        "queue_limit": 2,
        "default_time_budget_ms": 500,
        "write_timeout_ms": 500,
        "read_timeout_ms": 500,
        "backends": [
            {
                "id": "test-backend",
                "url": f"http://127.0.0.1:{port}",
                "capacity_bytes": 30000,
                "max_concurrency": 2,
                "max_parts_per_session": 8,
                "estimated_latency_ms": 20,
                "enabled": True,
            }
        ],
    }
    config_path = temp_dir / "backends.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    env = os.environ.copy()
    env["BACKEND_CONFIG_FILE"] = str(config_path)
    env["BACKEND_ID"] = "test-backend"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.backend_sim:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{base_url}/health", timeout=0.25)
            if response.status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(f"backend simulator exited early: {stderr}")
        time.sleep(0.05)
    else:
        process.kill()
        raise RuntimeError("backend simulator did not become healthy")
    try:
        yield {"base_url": base_url, "config_path": config_path, "config": config}
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.fixture
def backend(backend_fixture: dict[str, Any]) -> Iterator[dict[str, Any]]:
    base_url = backend_fixture["base_url"]
    httpx.post(f"{base_url}/admin/mode", json={"mode": "normal"}, timeout=2).raise_for_status()
    httpx.post(f"{base_url}/admin/reset", timeout=2).raise_for_status()
    yield backend_fixture


@pytest.fixture
def app_client(
    tmp_path: Path, backend: dict[str, Any]
) -> Iterator[tuple[TestClient, dict[str, Any]]]:
    config_path = backend["config_path"]
    app = create_app(db_file=str(tmp_path / "test.sqlite3"), settings_file=str(config_path))
    with TestClient(app) as client:
        yield client, backend
