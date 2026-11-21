from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from mirror_judge.config import Settings
from mirror_judge.main import create_app


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def load_jsonl(name: str) -> list[dict]:
    return [json.loads(line) for line in (DATA / name).read_text(encoding="utf-8").splitlines() if line]


def load_json(name: str) -> dict:
    return json.loads((DATA / name).read_text(encoding="utf-8"))


@pytest.fixture
def records() -> list[dict]:
    return load_jsonl("mirror_records.jsonl")


@pytest.fixture
def increment_records() -> list[dict]:
    return load_jsonl("mirror_records_increment.jsonl")


@pytest.fixture
def policy() -> dict:
    return load_json("policy_baseline.json")


@pytest.fixture
def client_factory(tmp_path: Path):
    clients: list[TestClient] = []
    counter = 0

    def make(target_url: str = "http://127.0.0.1:8765/_test/replay") -> TestClient:
        nonlocal counter
        counter += 1
        settings = Settings(tmp_path / f"mirror-{counter}.sqlite", target_url)
        app = create_app(settings)
        client = TestClient(app, raise_server_exceptions=False)
        clients.append(client)
        return client

    yield make
    for client in clients:
        client.close()


def bootstrap(client: TestClient, records: list[dict], policy: dict, *, batch_id: str = "fixture-base") -> dict:
    response = client.post(
        "/api/v1/snapshots",
        json={
            "base_version": None,
            "snapshot_version": "snap-1",
            "batch_id": batch_id,
            "records": records,
            "policy": policy,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def live_server(tmp_path: Path):
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    db_path = tmp_path / "live.sqlite"
    env = os.environ.copy()
    env.update(
        {
            "MIRROR_DB_PATH": str(db_path),
            "MIRROR_REPLAY_TARGET": f"{base_url}/_test/replay",
            "MIRROR_ENABLE_TEST_HOOKS": "0",
        }
    )
    process = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "scripts" / "run_server.py"),
            "--port",
            str(port),
            "--db",
            str(db_path),
            "--replay-target",
            f"{base_url}/_test/replay",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                if httpx.get(f"{base_url}/health", timeout=1.0).status_code == 200:
                    break
            except Exception:
                time.sleep(0.1)
        else:
            raise AssertionError("live server failed to start")
        with httpx.Client(base_url=base_url, timeout=20.0) as client:
            yield base_url, client
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
