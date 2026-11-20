from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class LiveServer:
    def __init__(self, tmp_path: Path):
        self.tmp_path = tmp_path
        self.db_path = tmp_path / "integration.db"
        self.config_path = tmp_path / "config.json"
        self.log_path = tmp_path / "server.log"
        self.port = free_port()
        self.process: subprocess.Popen[bytes] | None = None
        self._write_config()

    def _write_config(self) -> None:
        config = {
            "host": "127.0.0.1",
            "port": self.port,
            "sqlite_path": str(self.db_path),
            "samples_dir": str(SAMPLES),
            "edge_nodes": ["edge-a", "edge-b", "edge-c"],
            "refused_origin_url": "http://127.0.0.1:1",
            "origin_timeout_ms": 2500,
            "max_body_pad_bytes": 65536,
            "seed_content_manifest": str(SAMPLES / "origin_content_v1.json"),
            "seed_policy_file": None,
            "auto_recover": True,
            "debug_errors": True,
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self._write_config()
        log = self.log_path.open("ab")
        env = dict(os.environ)
        env.pop("CDN_SIM_CONFIG", None)
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "workbench.server",
                "--config",
                str(self.config_path),
            ],
            cwd=str(ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        log.close()
        deadline = time.time() + 12
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Server exited early:\n{self.log_path.read_text(errors='replace')}")
            try:
                response = httpx.get(self.base_url + "/api/health", timeout=0.5, trust_env=False)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"Server did not become ready:\n{self.log_path.read_text(errors='replace')}")

    def stop(self, *, force: bool = False) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            if force:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process = None

    def restart(self) -> None:
        self.stop(force=True)
        time.sleep(0.2)
        self.start()

    def client(self, timeout: float = 10.0) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=timeout, trust_env=False)

    def submit_policy(self, payload: dict, expected_status: int = 200) -> dict:
        with self.client() as client:
            response = client.post("/api/policies", json=payload)
        assert response.status_code == expected_status, response.text
        return response.json()

    def request(self, node: str, payload: dict, timeout: float = 10.0) -> httpx.Response:
        with self.client(timeout=timeout) as client:
            return client.post(f"/api/edges/{node}/requests", json=payload)

    def sqlite(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection


@pytest.fixture()
def server(tmp_path: Path):
    live = LiveServer(tmp_path)
    live.start()
    try:
        yield live
    finally:
        live.stop(force=True)


@pytest.fixture()
def policy_v1() -> dict:
    return json.loads((SAMPLES / "policy_v1.json").read_text(encoding="utf-8"))


@pytest.fixture()
def policy_v2() -> dict:
    return json.loads((SAMPLES / "policy_v2.json").read_text(encoding="utf-8"))

