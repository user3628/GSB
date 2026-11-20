from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import httpx
import pytest
import uvicorn

from confighub.client_endpoint import create_client_app
from confighub.service import ConfigService, snapshot_digest


CONFIG_V1 = {
    "w0": 1.2,
    "w1": -0.5,
    "w2": 0.25,
    "normalization_scale": 1.5,
    "threshold": 0.1,
}
CONFIG_V2 = {
    "w0": 2.0,
    "w1": 0.7,
    "w2": -0.3,
    "normalization_scale": 0.8,
    "threshold": 0.25,
}
CONFIG_KEYS = list(CONFIG_V1)
SAMPLE_SEED = 7
SAMPLE_SIZE = 16


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


@dataclass
class EndpointHarness:
    base_url: str
    port: int
    app: Any
    thread: threading.Thread
    server: uvicorn.Server

    def push_url(self, profile: str) -> str:
        return f"{self.base_url}/push/{profile}"

    def set_delay(self, profile: str, delay: float) -> None:
        response = httpx.post(
            self.base_url + "/admin/delay",
            json={"profile": profile, "delay_seconds": delay},
            timeout=5,
            trust_env=False,
        )
        response.raise_for_status()

    def set_mode(self, profile: str, mode: str) -> None:
        response = httpx.post(
            self.base_url + "/admin/mode",
            json={"profile": profile, "mode": mode},
            timeout=5,
            trust_env=False,
        )
        response.raise_for_status()

    def stats(self) -> dict[str, Any]:
        response = httpx.get(self.base_url + "/stats", timeout=5, trust_env=False)
        response.raise_for_status()
        return response.json()["profiles"]


@pytest.fixture
def endpoint() -> Iterator[EndpointHarness]:
    port = free_port()
    app = create_client_app()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        if not thread.is_alive():
            raise RuntimeError("test endpoint failed to start")
        time.sleep(0.01)
    if not server.started:
        raise RuntimeError("test endpoint startup timeout")
    harness = EndpointHarness(f"http://127.0.0.1:{port}", port, app, thread, server)
    try:
        yield harness
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture
def service(tmp_path: Path) -> ConfigService:
    return ConfigService(tmp_path / "config-hub.sqlite")


@pytest.fixture
def config_v1() -> dict[str, float]:
    return dict(CONFIG_V1)


@pytest.fixture
def config_v2() -> dict[str, float]:
    return dict(CONFIG_V2)


def submit(service: ConfigService, version: int, config: dict[str, float]) -> dict[str, Any]:
    generated_at = f"2026-01-01T00:00:{version:02d}+00:00"
    return service.submit_snapshot(
        version,
        config,
        digest=snapshot_digest(version, generated_at, config),
        generated_at=generated_at,
    )


def register(
    service: ConfigService,
    endpoint: EndpointHarness,
    client_id: str,
    profile: str | None = None,
    *,
    timeout: float = 1.0,
) -> dict[str, Any]:
    return service.register_client(
        client_id,
        CONFIG_KEYS,
        endpoint.push_url(profile or client_id),
        timeout_seconds=timeout,
        sample_seed=SAMPLE_SEED,
        sample_size=SAMPLE_SIZE,
    )

