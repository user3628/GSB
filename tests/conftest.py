from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_coordinator import ComponentDeclaration, Coordinator, Snapshot  # noqa: E402
from config_coordinator.codec import compute_snapshot_checksum  # noqa: E402


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def endpoint_server() -> Iterator[str]:
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "config_coordinator.endpoint_server",
            "--port",
            str(port),
            "--log-level",
            "critical",
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    deadline = time.monotonic() + 15
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("endpoint server exited during startup")
            try:
                if httpx.get(base + "/health", timeout=0.2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        else:
            raise RuntimeError("endpoint server did not start")
        yield base
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.fixture
def endpoint_base(endpoint_server: str) -> str:
    response = httpx.post(endpoint_server + "/control/reset", timeout=5)
    response.raise_for_status()
    return endpoint_server


def snapshot(version: int, configs: dict[str, Any], generated_at: str | None = None) -> Snapshot:
    timestamp = generated_at or f"2026-01-{version:02d}T00:00:00Z"
    return Snapshot(
        version=version,
        generated_at=timestamp,
        configs=configs,
        checksum=compute_snapshot_checksum(version, timestamp, configs),
    )


def component(
    component_id: str,
    config_keys: list[str],
    *,
    endpoint_url: str = "http://127.0.0.1:1/push",
    cluster_id: str = "east",
    logical_component: str | None = None,
    depends_on: list[str] | None = None,
    expected_config_version: int = 1,
) -> ComponentDeclaration:
    return ComponentDeclaration(
        component_id=component_id,
        config_keys=config_keys,
        expected_config_version=expected_config_version,
        endpoint_url=endpoint_url,
        cluster_id=cluster_id,
        logical_component=logical_component,
        depends_on=depends_on or [],
    )


def sample_components(endpoint_base: str) -> list[ComponentDeclaration]:
    raw = json.loads((ROOT / "samples" / "components.json").read_text(encoding="utf-8"))[
        "components"
    ]
    rendered = json.dumps(raw).replace("{endpoint_base}", endpoint_base)
    return [ComponentDeclaration.model_validate(item) for item in json.loads(rendered)]


@pytest.fixture
def coordinator(tmp_path: Path) -> Coordinator:
    return Coordinator(str(tmp_path / "coordinator.db"))

