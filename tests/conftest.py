from __future__ import annotations

import json
import socket
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

from grad_fault_lab.config import LabConfig
from grad_fault_lab.injector_app import create_injector_app
from grad_fault_lab.node_app import create_node_app
from grad_fault_lab.store import Store
from grad_fault_lab.validator_app import create_validator_app


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "default.json"


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def make_test_config(tmp_path: Path) -> LabConfig:
    base = LabConfig.load(DEFAULT_CONFIG)
    nodes = []
    for node in base.nodes:
        nodes.append(
            replace(
                node,
                port=free_port(),
                refused_port=free_port(),
            )
        )
    return replace(
        base,
        injector_port=free_port(),
        validator_port=free_port(),
        database_path=tmp_path / "lab.sqlite3",
        artifact_dir=tmp_path / "artifacts",
        nodes=tuple(nodes),
        socket_timeout_seconds=0.08,
        request_timeout_seconds=5.0,
        hang_seconds=10.0,
    )


def write_config_file(config: LabConfig, path: Path) -> Path:
    payload: dict[str, Any] = json.loads(DEFAULT_CONFIG.read_text(encoding="utf-8"))
    payload["host"] = config.host
    payload["injector_port"] = config.injector_port
    payload["validator_port"] = config.validator_port
    payload["database"] = str(config.database_path)
    payload["artifact_dir"] = str(config.artifact_dir)
    payload["socket_timeout_seconds"] = config.socket_timeout_seconds
    payload["request_timeout_seconds"] = config.request_timeout_seconds
    payload["hang_seconds"] = config.hang_seconds
    payload["nodes"] = [
        {
            "node_id": node.node_id,
            "port": node.port,
            "refused_port": node.refused_port,
            "sample_weights": list(node.sample_weights),
        }
        for node in config.nodes
    ]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


@dataclass
class LiveServer:
    server: uvicorn.Server
    thread: threading.Thread

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=8)


@dataclass
class LabRuntime:
    config: LabConfig
    store: Store
    servers: list[LiveServer]
    client: httpx.Client

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.config.validator_port}"

    def stop(self) -> None:
        self.client.close()
        for server in reversed(self.servers):
            server.stop()


def _start_server(app: Any, port: int) -> LiveServer:
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="critical",
            access_log=False,
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return LiveServer(server=server, thread=thread)
        except OSError:
            time.sleep(0.03)
    raise RuntimeError(f"Server on port {port} did not start")


@pytest.fixture(scope="session")
def lab(tmp_path_factory: pytest.TempPathFactory) -> LabRuntime:
    config = make_test_config(tmp_path_factory.mktemp("lab-runtime"))
    servers: list[LiveServer] = []
    for node in config.nodes:
        servers.append(_start_server(create_node_app(config, node.node_id), node.port))
    servers.append(_start_server(create_injector_app(config), config.injector_port))
    store = Store(config.database_path)
    servers.append(_start_server(create_validator_app(config, store=store), config.validator_port))
    client = httpx.Client(trust_env=False, timeout=30.0)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            if client.get(f"http://127.0.0.1:{config.validator_port}/health").status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.03)
    else:
        raise RuntimeError("Validator did not become healthy")
    runtime = LabRuntime(config=config, store=store, servers=servers, client=client)
    yield runtime
    runtime.stop()


@pytest.fixture()
def batch_spec() -> Any:
    def build(
        injection_type: str,
        *,
        round_no: int = 0,
        node_id: str = "node-1",
        shard: int = 0,
        max_retries: int = 0,
        timeout_seconds: float = 0.08,
        apply_attempts: list[int] | None = None,
    ) -> dict[str, Any]:
        return {
            "type": injection_type,
            "target_node": node_id,
            "target_shard": shard,
            "round": round_no,
            "retry": {
                "max_retries": max_retries,
                "backoff_seconds": 0.01,
                "backoff_multiplier": 2.0,
            },
            "params": {
                "timeout_seconds": timeout_seconds,
                "apply_attempts": apply_attempts or [1],
            },
        }

    return build

