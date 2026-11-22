from __future__ import annotations

import http.client
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from app.config import Settings
from app.engine import RuntimeEngine
from app.errors import BusinessError, E_NOT_FOUND


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class EndpointServer:
    base_url: str
    refuse_url: str
    process: subprocess.Popen[bytes]

    def stats(self) -> dict[str, int]:
        connection = http.client.HTTPConnection("127.0.0.1", int(self.base_url.rsplit(":", 1)[1]), timeout=2)
        try:
            connection.request("GET", "/stats")
            response = connection.getresponse()
            import json

            return json.loads(response.read().decode("utf-8"))
        finally:
            connection.close()


def _wait_health(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
        try:
            connection.request("GET", "/health")
            response = connection.getresponse()
            response.read()
            if response.status == 200:
                return
        except Exception as exc:
            last_error = exc
        finally:
            connection.close()
        time.sleep(0.05)
    raise RuntimeError(f"test endpoint did not become healthy: {last_error}")


@pytest.fixture(scope="session")
def endpoint_server(tmp_path_factory: pytest.TempPathFactory) -> EndpointServer:
    port = free_port()
    refuse_port = free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tools.test_endpoint",
            "--port",
            str(port),
            "--refuse-port",
            str(refuse_port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_health(port)
        yield EndpointServer(
            base_url=f"http://127.0.0.1:{port}",
            refuse_url=f"http://127.0.0.1:{refuse_port}",
            process=process,
        )
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


@pytest.fixture
def engine_factory(tmp_path: Path, endpoint_server: EndpointServer):
    counter = 0

    def factory(**overrides):
        nonlocal counter
        counter += 1
        db_path = tmp_path / f"runtime-{counter}.sqlite3"
        settings = Settings(
            db_path=db_path,
            log_path=tmp_path / f"runtime-{counter}.log",
            endpoint_base=endpoint_server.base_url,
            start_timeout_s=0.12,
            exec_timeout_s=0.15,
            default_queue_capacity=8,
            max_queue_wait_s=5.0,
        )
        for key, value in overrides.items():
            settings = settings.__class__(**{**settings.__dict__, key: value})
        return RuntimeEngine(settings)

    return factory


def spec(function_id: str = "fn-test", **overrides) -> dict:
    value = {
        "function_id": function_id,
        "runtime": "python3.11",
        "init_us": 8_000,
        "memory_bytes": 16 * 1024 * 1024,
        "per_instance_concurrency": 1,
        "function_concurrency_limit": 1,
        "reserved_quota": 0,
        "init_concurrency": 1,
        "prewarm_instances": 0,
        "queue_capacity": 2,
    }
    value.update(overrides)
    return value


def wait_for(predicate, timeout: float = 3.0, interval: float = 0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition was not reached before timeout")


def call_status(engine: RuntimeEngine, call_id: str):
    try:
        return engine.get_call(call_id)["status"]
    except BusinessError as exc:
        if exc.code == E_NOT_FOUND:
            return None
        raise

