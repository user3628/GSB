import json
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.db import Database  # noqa: E402


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def db(tmp_path):
    database = Database(tmp_path / "test.db")
    yield database
    database.close()


@pytest.fixture(scope="session")
def endpoint_server():
    """Real local HTTP endpoints: /ok /delay?ms= /hang /flaky."""
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "samples" / "test_endpoints_server.py"), str(port)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.1)
    yield f"http://127.0.0.1:{port}"
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def dead_port():
    """A loopback port where nothing listens (connection refused)."""
    return free_port()


TOPOLOGY = {
    "name": "t-topo",
    "fault_domains": [
        {"name": "fd-a", "isolation_quota": 1},
        {"name": "fd-b", "isolation_quota": 1},
        {"name": "fd-c", "isolation_quota": 1},
    ],
    "services": [
        {"name": "svc-a", "replicas": 2, "fault_domains": ["fd-a", "fd-b"],
         "failover_strategy": "cross_domain"},
        {"name": "svc-b", "replicas": 2, "fault_domains": ["fd-b", "fd-c"],
         "failover_strategy": "cross_domain"},
        {"name": "svc-c", "replicas": 1, "fault_domains": ["fd-c"],
         "failover_strategy": "none"},
        {"name": "svc-d", "replicas": 1, "fault_domains": ["fd-a"],
         "failover_strategy": "none"},
    ],
    "dependencies": [
        {"from": "svc-a", "to": "svc-b", "timeout_ms": 300},
        {"from": "svc-b", "to": "svc-c", "timeout_ms": 300},
    ],
    "budgets": {"injection_concurrency": 2, "injection_queue_capacity": 1,
                "round_time_budget_ms": 1500, "failover_ops_per_round": 1},
}


def make_plan(items, rounds=1, name="p"):
    return {"name": name, "rounds": rounds, "items": items}


def load_json(name):
    return json.loads((ROOT / "samples" / name).read_text(encoding="utf-8"))
