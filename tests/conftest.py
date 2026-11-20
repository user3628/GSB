import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gpworkbench.database import Database  # noqa: E402
from gpworkbench.scheduler import SchedulerService  # noqa: E402
from gpworkbench.spec import parse_workload  # noqa: E402


SIMPLE_WORKLOAD = {
    "quota_groups": [
        {"group_id": "g1", "memory_limit": "100Gi", "compute_limit": "1000m"},
        {"group_id": "g2", "memory_limit": "50Gi", "compute_limit": "500m"},
    ],
    "nodes": [
        {
            "node_id": "n1",
            "total_memory": "60Gi",
            "total_compute": "600m",
            "used_memory": "10Gi",
            "group_id": "g1",
            "health": "HEALTHY",
        },
        {
            "node_id": "n2",
            "total_memory": "40Gi",
            "total_compute": "400m",
            "used_memory": "0Gi",
            "group_id": "g1",
            "health": "HEALTHY",
        },
        {
            "node_id": "n3",
            "total_memory": "20Gi",
            "total_compute": "200m",
            "used_memory": "0Gi",
            "group_id": "g2",
            "health": "DRAIN",
        },
    ],
    "models": [
        {"model_id": "m1", "cost_per_sample": 10, "max_batch_samples": 8},
        {"model_id": "m2", "cost_per_sample": 5, "max_batch_samples": 16},
    ],
    "requests": [
        {
            "request_id": "r1",
            "service_name": "svc-a",
            "model_id": "m1",
            "memory_need": "8Gi",
            "compute_need": "100m",
            "samples_per_batch": 4,
            "latency_limit_ms": 100,
            "priority": 5,
            "group_id": "g1",
        },
        {
            "request_id": "r2",
            "service_name": "svc-a",
            "model_id": "m1",
            "memory_need": "6Gi",
            "compute_need": "80m",
            "samples_per_batch": 2,
            "latency_limit_ms": 80,
            "priority": 2,
            "group_id": "g1",
        },
        {
            "request_id": "r3",
            "service_name": "svc-b",
            "model_id": "m2",
            "memory_need": "4Gi",
            "compute_need": "60m",
            "samples_per_batch": 4,
            "latency_limit_ms": 40,
            "priority": 1,
            "group_id": "g1",
        },
        {
            "request_id": "r4",
            "service_name": "svc-c",
            "model_id": "m1",
            "memory_need": "30Gi",
            "compute_need": "900m",
            "samples_per_batch": 2,
            "latency_limit_ms": 60,
            "priority": 9,
            "group_id": "g1",
        },
    ],
}


@pytest.fixture()
def service(tmp_path):
    db = Database(tmp_path / "test.sqlite3")
    svc = SchedulerService(db)
    svc.load_spec(parse_workload(SIMPLE_WORKLOAD))
    return svc
