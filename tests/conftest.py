from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from quota_scheduler.service import QuotaScheduler


def write_bundle(
    directory: Path,
    *,
    groups: list[dict[str, Any]] | None = None,
    nodes: list[dict[str, Any]] | None = None,
    models: list[dict[str, Any]] | None = None,
    requests: list[dict[str, Any]] | None = None,
    config: dict[str, Any] | None = None,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    groups = groups or [
        {"group_id": "g1", "memory_limit": "100Gi", "compute_limit": "100000m"}
    ]
    nodes = nodes or [
        {
            "node_id": "n1",
            "total_memory": "80Gi",
            "available_compute": "10000m",
            "occupied_memory": "0Gi",
            "occupied_compute": "0m",
            "quota_group": "g1",
            "healthy": True,
        }
    ]
    models = models or [
        {
            "model_id": "m1",
            "max_batch_samples": 8,
            "unit_sample_cost": "100m",
        }
    ]
    requests = requests or []
    config = config or {}
    for filename, payload in (
        ("quota_groups.json", {"quota_groups": groups}),
        ("nodes.json", {"nodes": nodes}),
        ("models.json", {"models": models}),
        ("requests.json", {"requests": requests}),
        ("config.json", config),
    ):
        (directory / filename).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return directory


def make_service(
    tmp_path: Path,
    *,
    groups: list[dict[str, Any]] | None = None,
    nodes: list[dict[str, Any]] | None = None,
    models: list[dict[str, Any]] | None = None,
    requests: list[dict[str, Any]] | None = None,
    config: dict[str, Any] | None = None,
    db_name: str = "state.db",
) -> QuotaScheduler:
    samples = write_bundle(
        tmp_path / "samples",
        groups=groups,
        nodes=nodes,
        models=models,
        requests=requests,
        config=config,
    )
    service = QuotaScheduler(tmp_path / db_name)
    service.bootstrap(samples)
    return service


def request_payload(
    request_id: str,
    *,
    model_id: str = "m1",
    node_id: str = "n1",
    group: str = "g1",
    memory: str = "2Gi",
    samples: int = 2,
    latency: str = "1000ms",
    priority: int = 5,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "service_name": f"svc-{request_id}",
        "model_id": model_id,
        "node_id": node_id,
        "memory_demand": memory,
        "samples_per_batch": samples,
        "expected_latency": latency,
        "priority": priority,
        "quota_group": group,
    }


@pytest.fixture
def service(tmp_path: Path) -> QuotaScheduler:
    return make_service(tmp_path)

