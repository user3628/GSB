from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.seed import seed_workbench
from app.service import WorkbenchService


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


def load_sample(name: str) -> Any:
    with (SAMPLES / name).open("r", encoding="utf-8") as handle:
        return json.load(handle)


@pytest.fixture
def policy_v1() -> dict[str, Any]:
    return load_sample("policy-v1.json")


@pytest.fixture
def policy_v2() -> dict[str, Any]:
    return load_sample("policy-v2.json")


@pytest.fixture
def service(tmp_path: Path) -> WorkbenchService:
    return WorkbenchService(tmp_path / "workbench.sqlite3")


@pytest.fixture
def seeded_service(tmp_path: Path) -> WorkbenchService:
    instance = WorkbenchService(tmp_path / "seeded.sqlite3")
    seed_workbench(instance, SAMPLES)
    return instance


@pytest.fixture
def client(tmp_path: Path):
    app = create_app(
        db_path=tmp_path / "api.sqlite3",
        fault_injection=True,
        static_dir=ROOT / "app" / "static",
        samples_dir=SAMPLES,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def seeded_client(client: TestClient):
    response = client.post("/api/demo/seed")
    assert response.status_code == 201, response.text
    return client


@pytest.fixture
def batch_factory():
    def make(
        batch_id: str = "B-TEST",
        *,
        service: str = "orders-api",
        started_at_ms: int = 1_000_000,
        policy_version: int = 1,
    ) -> dict[str, Any]:
        return {
            "batch_id": batch_id,
            "service": service,
            "change_type": "IMAGE_UPGRADE",
            "batch_size": 20,
            "started_at_ms": started_at_ms,
            "policy_version": policy_version,
            "baseline": {
                "sample_count": 40,
                "success_count": 38,
                "latency_p90_ms": 180,
            },
        }

    return make


@pytest.fixture
def observation_factory():
    def make(
        *,
        count: int = 12,
        started_at_ms: int = 1_000_000,
        failures: set[int] | None = None,
        latency: float = 181,
    ) -> list[dict[str, Any]]:
        failures = failures or set()
        return [
            {
                "sequence": index + 1,
                "timestamp_ms": started_at_ms + index * 1_000,
                "metric_type": "CANDIDATE",
                "latency_ms": latency + (index % 3),
                "success": index + 1 not in failures,
            }
            for index in range(count)
        ]

    return make
