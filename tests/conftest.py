from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


@pytest.fixture
def client(tmp_path: Path):
    settings = Settings(
        database_path=tmp_path / "gateway.db",
        samples_dir=SAMPLES,
        auto_seed=True,
        auto_recover=True,
        throttle_rejection_threshold=3,
    )
    app = create_app(settings)
    with TestClient(app) as test_client:
        test_client.settings = settings  # type: ignore[attr-defined]
        yield test_client


@pytest.fixture
def fixed_batch() -> dict:
    return json.loads((SAMPLES / "events_batch.json").read_text(encoding="utf-8"))


@pytest.fixture
def seed_policy() -> dict:
    return json.loads((SAMPLES / "policy_snapshot.json").read_text(encoding="utf-8"))


def state_fingerprint(client: TestClient) -> dict:
    body = client.get("/api/v1/state").json()
    return {
        item["tenant_id"]: {
            "status": item["state"]["status"],
            "tokens_units": item["state"]["tokens_units"],
            "used_bytes_units": item["state"]["used_bytes_units"],
            "used_request_units": item["state"]["used_request_units"],
            "last_refill_ms": item["state"]["last_refill_ms"],
        }
        for item in body["tenants"]
    }
