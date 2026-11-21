from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLES = PROJECT_ROOT / "samples"


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "scaler.db"


@pytest.fixture
def client(db_path: Path):
    app = create_app(db_path, SAMPLES)
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


def load_sample(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


def round_body(round_id: str, observation_name: str, policy_version: int = 1) -> dict:
    return {
        "round_id": round_id,
        "policy_version": policy_version,
        "observations": load_sample(observation_name),
    }
