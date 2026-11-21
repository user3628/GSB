from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from pdb_engine.api import create_app
from pdb_engine.service import EngineService

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = PROJECT_ROOT / "samples" / "scenario.json"


@pytest.fixture
def sample_data() -> dict[str, Any]:
    return json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))


@pytest.fixture
def service(tmp_path: Path) -> EngineService:
    engine = EngineService(tmp_path / "engine.sqlite3")
    engine.initialize()
    engine.load_sample(str(SAMPLE_PATH))
    return engine


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    application = create_app(tmp_path / "api.sqlite3")
    with TestClient(application) as test_client:
        response = test_client.post(
            "/api/v1/samples/load", json={"sample_path": str(SAMPLE_PATH)}
        )
        assert response.status_code == 200, response.text
        yield test_client

