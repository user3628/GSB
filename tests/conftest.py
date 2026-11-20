from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.models import PolicySnapshot, TraceSample


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_json(name: str) -> dict[str, Any]:
    return json.loads((PROJECT_ROOT / "samples" / name).read_text(encoding="utf-8"))


@pytest.fixture
def policy_v1() -> PolicySnapshot:
    return PolicySnapshot.model_validate(load_json("policy_v1.json"))


@pytest.fixture
def policy_v2() -> PolicySnapshot:
    return PolicySnapshot.model_validate(load_json("policy_v2_conflict_ambiguous.json"))


@pytest.fixture
def trace() -> TraceSample:
    return TraceSample.model_validate(load_json("trace_baseline.json"))


@pytest.fixture
def v1_payload() -> dict[str, Any]:
    return load_json("policy_v1.json")


@pytest.fixture
def v2_payload() -> dict[str, Any]:
    return load_json("policy_v2_conflict_ambiguous.json")


@pytest.fixture
def trace_payload() -> dict[str, Any]:
    return load_json("trace_baseline.json")


@pytest.fixture
def client(tmp_path: Path):
    application = create_app(tmp_path / "workbench-test.db")
    with TestClient(application) as test_client:
        yield test_client
