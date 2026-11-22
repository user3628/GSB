from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.service import GovernanceService

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
T0 = 1_767_225_600_000
T1 = 1_798_761_600_000
GI = 1_073_741_824
MI = 1_048_576


def sample(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


@pytest.fixture
def service(tmp_path):
    svc = GovernanceService(str(tmp_path / "governance.sqlite"))
    svc.startup()
    return svc


@pytest.fixture
def loaded_service(service):
    service.load_snapshot(sample("policies_hierarchical_v2.json"))
    return service


@pytest.fixture
def batch():
    return sample("requests_batch.json")
