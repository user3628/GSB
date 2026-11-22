from __future__ import annotations

import json
from pathlib import Path

import pytest

from mv_workbench.service import WorkbenchService


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


@pytest.fixture
def service(tmp_path: Path) -> WorkbenchService:
    instance = WorkbenchService(tmp_path / "workbench.db", samples_dir=SAMPLES)
    instance.initialize()
    return instance


@pytest.fixture
def fresh_service(service: WorkbenchService) -> WorkbenchService:
    service.refresh("test-initial-full-build", mode="all")
    return service


@pytest.fixture
def correction_batch() -> dict:
    payload = json.loads((SAMPLES / "batches.json").read_text(encoding="utf-8"))
    return payload["batches"][0]


@pytest.fixture
def stale_batch() -> dict:
    payload = json.loads((SAMPLES / "batches.json").read_text(encoding="utf-8"))
    return payload["batches"][1]


@pytest.fixture
def definitions_v2() -> dict:
    return json.loads((SAMPLES / "view_definitions_v2.json").read_text(encoding="utf-8"))


def group_map(query_result: dict) -> dict[str, dict]:
    return {
        json.dumps(group["key"], sort_keys=True, separators=(",", ":")): group
        for group in query_result["groups"]
    }
