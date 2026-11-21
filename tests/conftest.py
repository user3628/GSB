from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.build_fixtures import build_all
from slowquery.config import WorkbenchConfig
from slowquery.service import SlowQueryService


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


@pytest.fixture(scope="session", autouse=True)
def deterministic_fixtures() -> None:
    required = [
        DATA_DIR / "slowquery_sample.sqlite3",
        DATA_DIR / "samples.json",
        DATA_DIR / "snapshot_v1.json",
        DATA_DIR / "snapshot_v2.json",
    ]
    if not all(path.exists() for path in required):
        build_all(DATA_DIR)


@pytest.fixture
def config(tmp_path: Path) -> WorkbenchConfig:
    return WorkbenchConfig(
        database_path=tmp_path / "workbench.sqlite3",
        sample_database_path=DATA_DIR / "slowquery_sample.sqlite3",
    )


@pytest.fixture
def service(config: WorkbenchConfig) -> SlowQueryService:
    return SlowQueryService(config)


@pytest.fixture
def seeded_service(service: SlowQueryService) -> SlowQueryService:
    service.submit_samples(json.loads((DATA_DIR / "samples.json").read_text(encoding="utf-8")))
    service.load_snapshot(json.loads((DATA_DIR / "snapshot_v1.json").read_text(encoding="utf-8")))
    return service


def load_fixture(name: str):
    return json.loads((DATA_DIR / name).read_text(encoding="utf-8"))
