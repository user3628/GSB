from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from graph_bench.service import BenchService  # noqa: E402
from graph_bench.storage import Storage  # noqa: E402

SAMPLES = ROOT / "samples"


def load_sample(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


@pytest.fixture()
def service(tmp_path: Path) -> BenchService:
    storage = Storage(tmp_path / "graph_bench.db")
    return BenchService(storage)


@pytest.fixture()
def seeded_service(service: BenchService) -> BenchService:
    service.submit_snapshot(load_sample("snapshot_v1.json"), "seed-v1")
    return service
