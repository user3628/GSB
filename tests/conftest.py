from __future__ import annotations

import copy
import sys
from decimal import Decimal
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.service import ConsistencyService  # noqa: E402
from app.snapshot import attach_checksum, load_json_decimal, parse_snapshot  # noqa: E402


@pytest.fixture
def root() -> Path:
    return ROOT


def read_payload(name: str) -> dict:
    return load_json_decimal((ROOT / "data" / name).read_text(encoding="utf-8"))


@pytest.fixture
def baseline_payload() -> dict:
    return read_payload("baseline.json")


@pytest.fixture
def adjusted_payload() -> dict:
    return read_payload("adjusted.json")


@pytest.fixture
def crash_payload() -> dict:
    return read_payload("crash_candidate.json")


@pytest.fixture
def reseal():
    def _reseal(payload: dict) -> dict:
        return attach_checksum(copy.deepcopy(payload))

    return _reseal


@pytest.fixture
def snapshot_from_payload():
    def _snapshot(payload: dict):
        return parse_snapshot(payload)

    return _snapshot


@pytest.fixture
def service(tmp_path: Path) -> ConsistencyService:
    coordinator = ConsistencyService(tmp_path / "consistency.sqlite3")
    coordinator.initialize(recover=True)
    return coordinator
