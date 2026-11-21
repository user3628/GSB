from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.engine import RepairEngine
from app.store import Store


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def read_json(name: str) -> dict:
    return json.loads((DATA / name).read_text(encoding="utf-8"))


@pytest.fixture
def sample_layout() -> dict:
    return read_json("sample_layout.json")


@pytest.fixture
def sample_trace() -> dict:
    return read_json("sample_trace.json")


@pytest.fixture
def engine(tmp_path: Path) -> RepairEngine:
    store = Store(tmp_path / "engine.db")
    instance = RepairEngine(store)
    instance.recover()
    return instance


@pytest.fixture
def loaded_engine(engine: RepairEngine, sample_layout: dict) -> RepairEngine:
    engine.load_layout(sample_layout)
    return engine
