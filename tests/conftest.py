from __future__ import annotations

import json
from pathlib import Path

import pytest

from h2workbench.engine import Workbench


ROOT = Path(__file__).resolve().parents[1]


def load_sample(name: str) -> dict:
    return json.loads((ROOT / "samples" / name).read_text(encoding="utf-8"))


@pytest.fixture
def wb(tmp_path: Path) -> Workbench:
    return Workbench(tmp_path / "workbench.db")


@pytest.fixture
def sample_loader():
    return load_sample
