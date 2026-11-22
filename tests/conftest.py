from __future__ import annotations

from pathlib import Path

import pytest

from app.service import TieringService


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


@pytest.fixture
def service(tmp_path: Path):
    instance = TieringService(tmp_path / "tiering.db", samples_dir=SAMPLES)
    instance.startup()
    try:
        yield instance
    finally:
        instance.safe_shutdown("pytest_teardown")


@pytest.fixture
def samples_dir() -> Path:
    return SAMPLES
