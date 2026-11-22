from __future__ import annotations

import shutil
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_ROOT = REPO_ROOT / "samples" / "main"


@pytest.fixture
def sample_copy(tmp_path: Path) -> Path:
    target = tmp_path / "data"
    shutil.copytree(SAMPLE_ROOT, target)
    return target


@pytest.fixture
def ragged_copy(tmp_path: Path) -> Path:
    target = tmp_path / "ragged"
    shutil.copytree(REPO_ROOT / "samples" / "ragged", target)
    return target
