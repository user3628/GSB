from __future__ import annotations

import shutil
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


def copy_workspace(destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copytree(REPO / "data", destination / "data")
    (destination / "config").mkdir()
    shutil.copy2(REPO / "config" / "default.yaml", destination / "config" / "default.yaml")
    return destination


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    return copy_workspace(tmp_path / "workspace")
