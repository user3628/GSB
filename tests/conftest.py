from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dep_engine import Engine, generate_demo_data


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    generate_demo_data(tmp_path)
    return tmp_path


@pytest.fixture()
def engine(workspace: Path) -> Engine:
    return Engine(
        workspace,
        state_dir="state",
        views_dir="views",
        config_path="config/analysis.yaml",
        manifest_path="data/manifest.json",
    )
