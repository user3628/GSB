from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from gsb.data import generate_datasets, load_dataset
from gsb.workspace import Workspace


FAST_CONFIG = {
    "bins": 8,
    "rounds": 4,
    "learning_rate": 0.15,
    "max_depth": 2,
    "min_leaf": 10,
    "l2": 1.0,
    "calibrator": "platt",
    "calibration_folds": 2,
    "calibration_seed": 7,
    "diagnostic_bins": 5,
    "quantize_decimals": 10,
}


@pytest.fixture(scope="session")
def project_root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("gsb-test-project")
    (root / "state").mkdir(parents=True, exist_ok=True)
    (root / "out").mkdir(parents=True, exist_ok=True)
    generate_datasets(root / "data")
    return root


@pytest.fixture
def small_dataset(project_root: Path):
    return load_dataset("small", project_root=project_root, data_dir=project_root / "data")


@pytest.fixture
def medium_dataset(project_root: Path):
    return load_dataset("medium", project_root=project_root, data_dir=project_root / "data")


@pytest.fixture
def make_workspace(project_root: Path):
    def factory(name: str) -> Workspace:
        state_dir = project_root / "state" / name
        if state_dir.exists():
            shutil.rmtree(state_dir)
        return Workspace(project_root, state_dir=state_dir, data_dir=project_root / "data")

    return factory


@pytest.fixture
def fast_config():
    return dict(FAST_CONFIG)
