from __future__ import annotations

import copy
from pathlib import Path

import pytest

from linreg_workbench.config import default_config
from linreg_workbench.workbench import Workbench

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


def fast_config(dataset_id: str = "small", *, folds: int = 3) -> dict:
    config = default_config(dataset_id)
    config["cv"] = {
        "folds": folds,
        "seed": 17,
        "stratify": True,
        "metric": "log_loss",
    }
    config["regularization"] = {
        "mode": "elasticnet",
        "strengths": [0.02],
        "l1_ratios": [0.5],
    }
    config["solver"] = {
        "name": "coordinate_descent",
        "tolerance": 1e-5,
        "max_iter": 100,
        "objective_limit": 1_000_000.0,
    }
    return config


@pytest.fixture()
def wb(tmp_path: Path) -> Workbench:
    return Workbench(tmp_path / "workspace", data_dir=DATA_DIR)


@pytest.fixture()
def training_config() -> dict:
    return copy.deepcopy(fast_config())
