from __future__ import annotations

from pathlib import Path

import pytest

from qworkbench.config import WorkbenchConfig
from qworkbench.samples import SampleDataset, dataset_from_directory


ROOT = Path(__file__).resolve().parents[1]
DEMO_DATA = ROOT / "data" / "demo"
DEMO_CONFIG = ROOT / "configs" / "demo.json"


@pytest.fixture(scope="session")
def config() -> WorkbenchConfig:
    return WorkbenchConfig.load(DEMO_CONFIG)


@pytest.fixture(scope="session")
def config_path() -> Path:
    return DEMO_CONFIG


@pytest.fixture(scope="session")
def dataset() -> SampleDataset:
    return dataset_from_directory(DEMO_DATA)


@pytest.fixture()
def workdir(tmp_path: Path) -> Path:
    path = tmp_path / "work"
    path.mkdir()
    return path
