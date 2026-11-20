from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from scripts.generate_data import generate_demo_dataset


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture()
def demo_data(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    generate_demo_dataset(data_dir)
    return data_dir


@pytest.fixture()
def clean_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    yield run_dir
    shutil.rmtree(run_dir, ignore_errors=True)
