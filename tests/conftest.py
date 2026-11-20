from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from vadkit.audio import Dataset, load_dataset
from vadkit.samplegen import generate_dataset


@pytest.fixture(scope="session")
def dataset_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    target = tmp_path_factory.mktemp("vadkit-data")
    generate_dataset(target)
    return target


@pytest.fixture(scope="session")
def dataset(dataset_dir: Path) -> Dataset:
    return load_dataset(dataset_dir / "manifest.json")


@pytest.fixture()
def dataset_copy(dataset_dir: Path, tmp_path: Path) -> Path:
    target = tmp_path / "data-copy"
    shutil.copytree(dataset_dir, target)
    return target

