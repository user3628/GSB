from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from sonic_sep.generate import generate_dataset
from sonic_sep.workspace import Workspace


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def dataset_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("sonic-sep-dataset")
    samples = root / "samples"
    generate_dataset(samples, seed=20261123, durations=(2.0, 2.2, 2.4))
    configs = root / "configs"
    configs.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PROJECT_ROOT / "configs" / "default.yaml", configs / "default.yaml")
    return root


def imported_workspace(dataset_root: Path, state_root: Path) -> Workspace:
    workspace = Workspace(dataset_root, state_root=state_root)
    workspace.import_dataset(dataset_root / "samples" / "manifest.json", dataset_root / "configs" / "default.yaml")
    return workspace


@pytest.fixture()
def workspace(dataset_root: Path, tmp_path: Path) -> Workspace:
    return imported_workspace(dataset_root, tmp_path / "state")


def write_config(path: Path, overrides: dict) -> Path:
    base = yaml.safe_load((PROJECT_ROOT / "configs" / "default.yaml").read_text(encoding="utf-8"))
    base.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(base, sort_keys=True), encoding="utf-8")
    return path

