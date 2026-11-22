from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest

from feature_workbench import Workbench
from feature_workbench.config import load_yaml, normalize_config, dump_config
from feature_workbench.generator import generate_demo


@pytest.fixture(scope="session")
def demo_source(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("demo-source")
    generate_demo(path / "data")
    return path


@pytest.fixture
def workspace(tmp_path: Path, demo_source: Path) -> Path:
    root = tmp_path / "workspace"
    shutil.copytree(demo_source / "data", root / "data")
    return root


@pytest.fixture
def workbench(workspace: Path) -> Workbench:
    return Workbench(root=workspace)


@pytest.fixture
def make_workspace(tmp_path: Path, demo_source: Path):
    counter = {"value": 0}

    def factory() -> Path:
        counter["value"] += 1
        root = tmp_path / f"workspace-{counter['value']}"
        shutil.copytree(demo_source / "data", root / "data")
        return root

    return factory


def read_manifest(root: Path) -> dict:
    with (root / "data" / "manifest.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_manifest(root: Path, manifest: dict) -> None:
    from feature_workbench.util import atomic_write_json

    atomic_write_json(root / "data" / "manifest.json", manifest)


def read_config(root: Path) -> dict:
    return load_yaml(root / "data" / "config.yaml")


def write_config(root: Path, config: dict) -> dict:
    normalized = normalize_config(config)
    dump_config(root / "data" / "config.yaml", normalized)
    return normalized


def mutate_config(root: Path, changes: dict) -> dict:
    config = read_config(root)
    cursor = config
    for key, value in changes.items():
        parts = key.split(".")
        current = cursor
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value
    return write_config(root, config)


def clone_config(root: Path) -> dict:
    return copy.deepcopy(read_config(root))

