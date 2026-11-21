from __future__ import annotations

import json
from pathlib import Path

import pytest

from chunkops.data import generate_datasets


@pytest.fixture(scope="session")
def dataset_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("chunkops-data")
    generate_datasets(root, seed=20261122, include_large=False, overwrite=True)
    return root


@pytest.fixture(scope="session")
def dataset_manifest(dataset_root: Path) -> dict:
    return json.loads((dataset_root / "manifest.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def fragment_paths(dataset_manifest: dict) -> dict[str, str]:
    return {
        record["fragment_id"]: record["path"]
        for record in dataset_manifest["fragments"]
    }
