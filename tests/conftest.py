from __future__ import annotations

from pathlib import Path

import pytest

from image_workbench.demo_data import generate_demo_data
from image_workbench.manifest import load_manifest


@pytest.fixture(scope="session")
def sample_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("samples")
    generate_demo_data(directory)
    return directory


@pytest.fixture
def manifest(sample_dir: Path):
    return load_manifest(sample_dir / "manifest.json")


@pytest.fixture
def fragment_a(manifest):
    return manifest.fragments[1]

