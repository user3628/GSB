from __future__ import annotations

from pathlib import Path

import pytest

from gridpipe.generator import generate_samples


@pytest.fixture(scope="session")
def sample_project(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("gridpipe-sample")
    generate_samples(root / "data", seed=20261122)
    return root


@pytest.fixture()
def manifest_path(sample_project: Path) -> Path:
    return sample_project / "data" / "manifest.json"
