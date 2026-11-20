from __future__ import annotations

from pathlib import Path

import pytest

from spmm_ops.samples import generate_samples, load_manifest


@pytest.fixture(scope="session")
def sample_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    destination = tmp_path_factory.mktemp("spmm_samples")
    generate_samples(destination)
    return destination


@pytest.fixture(scope="session")
def manifest(sample_dir: Path) -> dict:
    return load_manifest(sample_dir / "manifest.json")

