from __future__ import annotations

from pathlib import Path

import pytest

from blockgemm.manifest import generate_samples


@pytest.fixture(scope="session")
def manifest_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("blockgemm-samples")
    generate_samples(root, force=True)
    return root / "manifest.json"
