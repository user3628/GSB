from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from headworkbench.demo import reset_stage_cache, reset_versions
from headworkbench.generator import generate_demo
from headworkbench.versioning import VersionStore


@pytest.fixture(scope="session")
def generated_source(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("generated-source") / "workspace"
    generate_demo(root, force=True)
    VersionStore(root).ensure_layout()
    reset_versions(root)
    reset_stage_cache(root)
    return root


@pytest.fixture
def workspace(tmp_path: Path, generated_source: Path) -> Path:
    target = tmp_path / "workspace"
    shutil.copytree(generated_source, target)
    reset_versions(target)
    reset_stage_cache(target)
    return target

