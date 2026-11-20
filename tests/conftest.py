from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from storypipe.generator import generate_samples
from storypipe.pipeline import PipelineRunner
from storypipe.workspace import import_sample


@pytest.fixture(scope="session")
def sample_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("samples")
    generate_samples(root)
    return root


@pytest.fixture(scope="session")
def small_run(sample_root: Path, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    workspace = tmp_path_factory.mktemp("small-run")
    import_sample(sample_root / "small", workspace)
    result = PipelineRunner(workspace).run(fresh=True)
    return {"workspace": workspace, "result": result}


@pytest.fixture(scope="session")
def large_run(sample_root: Path, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    workspace = tmp_path_factory.mktemp("large-run")
    import_sample(sample_root / "large", workspace)
    result = PipelineRunner(workspace).run(fresh=True)
    return {"workspace": workspace, "result": result}


def output_file_digests(workspace: Path) -> dict[str, str]:
    output_dir = workspace / "stages" / "04_encode" / "output"
    digests = {}
    for path in sorted(output_dir.rglob("*")):
        if path.is_file():
            digests[path.relative_to(output_dir).as_posix()] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    return digests
