from __future__ import annotations

from pathlib import Path

import pytest

from qpipe.pipeline import Pipeline
from qpipe.samples import generate_samples


@pytest.fixture
def samples_dir(tmp_path: Path) -> Path:
    path = tmp_path / "samples"
    generate_samples(path, clean=True)
    return path


@pytest.fixture
def pipeline(tmp_path: Path, samples_dir: Path) -> Pipeline:
    pipeline = Pipeline(tmp_path, state_dir=".qpipe-test")
    pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest.json",
        shapes_path=samples_dir / "shape_declarations.json",
        version="v1",
    )
    return pipeline
