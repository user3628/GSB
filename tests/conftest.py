from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from density_trace.demo import generate_demo_data
from density_trace.engine import Engine, import_inputs


@pytest.fixture(scope="session")
def demo_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("density-trace-demo") / "data"
    generate_demo_data(path, overwrite=True)
    return path


@pytest.fixture
def data_copy(tmp_path: Path, demo_data: Path) -> Path:
    target = tmp_path / "data"
    shutil.copytree(demo_data, target)
    return target


def make_engine(workspace: Path, data: Path) -> Engine:
    import_inputs(
        workspace,
        data / "manifest.json",
        data / "config.json",
        data / "reference_quantiles.json",
    )
    return Engine.from_workspace(workspace)
