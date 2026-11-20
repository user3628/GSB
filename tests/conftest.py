from __future__ import annotations

from pathlib import Path

import pytest

from panopipe.demo import generate_demo
from panopipe.manifest import ParseOptions
from panopipe.workspace import Workspace


@pytest.fixture(scope="session")
def demo_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("demo")
    generate_demo(root, seed=20261120)
    return root


@pytest.fixture
def imported_workspace(tmp_path: Path, demo_dir: Path) -> Workspace:
    workspace = Workspace.create(tmp_path / "workspace")
    workspace.import_manifest(
        demo_dir / "manifest_v2.json",
        parse_options=ParseOptions(
            default_instance_start_id=100,
            min_instance_area=4,
            confidence_threshold=0.6,
            mask_format="png_palette",
        ),
    )
    return workspace


@pytest.fixture
def built_workspace(imported_workspace: Workspace):
    from panopipe.pipeline import Pipeline

    result = Pipeline(imported_workspace).build()
    assert result["ok"], result
    return imported_workspace, result
