from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_demo import generate
from xrel_workbench.pipeline import run_pipeline
from xrel_workbench.workspace import initialize_workspace


@pytest.fixture(scope="session")
def generated_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("generated-data")
    generate(root, 20261121, force=True)
    return root


@pytest.fixture()
def workspace(tmp_path: Path, generated_data: Path):
    paths = initialize_workspace(generated_data, tmp_path / "workspace", force=True)
    result = run_pipeline(paths, full=True, verify_incremental=False)
    assert result["ok"] and not result["stopped"]
    return paths
