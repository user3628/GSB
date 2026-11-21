from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tonelab.config import Config
from tonelab.engine import BuildEngine
from tonelab.workspace import Workspace


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def demo_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("tonelab-data")
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "generate_demo.py"), "--data-dir", str(root)],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return root


def build_workspace(root: Path, data_dir: Path, *, config: Config | None = None, mode: str = "full") -> tuple[Workspace, BuildEngine, dict]:
    workspace = Workspace(root, config=config or Config())
    workspace.import_manifests([data_dir / "manifest_v2.json", data_dir / "manifest_v1.json"], project_id="test-demo")
    engine = BuildEngine(workspace)
    report = engine.build(mode)
    return workspace, engine, report


@pytest.fixture(scope="session")
def baseline(tmp_path_factory: pytest.TempPathFactory, demo_data: Path) -> tuple[Workspace, BuildEngine, dict]:
    root = tmp_path_factory.mktemp("tonelab-baseline") / "workspace"
    return build_workspace(root, demo_data, mode="full")


@pytest.fixture
def isolated_workspace(tmp_path: Path, demo_data: Path) -> tuple[Workspace, BuildEngine, dict]:
    return build_workspace(tmp_path / "workspace", demo_data, mode="full")


@pytest.fixture
def data_index(demo_data: Path) -> dict:
    return json.loads((demo_data / "index.json").read_text(encoding="utf-8"))

