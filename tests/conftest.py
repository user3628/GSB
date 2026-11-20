from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from prefalign.core.config import Config
from prefalign.core.workspace import build_workspace, import_demo_shards
from scripts.generate_demo_data import main as generate_demo

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "proj"
    shutil.copytree(
        PROJECT_ROOT,
        root,
        ignore=shutil.ignore_patterns(
            ".venv", ".git", "__pycache__", ".pytest_cache", "workspace", "artifacts"),
    )
    for rel in ("workspace/state", "workspace/artifacts", "workspace/checkpoints",
                "workspace/locks", "data/shards"):
        (root / rel).mkdir(parents=True, exist_ok=True)
    generate_demo(root)
    return root


@pytest.fixture()
def pipeline(workspace: Path):
    cfg, store, pipe = build_workspace(workspace)
    import_demo_shards(store, workspace)
    return pipe


@pytest.fixture()
def built_pipeline(pipeline):
    pipeline.run(reason="test_initial")
    return pipeline
