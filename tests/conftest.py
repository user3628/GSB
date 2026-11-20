from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pytest

from conv_workbench.samples import generate_demo_workspace


@pytest.fixture
def demo_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    generate_demo_workspace(root, force=True)
    return root


@pytest.fixture
def copied_demo_workspace(tmp_path: Path, demo_workspace: Path) -> Path:
    root = tmp_path / "copied_workspace"
    shutil.copytree(demo_workspace, root)
    return root

