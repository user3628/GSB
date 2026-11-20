"""Shared fixtures: deterministic sample bundle in temporary workspaces."""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


@pytest.fixture(scope="session", autouse=True)
def ensure_samples() -> Path:
    if not (SAMPLES / "manifest.json").exists():
        subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_samples.py")],
                       check=True)
    return SAMPLES


@pytest.fixture()
def tmp_workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "ws"
    yield ws
    if ws.exists():
        shutil.rmtree(ws)


@pytest.fixture()
def service(tmp_workspace: Path):
    import depworkbench.workspace as workspace_mod
    ws = workspace_mod.Workspace.create(tmp_workspace)
    return workspace_mod.WorkbenchService(ws)


@pytest.fixture()
def imported_service(service, ensure_samples: Path):
    service.import_bundle(ensure_samples, ensure_samples / "manifest.json",
                          ensure_samples / "config.yaml")
    return service


@pytest.fixture()
def config(ensure_samples: Path):
    from depworkbench.config import load_config
    return load_config(ensure_samples / "config.yaml")
