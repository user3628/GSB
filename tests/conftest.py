from __future__ import annotations

from pathlib import Path

import pytest

from cabin_voice.samplegen import generate_demo
from cabin_voice.storage import Workspace


@pytest.fixture(scope="session")
def generated_demo(tmp_path_factory: pytest.TempPathFactory) -> dict:
    root = tmp_path_factory.mktemp("generated_demo")
    return generate_demo(root, seed=20261123)


@pytest.fixture()
def workspace(tmp_path: Path, generated_demo: dict) -> Workspace:
    ws = Workspace(tmp_path / "workspace").ensure()
    ws.import_bundle(generated_demo["manifest"])
    return ws


@pytest.fixture()
def full_workspace(workspace: Workspace) -> Workspace:
    from cabin_voice.engine import Engine

    Engine(workspace).build(mode="full")
    return workspace
