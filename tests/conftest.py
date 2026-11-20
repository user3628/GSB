from __future__ import annotations

from pathlib import Path

import pytest

from quant_audit.demo import generate_demo_data
from quant_audit.workspace import Workspace


@pytest.fixture()
def demo_paths(tmp_path: Path) -> dict[str, Path]:
    data = tmp_path / "demo_data"
    generate_demo_data(data, force=True)
    workspace = Workspace.initialize(
        tmp_path / "workspace",
        data / "weights.npz",
        data / "calibration_clean.npy",
        data / "config.yaml",
        force=True,
    )
    return {"root": tmp_path, "data": data, "workspace": workspace}

