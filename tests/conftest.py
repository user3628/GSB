from __future__ import annotations

from pathlib import Path

import pytest

from audioq.demo import generate_demo_assets


@pytest.fixture(scope="session")
def demo_workspace(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    root = tmp_path_factory.mktemp("audioq-workspace")
    assets = generate_demo_assets("demo", root=root)
    return {
        "root": root,
        "assets": assets,
        "manifest": Path("demo/manifest.json"),
        "config": Path("demo/configs/config_v2.json"),
        "legacy_config": Path("demo/configs/config_v1_legacy.json"),
    }


@pytest.fixture()
def workspace(tmp_path: Path, demo_workspace: dict[str, object]) -> dict[str, object]:
    return {**demo_workspace, "scratch": tmp_path}
