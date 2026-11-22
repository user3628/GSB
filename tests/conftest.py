from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from tracklab.config import load_settings
from tracklab.demo import run_demo
from tracklab.generator import generate_demo


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_settings(root: Path, *, frames: int = 26) -> object:
    config = yaml.safe_load((PROJECT_ROOT / "config" / "workbench.yaml").read_text(encoding="utf-8"))
    config = copy.deepcopy(config)
    config["generator"]["frames_per_sequence"] = frames
    config["generator"]["sample_count_upper_bound"] = 64
    config["server"]["port"] = 0
    config_path = root / "config" / "workbench.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config, sort_keys=True), encoding="utf-8")
    return load_settings(root, config_path)


@pytest.fixture
def workspace(tmp_path: Path):
    settings = make_settings(tmp_path)
    generate_demo(settings, force=True)
    return settings


@pytest.fixture(scope="session")
def demo_workspace(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("demo-workspace")
    settings = make_settings(root)
    result = run_demo(settings)
    return settings, result
