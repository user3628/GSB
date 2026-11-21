import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


@pytest.fixture(scope="session", autouse=True)
def generated_data():
    """Ensure fixed-seed shards/configs exist (deterministic regeneration)."""
    subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_data.py"),
                    "--out", str(DATA)], check=True, capture_output=True)
    return DATA


@pytest.fixture()
def shards_dir():
    return str(DATA / "shards")


@pytest.fixture()
def config_v2():
    return str(DATA / "configs" / "config_v2.json")


@pytest.fixture()
def config_v1():
    return str(DATA / "configs" / "config_v1.json")


@pytest.fixture()
def workspace(tmp_path):
    return str(tmp_path / "ws")


@pytest.fixture()
def cfg_dict(config_v2):
    return json.loads(Path(config_v2).read_text(encoding="utf-8"))


@pytest.fixture()
def cfg_path(tmp_path, cfg_dict):
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg_dict), encoding="utf-8")
    return str(path)
