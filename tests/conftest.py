from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


@pytest.fixture
def sample_config() -> Path:
    return SAMPLES / "governance.yaml"


@pytest.fixture
def sample_shards() -> list[Path]:
    return sorted(SAMPLES.glob("shard_*.jsonl"))


def copy_sample_inputs(tmp_path: Path) -> tuple[Path, list[Path]]:
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    shards: list[Path] = []
    for source in sorted(SAMPLES.glob("shard_*.jsonl")):
        target = inputs / source.name
        shutil.copyfile(source, target)
        shards.append(target)
    config = tmp_path / "governance.yaml"
    shutil.copyfile(SAMPLES / "governance.yaml", config)
    return config, shards


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    )
    path.write_bytes(data.encode("utf-8"))

