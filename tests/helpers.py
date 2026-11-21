from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from ctr_cvr_workbench.config import DEFAULT_CONFIG, Config
from ctr_cvr_workbench.generator import write_dataset


def make_workspace(
    root: Path,
    *,
    seed: int = 20261122,
    shards: int = 2,
    rows_per_shard: int = 60,
    epochs: int = 2,
    budget: int = 10000,
    quota: int = 60,
    workers: int = 2,
    dimension: int = 64,
) -> tuple[Path, str]:
    write_dataset(root / "data" / "shards", seed=seed, shards=shards, rows_per_shard=rows_per_shard)
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["experiment"]["name"] = "test-workbench"
    config["paths"] = {
        "manifest": "data/shards/manifest.json",
        "checkpoints": "checkpoints",
        "outputs": "outputs",
        "state": "state",
    }
    config["features"]["dimension"] = dimension
    config["training"]["epochs"] = epochs
    config["scheduler"]["max_global_sample_rounds"] = budget
    config["scheduler"]["per_shard_sample_quota"] = quota
    config["scheduler"]["max_workers"] = workers
    config_path = root / "configs" / "test.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_bytes(yaml.safe_dump(config, sort_keys=True).encode("utf-8"))
    return root, "configs/test.yaml"


def load_raw_config(root: Path, path: str = "configs/test.yaml") -> dict[str, Any]:
    return yaml.safe_load((root / path).read_text(encoding="utf-8"))


def write_raw_config(root: Path, payload: dict[str, Any], path: str = "configs/test_changed.yaml") -> str:
    destination = root / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(yaml.safe_dump(payload, sort_keys=True).encode("utf-8"))
    return path


def run_result(root: Path, config_path: str, **kwargs: Any) -> dict[str, Any]:
    from ctr_cvr_workbench.pipeline import Pipeline

    return Pipeline(config_path, base_dir=root, **kwargs).run()


def config_for(root: Path, path: str) -> Config:
    return Config.load(path, root)
