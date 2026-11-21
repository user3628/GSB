from __future__ import annotations

from pathlib import Path

from kde_drift.config import normalize_config
from kde_drift.generator import generate_dataset


def generate_small_data(root: Path) -> Path:
    generate_dataset(root, scales=("small",))
    return root


def fast_config(**overrides):
    config = {
        "grid": {"points": 48},
        "chunk_size": 128,
        "mmd": {
            "chunk_size": 128,
            "max_eval_samples": 512,
            "bootstrap_sample_size": 96,
        },
        "drift": {"bootstrap_resamples": 24},
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(config.get(key), dict):
            config[key].update(value)
        else:
            config[key] = value
    return normalize_config(config)
