"""Deterministic local sample generator.

Produces a CSV with numeric columns, categorical columns, a column with
missing values and a binary label column, plus a JSON pipeline config.
Everything is derived from a fixed seed so the artefacts are perfectly
rebuildable.  No external data, services or GPUs are involved.
"""
from __future__ import annotations

import csv
import json
import os

import numpy as np

DEFAULT_SEED = 20261123
DEFAULT_ROWS = 640
MISSING_TOKEN = ""  # empty cell encodes a missing value in the CSV


def generate_sample_csv(path: str, rows: int = DEFAULT_ROWS, seed: int = DEFAULT_SEED) -> str:
    """Write the deterministic sample CSV and return its absolute path."""
    rng = np.random.RandomState(seed)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    age = np.clip(rng.normal(41.0, 12.5, rows), 18.0, 90.0)
    income = np.clip(rng.lognormal(10.4, 0.55, rows), 8_000.0, 400_000.0)
    score = np.clip(rng.beta(2.2, 5.0, rows), 0.0, 1.0)
    balance = rng.normal(1_200.0, 640.0, rows)

    regions = np.array(["north", "south", "east", "west", "central"])
    segments = np.array(["basic", "plus", "premium", "enterprise"])
    channels = np.array(["web", "app", "branch", "partner", "phone", "mail"])
    tiers = np.array(["low", "mid", "high"])

    region = regions[rng.randint(0, len(regions), rows)]
    segment = segments[rng.randint(0, len(segments), rows)]
    channel = channels[rng.randint(0, len(channels), rows)]
    tier = tiers[rng.randint(0, len(tiers), rows)]

    # Column with missing values (~18% empty cells).
    loyalty = np.clip(rng.normal(0.55, 0.2, rows), 0.0, 1.0)
    missing_mask = rng.rand(rows) < 0.18

    logits = (
        0.045 * (age - 40.0)
        + 0.9 * (score - 0.3)
        + 0.6 * (segment == "premium")
        + 0.4 * (channel == "app")
        - 0.5 * (tier == "low")
        + rng.normal(0.0, 0.6, rows)
    )
    label = (logits > 0.35).astype(int)

    header = [
        "age", "income", "score", "balance",
        "region", "segment", "channel", "tier",
        "loyalty", "label",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for i in range(rows):
            writer.writerow([
                f"{age[i]:.6f}",
                f"{income[i]:.6f}",
                f"{score[i]:.6f}",
                f"{balance[i]:.6f}",
                region[i],
                segment[i],
                channel[i],
                tier[i],
                MISSING_TOKEN if missing_mask[i] else f"{loyalty[i]:.6f}",
                int(label[i]),
            ])
    return os.path.abspath(path)


def default_config() -> dict:
    """The deterministic JSON config driving the demo run."""
    return {
        "config_version": "demo-v1",
        "inference": {
            "numeric_parse_ratio": 0.9,
            "max_categorical_cardinality": 48,
            "missing_ratio_threshold": 0.9,
            "numeric_bins": 8,
            "label_column": "label",
        },
        "crossing": {
            "max_order": 2,
            "max_crosses": 8,
        },
        "reduction": {
            "candidates": [
                {"method": "pca", "target_dim": 6},
                {"method": "truncated_svd", "target_dim": 6},
                {"method": "random_projection", "target_dim": 6},
            ],
            "seed": 424242,
        },
        "cache": {"capacity": 16},
        "query": {"latency_budget_ms": 500},
        "quantization": {"scale": 1_000_000},
    }


def write_config(path: str, config: dict | None = None) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    cfg = config if config is not None else default_config()
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=2, sort_keys=True)
        fh.write("\n")
    return os.path.abspath(path)
