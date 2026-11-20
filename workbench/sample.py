"""Deterministic sample CSV generator (fixed seed, no external data)."""

from __future__ import annotations

import os

import numpy as np

HEADER = ["age", "income", "score", "city", "segment", "channel", "device", "loyalty", "label"]

CITIES = ["shanghai", "beijing", "shenzhen", "hangzhou", "chengdu", "wuhan", "xian", "suzhou"]
SEGMENTS = ["A", "B", "C", "D"]
CHANNELS = ["organic", "ads", "referral", "email", "social", "direct"]
DEVICES = ["android", "ios", "web"]
LOYALTY_TIERS = ["t%02d" % i for i in range(12)]

DEFAULT_SEED = 20261120
DEFAULT_ROWS = 2000


def generate_csv(path: str, seed: int = DEFAULT_SEED, rows: int = DEFAULT_ROWS) -> dict:
    """Generate the deterministic demo sample and return its schema summary.

    Columns: 3 numeric (age/income/score, income has missing values),
    5 categorical (city/segment/channel/device/loyalty, city & channel have
    missing values) and one binary label correlated with some features.
    """
    rng = np.random.RandomState(seed)
    age = np.clip(rng.normal(40.0, 12.0, rows), 18.0, 80.0)
    income = rng.lognormal(10.5, 0.6, rows)
    score = rng.uniform(0.0, 100.0, rows)
    city = rng.choice(CITIES, rows)
    segment = rng.choice(SEGMENTS, rows, p=[0.40, 0.25, 0.20, 0.15])
    channel = rng.choice(CHANNELS, rows)
    device = rng.choice(DEVICES, rows)
    loyalty = rng.choice(LOYALTY_TIERS, rows)

    z = (
        -0.6
        + 0.025 * (age - 40.0)
        + 0.000004 * (income - 40000.0)
        + 0.004 * (score - 50.0)
        + np.where(segment == "B", 0.9, 0.0)
        + np.where(np.isin(city, ["shanghai", "beijing"]), 0.5, 0.0)
        + np.where(channel == "ads", -0.5, 0.0)
    )
    prob = 1.0 / (1.0 + np.exp(-z))
    label = (rng.uniform(0.0, 1.0, rows) < prob).astype(np.int64)

    income_missing = rng.uniform(0.0, 1.0, rows) < 0.05
    city_missing = rng.uniform(0.0, 1.0, rows) < 0.03
    channel_missing = rng.uniform(0.0, 1.0, rows) < 0.08

    lines = [",".join(HEADER)]
    for i in range(rows):
        lines.append(
            ",".join(
                [
                    "%.1f" % age[i],
                    "" if income_missing[i] else "%.2f" % income[i],
                    "%.3f" % score[i],
                    "" if city_missing[i] else str(city[i]),
                    str(segment[i]),
                    "" if channel_missing[i] else str(channel[i]),
                    str(device[i]),
                    str(loyalty[i]),
                    str(int(label[i])),
                ]
            )
        )
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(lines) + "\n")
    return {
        "path": path,
        "seed": seed,
        "rows": rows,
        "columns": list(HEADER),
        "numeric": ["age", "income", "score"],
        "categorical": ["city", "segment", "channel", "device", "loyalty"],
        "label": "label",
        "positive_rate": float(label.mean()),
    }
