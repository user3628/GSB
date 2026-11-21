"""Fixed-seed sample generation for reproducible local demonstrations."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import numpy as np

from .digest import atomic_write_bytes, atomic_write_json


DEMO_SEED = 20261122
DEMO_SHARDS = (
    {
        "id": "short",
        "file": "short.npz",
        "shape": [1, 2, 7, 8],
        "seed_offset": 101,
        "purpose": "短序列基线",
    },
    {
        "id": "long",
        "file": "long.npz",
        "shape": [2, 3, 96, 16],
        "seed_offset": 202,
        "purpose": "长序列且批量大于 1",
    },
    {
        "id": "odd",
        "file": "odd.npz",
        "shape": [2, 5, 11, 9],
        "seed_offset": 303,
        "purpose": "奇数头维度 [2,5,11,9]，最后一维不旋转",
    },
)


def generate_demo_assets(base_dir: str | Path) -> dict[str, Any]:
    base = Path(base_dir).resolve()
    data_dir = base / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    shard_entries: list[dict[str, Any]] = []
    for item in DEMO_SHARDS:
        shape = tuple(int(value) for value in item["shape"])
        seed = DEMO_SEED + int(item["seed_offset"])
        rng = np.random.default_rng(seed)
        query = rng.standard_normal(shape, dtype=np.float32) * np.float32(0.35)
        key = rng.standard_normal(shape, dtype=np.float32) * np.float32(0.35)
        buffer = io.BytesIO()
        np.savez(buffer, q=query, k=key)
        atomic_write_bytes(data_dir / str(item["file"]), buffer.getvalue())
        shard_entries.append(
            {
                "id": item["id"],
                "file": str(item["file"]),
                "shape": list(shape),
                "seed": seed,
                "purpose": item["purpose"],
            }
        )

    manifest = {
        "schema_version": 1,
        "generator": "rope_diag.demo_data",
        "seed": DEMO_SEED,
        "shards": shard_entries,
    }
    atomic_write_json(data_dir / "manifest.json", manifest)
    return {"data_dir": str(data_dir), "manifest": str(data_dir / "manifest.json"), "shards": shard_entries}
