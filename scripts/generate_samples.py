from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.policy import normalize_snapshot  # noqa: E402
from app.utils import canonical_json, snapshot_checksum  # noqa: E402


SEED = 20261123
LAYERS = [
    {"id": "hot-nvme", "capacity_bytes": 4 * 1024**3},
    {"id": "warm-ssd", "capacity_bytes": 8 * 1024**3},
    {"id": "cold-hdd", "capacity_bytes": 16 * 1024**3},
]


def main() -> int:
    rng = np.random.default_rng(SEED)
    heats = [int(value) for value in rng.integers(0, 256, size=12)]
    sizes_mib = [int(value) for value in rng.integers(256, 769, size=12)]

    snapshot = {
        "version": 1,
        "generated_at": "2026-01-01T00:00:00Z",
        "layers": LAYERS,
        "heat_threshold": 128,
        "batch_limit": 16,
        "cooldown_seconds": 3600,
    }
    snapshot["checksum"] = snapshot_checksum(snapshot)
    snapshot = normalize_snapshot(snapshot)

    shards = []
    for index, (heat, size_mib) in enumerate(zip(heats, sizes_mib, strict=True)):
        layer_index = index // 4
        shards.append(
            {
                "shard_id": f"shard-{index + 1:03d}",
                "size_bytes": size_mib * 1024**2,
                "heat": heat,
                "layer_id": LAYERS[layer_index]["id"],
                "cooldown_until": "2000-01-01T00:00:00Z",
            }
        )

    distribution = {
        "seed": SEED,
        "generator": "numpy.random.default_rng(20261123).integers",
        "sample_generated_at": snapshot["generated_at"],
        "threshold": snapshot["heat_threshold"],
        "total_shards": len(shards),
        "heat_histogram_16": {
            f"{index * 16:03d}-{index * 16 + 15:03d}": Counter(
                heat // 16 for heat in heats
            ).get(index, 0)
            for index in range(16)
        },
        "layer_heat_counts": {
            layer["id"]: {
                "hot": sum(
                    1
                    for shard in shards
                    if shard["layer_id"] == layer["id"] and shard["heat"] >= snapshot["heat_threshold"]
                ),
                "cold": sum(
                    1
                    for shard in shards
                    if shard["layer_id"] == layer["id"] and shard["heat"] < snapshot["heat_threshold"]
                ),
            }
            for layer in LAYERS
        },
    }

    samples = ROOT / "samples"
    samples.mkdir(parents=True, exist_ok=True)
    (samples / "policy_snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (samples / "shards.json").write_text(
        json.dumps(
            {
                "seed": SEED,
                "description": "Deterministic local block shards generated without external data",
                "shards": shards,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (samples / "heat_distribution.json").write_text(
        json.dumps(distribution, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(canonical_json({"snapshot_checksum": snapshot["checksum"], "shards": len(shards)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
