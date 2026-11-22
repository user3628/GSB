from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .policy import normalize_snapshot


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def load_samples(samples_dir: str | Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    root = Path(samples_dir)
    snapshot = normalize_snapshot(load_json(root / "policy_snapshot.json"))
    shard_document = load_json(root / "shards.json")
    shards = shard_document.get("shards")
    if not isinstance(shards, list):
        raise ValueError("shards.json must contain a shards array")

    layer_ids = {layer["id"] for layer in snapshot["layers"]}
    seen: set[str] = set()
    normalized_shards: list[dict[str, Any]] = []
    for index, shard in enumerate(shards):
        if not isinstance(shard, dict):
            raise ValueError(f"shards[{index}] must be an object")
        shard_id = shard.get("shard_id")
        size_bytes = shard.get("size_bytes")
        heat = shard.get("heat")
        layer_id = shard.get("layer_id")
        cooldown_until = shard.get("cooldown_until")
        if not isinstance(shard_id, str) or not shard_id or shard_id in seen:
            raise ValueError(f"shards[{index}] has an invalid or duplicate shard_id")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes <= 0:
            raise ValueError(f"shards[{index}].size_bytes must be a positive integer")
        if isinstance(heat, bool) or not isinstance(heat, int) or not 0 <= heat <= 255:
            raise ValueError(f"shards[{index}].heat must be between 0 and 255")
        if layer_id not in layer_ids:
            raise ValueError(f"shards[{index}] references unknown layer {layer_id!r}")
        if not isinstance(cooldown_until, str):
            raise ValueError(f"shards[{index}].cooldown_until must be a string")
        seen.add(shard_id)
        normalized_shards.append(
            {
                "shard_id": shard_id,
                "size_bytes": size_bytes,
                "heat": heat,
                "layer_id": layer_id,
                "cooldown_until": cooldown_until,
            }
        )
    return snapshot, normalized_shards
