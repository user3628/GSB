from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

from .models import canonical_json


SAMPLE_SEED = 20261120


def _write_array(path: Path, array: np.ndarray) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    array.astype("<f4").tofile(path)
    data = path.read_bytes()
    return {
        "path": path.as_posix(),
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def generate_samples(root: Path, rounds: int = 6, seed: int = SAMPLE_SEED) -> list[dict[str, Any]]:
    """Create deterministic float32 parameter/optimizer shards and cursor records."""

    root.mkdir(parents=True, exist_ok=True)
    indexes: list[dict[str, Any]] = []
    param_shapes = ((257,), (193,))
    optimizer_shapes = ((257,), (193,))
    for round_number in range(1, rounds + 1):
        rng = np.random.RandomState(seed + round_number * 1009)
        round_dir = root / f"round-{round_number:04d}"
        params: list[dict[str, Any]] = []
        optimizer: list[dict[str, Any]] = []
        for ordinal, shape in enumerate(param_shapes):
            array = rng.standard_normal(shape).astype(np.float32) * np.float32(0.03 + round_number * 0.001)
            path = round_dir / "params" / f"shard-{ordinal:03d}.bin"
            item = _write_array(path, array)
            item.update({"ordinal": ordinal, "name": path.name, "kind": "params"})
            item["path"] = path.relative_to(root).as_posix()
            params.append(item)
        for ordinal, shape in enumerate(optimizer_shapes):
            array = rng.standard_normal(shape).astype(np.float32) * np.float32(0.005 + round_number * 0.0001)
            path = round_dir / "optimizer" / f"shard-{ordinal:03d}.bin"
            item = _write_array(path, array)
            item.update({"ordinal": ordinal, "name": path.name, "kind": "optimizer"})
            item["path"] = path.relative_to(root).as_posix()
            optimizer.append(item)
        metadata = {
            "round": round_number,
            "scheduler_position": round_number * 37 + 5,
            "data_cursor": {
                "epoch": (round_number - 1) // 3,
                "batch": (round_number * 11) % 40,
                "sample_index": round_number * 128,
            },
            "params": params,
            "optimizer": optimizer,
            "seed": seed,
        }
        metadata_path = round_dir / "metadata.json"
        metadata_path.write_text(canonical_json(metadata) + "\n", encoding="utf-8")
        indexes.append(
            {
                "round": round_number,
                "directory": round_dir.as_posix(),
                "metadata_path": metadata_path.as_posix(),
                "scheduler_position": metadata["scheduler_position"],
                "data_cursor": metadata["data_cursor"],
            }
        )
    (root / "index.json").write_text(canonical_json(indexes) + "\n", encoding="utf-8")
    return indexes


def load_sample_payload(metadata_path: Path, request_id: str) -> dict[str, Any]:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    base_dir = metadata_path.parent

    def shard_payload(item: dict[str, Any]) -> dict[str, Any]:
        candidate = base_dir / item["kind"] / item["name"]
        data = candidate.read_bytes()
        return {
            "ordinal": item["ordinal"],
            "name": item["name"],
            "data_base64": base64.b64encode(data).decode("ascii"),
            "sha256": item["sha256"],
        }

    return {
        "request_id": request_id,
        "round": metadata["round"],
        "scheduler_position": metadata["scheduler_position"],
        "data_cursor": metadata["data_cursor"],
        "params": [shard_payload(item) for item in metadata["params"]],
        "optimizer": [shard_payload(item) for item in metadata["optimizer"]],
    }

