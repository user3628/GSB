"""Deterministic local sample shard generation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np


@dataclass(frozen=True)
class SampleSpec:
    shard_id: str
    batch: int
    heads: int
    sequence_length: int
    head_dim: int
    tags: tuple[str, ...]
    seed_offset: int


SAMPLE_SPECS: tuple[SampleSpec, ...] = (
    SampleSpec(
        shard_id="short_causal",
        batch=2,
        heads=2,
        sequence_length=128,
        head_dim=32,
        tags=("short_sequence", "batch_gt_1", "causal"),
        seed_offset=101,
    ),
    SampleSpec(
        shard_id="long_causal",
        batch=1,
        heads=2,
        sequence_length=1024,
        head_dim=64,
        tags=("long_sequence", "causal"),
        seed_offset=202,
    ),
    SampleSpec(
        shard_id="odd_dim_causal",
        batch=2,
        heads=1,
        sequence_length=257,
        head_dim=40,
        tags=("non_divisible_head_dim", "odd_sequence", "batch_gt_1", "causal"),
        seed_offset=303,
    ),
)


CONFIG_YAML = """# Deterministic operator configuration for local execution.
operator:
  name: flashblock
  row_block_size: 64
  col_block_size: 64
  causal: true
  online_strategy: stable_running_max
  score_clamp: 80.0
  extreme_threshold: 60.0
  atol: 1.0e-10
  rtol: 1.0e-9
  memory_budget_bytes: 67108864
  workspace_factor: 1.25
  verify_incremental: true
"""


def _array_digest(array: np.ndarray) -> str:
    contiguous = np.ascontiguousarray(array)
    hasher = hashlib.sha256()
    hasher.update(contiguous.dtype.str.encode("ascii"))
    hasher.update(json.dumps(list(contiguous.shape), separators=(",", ":")).encode("ascii"))
    hasher.update(contiguous.tobytes(order="C"))
    return hasher.hexdigest()


def generate_samples(project_root: str | Path, seed: int = 20260301) -> dict[str, Any]:
    root = Path(project_root).resolve()
    data_dir = root / "data"
    shard_dir = data_dir / "shards"
    config_dir = root / "configs"
    shard_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "operator.yaml"
    config_path.write_text(CONFIG_YAML, encoding="utf-8")

    shards: list[dict[str, Any]] = []
    for spec in SAMPLE_SPECS:
        rng = np.random.default_rng(seed + spec.seed_offset)
        shape = (spec.batch, spec.heads, spec.sequence_length, spec.head_dim)
        q = np.ascontiguousarray(rng.normal(0.0, 0.6, size=shape), dtype=np.float64)
        k = np.ascontiguousarray(rng.normal(0.0, 0.6, size=shape), dtype=np.float64)
        v = np.ascontiguousarray(rng.normal(0.0, 0.6, size=shape), dtype=np.float64)
        filename = f"{spec.shard_id}.npz"
        path = shard_dir / filename
        np.savez_compressed(path, q=q, k=k, v=v)
        shards.append(
            {
                "shard_id": spec.shard_id,
                "path": str(path.relative_to(root)).replace("\\", "/"),
                "shape": list(shape),
                "sequence_length": spec.sequence_length,
                "head_dim": spec.head_dim,
                "batch": spec.batch,
                "heads": spec.heads,
                "tags": list(spec.tags),
                "sha256": {
                    "q": _array_digest(q),
                    "k": _array_digest(k),
                    "v": _array_digest(v),
                },
            }
        )

    manifest = {
        "format": "flashblock/shapes-v1",
        "seed": seed,
        "dtype": "float64",
        "operator_config": "configs/operator.yaml",
        "shards": shards,
    }
    manifest_path = data_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest

