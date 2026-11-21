from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from .canonical import atomic_write_json, digest_file, digest_object, read_json, write_json_file
from .errors import WorkbenchError


DEFAULT_SEED = 20261122
DEFAULT_SHARDS = 3
DEFAULT_HEADS_PER_SHARD = 2
DEFAULT_SAMPLES = 64
DEFAULT_SEQ_LEN = 16
DEFAULT_HEAD_DIM = 8
HEAD_ACTIVATION_SCALES = (0.55, 0.85, 1.20, 0.95, 1.50, 0.68)


def demo_config_dict(seed: int = DEFAULT_SEED) -> dict[str, Any]:
    return {
        "schema": "headworkbench.config/v1",
        "version": "demo-2026-11-22-v1",
        "seed": seed,
        "candidate_bitwidths": [6, 8, 10, 12, 14],
        "sampling_windows": {"compact": 12, "full": DEFAULT_SAMPLES},
        "sample_cap_per_head": DEFAULT_SAMPLES,
        "min_samples_per_head": 8,
        "sensitivity_threshold": 0.90,
        "global_error_budget": 0.030,
        "per_head_error_tolerance": 0.040,
        "activation_clip_abs": 8.0,
        "latency_budget_ms": 250.0,
    }


def write_demo_config(path: Path, *, seed: int = DEFAULT_SEED) -> dict[str, Any]:
    payload = demo_config_dict(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    path.write_text(text, encoding="utf-8")
    return payload


def _projection(rng: np.random.Generator, dim: int, *, scale: float) -> np.ndarray:
    matrix = rng.normal(0.0, scale / np.sqrt(dim), size=(dim, dim))
    diagonal = float(np.trace(matrix @ matrix.T))
    return (matrix / np.sqrt(max(diagonal, 1e-12)) * dim).astype(np.float64)


def generate_demo(
    root: Path,
    *,
    seed: int = DEFAULT_SEED,
    shards: int = DEFAULT_SHARDS,
    heads_per_shard: int = DEFAULT_HEADS_PER_SHARD,
    samples: int = DEFAULT_SAMPLES,
    seq_len: int = DEFAULT_SEQ_LEN,
    head_dim: int = DEFAULT_HEAD_DIM,
    force: bool = False,
) -> dict[str, Any]:
    root = root.resolve()
    data_dir = root / "data"
    config_path = root / "config" / "adjudication.yaml"
    if (data_dir / "manifest.json").exists() and not force:
        raise WorkbenchError(
            "E_DATA_EXISTS",
            "demo data already exists; pass force=True to replace it",
            details={"manifest": "data/manifest.json"},
        )

    staging = data_dir / "shards"
    if force and staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)
    (data_dir / "imports").mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    manifest_shards: list[dict[str, Any]] = []
    manifest_heads: list[dict[str, Any]] = []
    total_heads = shards * heads_per_shard
    if len(HEAD_ACTIVATION_SCALES) < total_heads:
        scales = [0.7 + 0.15 * i for i in range(total_heads)]
    else:
        scales = list(HEAD_ACTIVATION_SCALES[:total_heads])

    for shard_index in range(shards):
        local_heads = heads_per_shard
        head_ids = np.arange(shard_index * heads_per_shard, (shard_index + 1) * heads_per_shard, dtype=np.int64)
        layer_names = np.asarray([f"layer{shard_index}"] * local_heads, dtype="<U16")
        head_indices = np.arange(local_heads, dtype=np.int64)
        sample_ids = np.arange(samples, dtype=np.int64)

        activation = np.empty((samples, local_heads, seq_len, head_dim), dtype=np.float64)
        query_proj = np.empty((local_heads, head_dim, head_dim), dtype=np.float64)
        key_proj = np.empty((local_heads, head_dim, head_dim), dtype=np.float64)
        for local_index, global_head in enumerate(head_ids.tolist()):
            scale = scales[global_head]
            base = rng.normal(0.0, scale, size=(samples, seq_len, head_dim))
            positional = np.linspace(-0.22, 0.22, seq_len, dtype=np.float64)[None, :, None]
            harmonic = 0.10 * np.sin(np.arange(head_dim, dtype=np.float64) * (0.7 + 0.05 * local_index))
            activation[:, local_index, :, :] = base + positional + harmonic[None, None, :]
            common = rng.normal(0.0, 1.0, size=(head_dim, head_dim))
            query_proj[local_index] = _projection(rng, head_dim, scale=1.0)
            key_proj[local_index] = (0.72 * _projection(rng, head_dim, scale=1.0) + 0.28 * common).astype(np.float64)

        shard_name = f"shard_{shard_index:02d}.npz"
        shard_path = staging / shard_name
        np.savez_compressed(
            shard_path,
            activations=activation,
            query_proj=query_proj,
            key_proj=key_proj,
            head_ids=head_ids,
            layer_names=layer_names,
            head_indices=head_indices,
            sample_ids=sample_ids,
        )
        shard_digest = digest_file(shard_path)
        relative_shard = f"shards/{shard_name}"
        manifest_shards.append(
            {
                "shard_file": relative_shard,
                "sha256": shard_digest,
                "activation_shape": list(activation.shape),
                "query_proj_shape": list(query_proj.shape),
                "key_proj_shape": list(key_proj.shape),
                "head_ids": head_ids.tolist(),
            }
        )
        for local_index, global_head in enumerate(head_ids.tolist()):
            manifest_heads.append(
                {
                    "head_id": global_head,
                    "layer_name": str(layer_names[local_index]),
                    "head_index": int(head_indices[local_index]),
                    "shard_file": relative_shard,
                    "local_index": local_index,
                    "sample_count": samples,
                    "seq_len": seq_len,
                    "head_dim": head_dim,
                }
            )

    manifest_payload: dict[str, Any] = {
        "schema": "headworkbench.manifest/v1",
        "seed": seed,
        "sample_count_per_shard": samples,
        "seq_len": seq_len,
        "head_dim": head_dim,
        "shards": manifest_shards,
        "heads": manifest_heads,
    }
    manifest_payload["manifest_digest"] = digest_object(manifest_payload)
    manifest_path = data_dir / "manifest.json"
    atomic_write_json(manifest_path, manifest_payload)

    config_payload = write_demo_config(config_path, seed=seed)
    config_digest = digest_file(config_path)
    write_json_file(
        data_dir / "current.json",
        {
            "schema": "headworkbench.data-pointer/v1",
            "manifest_path": "data/manifest.json",
            "manifest_digest": manifest_payload["manifest_digest"],
        },
    )
    write_json_file(
        root / "config" / "current.json",
        {
            "schema": "headworkbench.config-pointer/v1",
            "config_path": "config/adjudication.yaml",
            "config_digest": config_digest,
            "config_version": config_payload["version"],
        },
    )

    summary = {
        "schema": "headworkbench.generation/v1",
        "root": str(root),
        "seed": seed,
        "shard_count": shards,
        "head_count": total_heads,
        "samples_per_shard": samples,
        "seq_len": seq_len,
        "head_dim": head_dim,
        "manifest_digest": manifest_payload["manifest_digest"],
        "config_digest": config_digest,
        "config_version": config_payload["version"],
        "sampling_windows": config_payload["sampling_windows"],
        "head_ids": [head["head_id"] for head in manifest_heads],
        "shards": [
            {"file": shard["shard_file"], "sha256": shard["sha256"], "activation_shape": shard["activation_shape"]}
            for shard in manifest_shards
        ],
    }
    atomic_write_json(root / "artifacts" / "generation.json", summary)
    return summary


def read_generation_summary(root: Path) -> dict[str, Any] | None:
    path = root / "artifacts" / "generation.json"
    return read_json(path) if path.exists() else None

