"""Generate deterministic demo shards (.npz) and the operator manifest.

Fixed seed 20261121 produces byte-reproducible tensors.  The dataset covers
8-bit and 4-bit, symmetric and asymmetric, per-tensor and per-channel
quantization, outliers, all-zero channels, NaN and Inf anomaly shards.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import numpy as np
import yaml

from qoplib.io_util import save_npz_deterministic

SEED = 20261121


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def _layer(name: str, weight: str, in_features: int, out_features: int,
           bitwidth: int, mode: str, granularity: str,
           act_bitwidth: int | None = None,
           act_mode: str | None = None,
           act_granularity: str | None = None) -> Dict[str, Any]:
    rel_tol = 0.35 if bitwidth == 4 else 0.25
    return {
        "name": name,
        "weight": weight,
        "in_features": in_features,
        "out_features": out_features,
        "bitwidth": bitwidth,
        "mode": mode,
        "granularity": granularity,
        "activation_bitwidth": act_bitwidth or bitwidth,
        "activation_mode": act_mode or mode,
        "activation_granularity": act_granularity or granularity,
        "rel_tol": rel_tol,
        "abs_tol": 0.6,
        "sample_positions": 24,
    }


def _save_shard(data_dir: Path, name: str,
                arrays: Dict[str, np.ndarray]) -> str:
    path = data_dir / f"{name}.npz"
    save_npz_deterministic(path, arrays)
    return path.name


def generate_demo_dataset(data_dir: str | Path, seed: int = SEED) -> Dict[str, Any]:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    # ---- shard 00: 8-bit symmetric, per tensor, with outliers ---------
    rng = _rng(seed + 1)
    batch0, in0 = 12, 16
    x0 = rng.normal(0.0, 0.8, size=(batch0, in0))
    x0[0, 0] = 4.5  # activation outlier
    x0[3, 5] = -4.0
    w00 = rng.normal(0.0, 0.6, size=(12, 16))
    b00 = rng.normal(0.0, 0.1, size=12)
    w01 = rng.normal(0.0, 0.5, size=(8, 12))
    b01 = rng.normal(0.0, 0.1, size=8)
    f00 = _save_shard(data_dir, "shard_00", {
        "activation": x0, "w_fc1": w00, "w_fc1_bias": b00,
        "w_fc2": w01, "w_fc2_bias": b01,
    })

    # ---- shard 01: 4-bit asymmetric per-channel weights --------------
    rng = _rng(seed + 2)
    batch1, in1 = 10, 16
    x1 = rng.uniform(-2.0, 4.0, size=(batch1, in1))
    w10 = rng.normal(0.0, 0.9, size=(12, 16))
    w10[2, :] *= 3.5  # high-dynamic-range channel
    b10 = rng.normal(0.0, 0.1, size=12)
    w11 = rng.normal(0.0, 0.7, size=(8, 12))
    b11 = rng.normal(0.0, 0.1, size=8)
    f01 = _save_shard(data_dir, "shard_01", {
        "activation": x1, "w_fc1": w10, "w_fc1_bias": b10,
        "w_fc2": w11, "w_fc2_bias": b11,
    })

    # ---- shard 02: 8-bit asymmetric per-channel, all-zero channel -----
    rng = _rng(seed + 3)
    batch2, in2 = 9, 16
    x2 = rng.normal(0.5, 0.7, size=(batch2, in2))
    w20 = rng.normal(0.0, 0.6, size=(12, 16))
    w20[7, :] = 0.0  # fully zero output channel
    b20 = rng.normal(0.0, 0.1, size=12)
    w21 = rng.normal(0.0, 0.5, size=(8, 12))
    b21 = rng.normal(0.0, 0.1, size=8)
    f02 = _save_shard(data_dir, "shard_02", {
        "activation": x2, "w_fc1": w20, "w_fc1_bias": b20,
        "w_fc2": w21, "w_fc2_bias": b21,
    })

    # ---- shard 03: 4-bit symmetric, NaN + Inf anomaly -----------------
    rng = _rng(seed + 4)
    batch3, in3 = 8, 16
    x3 = rng.normal(0.0, 0.8, size=(batch3, in3))
    x3[0, 0] = np.nan
    x3[1, 2] = np.inf
    x3[2, 3] = -np.inf
    w30 = rng.normal(0.0, 0.6, size=(12, 16))
    b30 = rng.normal(0.0, 0.1, size=12)
    w31 = rng.normal(0.0, 0.5, size=(8, 12))
    b31 = rng.normal(0.0, 0.1, size=8)
    f03 = _save_shard(data_dir, "shard_03", {
        "activation": x3, "w_fc1": w30, "w_fc1_bias": b30,
        "w_fc2": w31, "w_fc2_bias": b31,
    })

    shards = [
        {
            "shard_id": "shard_00", "index": 0, "npz_file": f00,
            "batch": batch0, "input_features": in0,
            "expected_output_shape": [batch0, 8],
            "expected_rel_error_bound": 0.25,
            "layers": [
                _layer("fc1", "w_fc1", 16, 12, 8, "symmetric", "tensor"),
                _layer("fc2", "w_fc2", 12, 8, 8, "symmetric", "tensor"),
            ],
        },
        {
            "shard_id": "shard_01", "index": 1, "npz_file": f01,
            "batch": batch1, "input_features": in1,
            "expected_output_shape": [batch1, 8],
            "expected_rel_error_bound": 0.35,
            "layers": [
                _layer("fc1", "w_fc1", 16, 12, 4, "asymmetric", "channel"),
                _layer("fc2", "w_fc2", 12, 8, 4, "asymmetric", "channel"),
            ],
        },
        {
            "shard_id": "shard_02", "index": 2, "npz_file": f02,
            "batch": batch2, "input_features": in2,
            "expected_output_shape": [batch2, 8],
            "expected_rel_error_bound": 0.25,
            "layers": [
                _layer("fc1", "w_fc1", 16, 12, 8, "asymmetric", "channel"),
                _layer("fc2", "w_fc2", 12, 8, 8, "asymmetric", "channel"),
            ],
        },
        {
            "shard_id": "shard_03", "index": 3, "npz_file": f03,
            "batch": batch3, "input_features": in3,
            "expected_output_shape": [batch3, 8],
            "expected_rel_error_bound": 0.35,
            "layers": [
                _layer("fc1", "w_fc1", 16, 12, 4, "symmetric", "tensor"),
                _layer("fc2", "w_fc2", 12, 8, 4, "symmetric", "tensor"),
            ],
        },
    ]

    manifest = {
        "config_version": 1,
        "budget": {
            "total_element_budget": 20000,
            "worker_element_quota": 8000,
            "peak_memory_bytes": 4_000_000,
        },
        "default_tolerance": {"rel_tol": 0.35, "abs_tol": 0.6},
        "shards": shards,
    }
    manifest_path = data_dir / "manifest.yaml"
    manifest_path.write_text(
        yaml.safe_dump(manifest, sort_keys=True, default_flow_style=False),
        encoding="utf-8",
    )
    tight = dict(manifest)
    tight["budget"] = {
        "total_element_budget": 6000,
        "worker_element_quota": 4000,
        "peak_memory_bytes": 4_000_000,
    }
    tight_path = data_dir / "manifest_tight.yaml"
    tight_path.write_text(
        yaml.safe_dump(tight, sort_keys=True, default_flow_style=False),
        encoding="utf-8",
    )
    tiny_memory = dict(manifest)
    tiny_memory["budget"] = {
        "total_element_budget": 20000,
        "worker_element_quota": 8000,
        "peak_memory_bytes": 1024,
    }
    tiny_path = data_dir / "manifest_tiny_memory.yaml"
    tiny_path.write_text(
        yaml.safe_dump(tiny_memory, sort_keys=True,
                       default_flow_style=False),
        encoding="utf-8",
    )
    return {"data_dir": str(data_dir), "manifest": str(manifest_path),
            "tight_manifest": str(tight_path),
            "tiny_memory_manifest": str(tiny_path),
            "shards": [item["shard_id"] for item in shards]}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    result = generate_demo_dataset(args.data_dir, args.seed)
    print(yaml.safe_dump(result, sort_keys=True))
