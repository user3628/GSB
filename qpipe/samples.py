from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from .util import atomic_write_json, atomic_write_npz, canonical_json_bytes

DEFAULT_SEED = 20261122


def _layer(
    layer_id: str,
    weight_shard: str,
    activation_shard: str,
    *,
    weight_bits: int,
    weight_mode: str,
    weight_granularity: str,
    activation_bits: int,
    activation_mode: str,
    activation_granularity: str,
    accumulator_bits: int,
    max_abs: float,
    max_rel: float,
    weight_axis: int = 0,
    activation_axis: int = 0,
) -> dict[str, Any]:
    return {
        "id": layer_id,
        "op": "linear",
        "weight": {
            "shard": weight_shard,
            "key": "weight",
            "quantization": {
                "bits": weight_bits,
                "mode": weight_mode,
                "granularity": weight_granularity,
                "channel_axis": weight_axis,
            },
        },
        "activation": {
            "shard": activation_shard,
            "key": "activation",
            "quantization": {
                "bits": activation_bits,
                "mode": activation_mode,
                "granularity": activation_granularity,
                "channel_axis": activation_axis,
            },
        },
        "accumulator_bits": accumulator_bits,
        "tolerance": {"max_abs": max_abs, "max_rel": max_rel},
    }


def _manifest() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "name": "deterministic-four-layer-demo",
        "seed": DEFAULT_SEED,
        "pipeline_version": "1.0.0",
        "operator_chain": ["l0", "l1", "l2", "l3"],
        "layers": [
            _layer(
                "l0",
                "weights/shard_00.npz",
                "calibration/shard_00.npz",
                weight_bits=8,
                weight_mode="symmetric",
                weight_granularity="per_channel",
                activation_bits=8,
                activation_mode="symmetric",
                activation_granularity="per_tensor",
                accumulator_bits=32,
                max_abs=2.0,
                max_rel=0.35,
            ),
            _layer(
                "l1",
                "weights/shard_01.npz",
                "calibration/shard_01.npz",
                weight_bits=4,
                weight_mode="asymmetric",
                weight_granularity="per_channel",
                activation_bits=8,
                activation_mode="asymmetric",
                activation_granularity="per_tensor",
                accumulator_bits=24,
                max_abs=2.5,
                max_rel=0.45,
            ),
            _layer(
                "l2",
                "weights/shard_02.npz",
                "calibration/shard_02.npz",
                weight_bits=8,
                weight_mode="asymmetric",
                weight_granularity="per_tensor",
                activation_bits=4,
                activation_mode="symmetric",
                activation_granularity="per_channel",
                activation_axis=0,
                accumulator_bits=11,
                max_abs=3.0,
                max_rel=0.55,
            ),
            _layer(
                "l3",
                "weights/shard_03.npz",
                "calibration/shard_03.npz",
                weight_bits=4,
                weight_mode="symmetric",
                weight_granularity="per_tensor",
                activation_bits=8,
                activation_mode="asymmetric",
                activation_granularity="per_tensor",
                accumulator_bits=16,
                max_abs=2.0,
                max_rel=0.35,
            ),
        ],
        "end_to_end_tolerance": {"max_abs": 4.0, "max_rel": 0.25},
        "attribution_sum_tolerance": 1e-9,
    }


def _shapes(weights: list[np.ndarray], activations: list[np.ndarray]) -> dict[str, Any]:
    layers: dict[str, Any] = {}
    for index in range(len(weights)):
        input_shape = list(activations[index].shape)
        weight_shape = list(weights[index].shape)
        output_shape = [input_shape[0], weight_shape[0]]
        layers[f"l{index}"] = {
            "input_shape": input_shape,
            "weight_shape": weight_shape,
            "output_shape": output_shape,
        }
    return {"schema_version": 1, "layers": layers}


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    atomic_write_json(path, manifest, pretty=True)


def generate_samples(
    samples_dir: Path, *, seed: int = DEFAULT_SEED, clean: bool = False
) -> dict[str, Any]:
    """Generate deterministic NPZ shards, manifests, and anomaly fixtures."""

    samples_dir = samples_dir.resolve()
    if clean and samples_dir.exists():
        shutil.rmtree(samples_dir)
    (samples_dir / "weights").mkdir(parents=True, exist_ok=True)
    (samples_dir / "calibration").mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    shapes = [(7, 6), (5, 7), (9, 5), (6, 9)]
    weights: list[np.ndarray] = []
    activations: list[np.ndarray] = []

    x = rng.normal(0.0, 1.0, size=(32, 6)).astype(np.float64)
    x[3, 2] = 12.0
    x[17, 4] = -8.5
    for index, shape in enumerate(shapes):
        weight = rng.normal(0.0, 0.24, size=shape).astype(np.float64)
        if index == 0:
            weight[4, 3] = -1.75
        elif index == 1:
            weight[2, 5] = 1.2
        elif index == 2:
            weight[6, 1] = -0.9
        weights.append(weight)
        activations.append(x.copy())
        if index < len(shapes) - 1:
            x = x @ weight.T
            if index == 1:
                x[5, 2] += 6.0
            elif index == 2:
                x[9, 0] -= 5.0

    for index, (weight, activation) in enumerate(zip(weights, activations, strict=True)):
        atomic_write_npz(
            samples_dir / "weights" / f"shard_{index:02d}.npz",
            {"weight": weight},
        )
        atomic_write_npz(
            samples_dir / "calibration" / f"shard_{index:02d}.npz",
            {"activation": activation},
        )

    manifest = _manifest()
    shape_declarations = _shapes(weights, activations)
    _write_manifest(samples_dir / "operator_manifest.json", manifest)
    atomic_write_json(
        samples_dir / "shape_declarations.json", shape_declarations, pretty=True
    )

    nan_activation = activations[0].copy()
    nan_activation[7, 1] = np.nan
    atomic_write_npz(
        samples_dir / "calibration_nan.npz", {"activation": nan_activation}
    )
    zero_weight = weights[0].copy()
    zero_weight[1, :] = 0.0
    atomic_write_npz(samples_dir / "weight_zero_channel.npz", {"weight": zero_weight})

    nan_manifest = json.loads(canonical_json_bytes(manifest))
    nan_manifest["name"] = "invalid-nan-activation"
    nan_manifest["layers"][0]["activation"]["shard"] = "calibration_nan.npz"
    _write_manifest(samples_dir / "operator_manifest_nan.json", nan_manifest)

    zero_manifest = json.loads(canonical_json_bytes(manifest))
    zero_manifest["name"] = "invalid-zero-channel"
    zero_manifest["layers"][0]["weight"]["shard"] = "weight_zero_channel.npz"
    _write_manifest(samples_dir / "operator_manifest_zero_channel.json", zero_manifest)

    bits_manifest = json.loads(canonical_json_bytes(manifest))
    bits_manifest["name"] = "invalid-unsupported-bitwidth"
    bits_manifest["layers"][0]["weight"]["quantization"]["bits"] = 6
    _write_manifest(
        samples_dir / "operator_manifest_unsupported_bits.json", bits_manifest
    )

    readme = """# Deterministic sample data

Generated by `python -m qpipe generate` with seed 20261122.

- Four weight shards and four calibration activation shards.
- 8-bit and 4-bit paths, symmetric and asymmetric modes, per-tensor and
  per-channel granularity, weight/activation outliers, an all-zero weight
  channel fixture, and a NaN activation fixture.
- The valid demo uses `operator_manifest.json` and
  `shape_declarations.json`.
- Invalid fixtures use `operator_manifest_nan.json`,
  `operator_manifest_zero_channel.json`, or
  `operator_manifest_unsupported_bits.json`.
"""
    atomic_write_json(
        samples_dir / "generation.json",
        {
            "seed": seed,
            "generator": "qpipe.samples",
            "layer_count": len(weights),
            "shards": 2 * len(weights),
            "fixtures": [
                "calibration_nan.npz",
                "weight_zero_channel.npz",
                "operator_manifest_nan.json",
                "operator_manifest_zero_channel.json",
                "operator_manifest_unsupported_bits.json",
            ],
        },
        pretty=True,
    )
    (samples_dir / "README.md").write_text(readme, encoding="utf-8")
    return {
        "samples_dir": str(samples_dir),
        "seed": seed,
        "layers": len(weights),
        "shards": 2 * len(weights),
        "manifest": str(samples_dir / "operator_manifest.json"),
        "shapes": str(samples_dir / "shape_declarations.json"),
    }
