"""Deterministic demo workspace generation."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import npy_bytes
from .manifest import Workspace
from .ops import ExecOptions, execute_operator
from .storage import atomic_write_bytes, atomic_write_json


SEED = 20261120
CONFIG_VERSION = "demo-v1-20261120"


def _rounded_weights(rng: np.random.Generator, shape: tuple[int, ...]) -> list[Any]:
    return np.round(rng.normal(0.0, 0.32, size=shape), 6).tolist()


def _rounded_bias(rng: np.random.Generator, size: int) -> list[float]:
    return np.round(rng.normal(0.0, 0.08, size=size), 6).tolist()


def _conv2d(weights: list[Any], bias: list[float], *, stride: list[int], padding: str, dilation: list[int]) -> dict[str, Any]:
    return {
        "name": "conv2d",
        "params": {
            "weights": weights,
            "bias": bias,
            "stride": stride,
            "padding": padding,
            "dilation": dilation,
        },
    }


def _conv1d(weights: list[Any], bias: list[float], *, stride: int, padding: str, dilation: int) -> dict[str, Any]:
    return {
        "name": "conv1d",
        "params": {
            "weights": weights,
            "bias": bias,
            "stride": stride,
            "padding": padding,
            "dilation": dilation,
        },
    }


def _resize(mode: str, scale: list[float], align_corners: bool) -> dict[str, Any]:
    return {
        "name": "interpolate",
        "params": {
            "mode": mode,
            "scale": scale,
            "align_corners": align_corners,
        },
    }


def build_demo_definition() -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    rng = np.random.Generator(np.random.PCG64(SEED))
    tensors: dict[str, np.ndarray] = {}

    small = np.round(rng.uniform(-1.0, 1.0, size=(2, 5, 6, 3)), 6)
    small[0, 0, :, 0] = 1.25
    small[1, :, 0, 2] = -1.25
    tensors["small_odd_pad"] = np.ascontiguousarray(small, dtype=np.float64)

    medium = np.round(rng.uniform(-1.0, 1.0, size=(1, 9, 11, 5)), 6)
    medium[0, 0, 0, :] = 1.0
    medium[0, -1, -1, :] = -1.0
    tensors["medium_odd_dilated"] = np.ascontiguousarray(medium, dtype=np.float64)

    boundary = np.round(rng.uniform(-1.0, 1.0, size=(1, 6, 5, 1)), 6)
    boundary[0, 0, :, 0] = np.linspace(-1.5, 1.5, 5, dtype=np.float64)
    boundary[0, :, 0, 0] = np.linspace(1.5, -1.5, 6, dtype=np.float64)
    tensors["boundary_chain"] = np.ascontiguousarray(boundary, dtype=np.float64)

    w_small_conv = _rounded_weights(rng, (3, 3, 3, 5))
    b_small_conv = _rounded_bias(rng, 5)
    w_medium_conv = _rounded_weights(rng, (2, 3, 5, 4))
    b_medium_conv = _rounded_bias(rng, 4)
    w_medium_1d = _rounded_weights(rng, (3, 4, 3))
    b_medium_1d = _rounded_bias(rng, 3)
    w_boundary_1d = _rounded_weights(rng, (5, 1, 3))
    b_boundary_1d = _rounded_bias(rng, 3)
    w_boundary_conv = _rounded_weights(rng, (3, 3, 3, 2))
    b_boundary_conv = _rounded_bias(rng, 2)

    manifest = {
        "format_version": 1,
        "config_version": CONFIG_VERSION,
        "seed": SEED,
        "notes": "Deterministic NHWC fixtures covering odd channels, padding, 1D/2D convolution, upsample, and downsample.",
        "shards": [
            {
                "id": "small_odd_pad",
                "tensor": "tensors/small_odd_pad.npy",
                "operators": [
                    _conv2d(
                        w_small_conv,
                        b_small_conv,
                        stride=[1, 2],
                        padding="same",
                        dilation=[1, 1],
                    ),
                    _resize("bilinear", [1.75, 1.5], False),
                ],
            },
            {
                "id": "medium_odd_dilated",
                "tensor": "tensors/medium_odd_dilated.npy",
                "operators": [
                    _conv2d(
                        w_medium_conv,
                        b_medium_conv,
                        stride=[2, 1],
                        padding="valid",
                        dilation=[1, 2],
                    ),
                    _resize("bicubic", [0.65, 0.5], True),
                    _conv1d(
                        w_medium_1d,
                        b_medium_1d,
                        stride=2,
                        padding="same",
                        dilation=1,
                    ),
                ],
            },
            {
                "id": "boundary_chain",
                "tensor": "tensors/boundary_chain.npy",
                "operators": [
                    _conv1d(
                        w_boundary_1d,
                        b_boundary_1d,
                        stride=2,
                        padding="same",
                        dilation=2,
                    ),
                    _resize("nearest", [2.0, 2.0], False),
                    _conv2d(
                        w_boundary_conv,
                        b_boundary_conv,
                        stride=[1, 1],
                        padding="same",
                        dilation=[2, 2],
                    ),
                    _resize("bicubic", [0.5, 0.5], False),
                ],
            },
        ],
    }
    return manifest, tensors


def _expectations_for_workspace(root: Path) -> dict[str, Any]:
    workspace = Workspace(root)
    document: dict[str, Any] = {"format_version": 1, "tolerance": 1e-6, "shards": {}}
    options = ExecOptions(block_h=2, block_w=3, out_channel_block=2)
    for entry in workspace.load_manifest()["shards"]:
        plan = workspace.plan_shard(entry["id"])
        tensor = workspace.load_tensor(entry)
        stages = []
        for index, spec in enumerate(plan.specs):
            output, _execution = execute_operator(spec, tensor, options)
            minimum = float(np.min(output))
            maximum = float(np.max(output))
            stages.append(
                {
                    "operator_index": index,
                    "operator_name": spec.name,
                    "shape": list(output.shape),
                    "range": [round(minimum - 1e-6, 9), round(maximum + 1e-6, 9)],
                    "tolerance": 1e-6,
                    "observed_min": minimum,
                    "observed_max": maximum,
                }
            )
            tensor = output
        document["shards"][entry["id"]] = {"stages": stages}
    return document


def generate_demo_workspace(root: str | Path, *, force: bool = False) -> dict[str, Any]:
    destination = Path(root).resolve()
    if destination.exists() and any(destination.iterdir()):
        if not force:
            raise FileExistsError(f"workspace is not empty: {destination}")
        for child in destination.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    destination.mkdir(parents=True, exist_ok=True)
    manifest, tensors = build_demo_definition()
    (destination / "tensors").mkdir(parents=True, exist_ok=True)
    for shard_id, tensor in tensors.items():
        atomic_write_bytes(destination / "tensors" / f"{shard_id}.npy", npy_bytes(tensor))
    atomic_write_json(destination / "manifest.json", manifest, pretty=True)

    expectations = _expectations_for_workspace(destination)
    atomic_write_json(destination / "expectations.json", expectations, pretty=True)
    _write_invalid_cases(destination)
    return {
        "root": str(destination),
        "seed": SEED,
        "config_version": CONFIG_VERSION,
        "shards": [
            {
                "id": shard["id"],
                "tensor": shard["tensor"],
                "shape": list(tensors[shard["id"]].shape),
                "operator_count": len(shard["operators"]),
            }
            for shard in manifest["shards"]
        ],
        "expectations": "expectations.json",
    }


def _write_invalid_cases(root: Path) -> None:
    """Keep copy-ready bad chains for the documented error-code demo."""

    cases = {
        "invalid_conv_missing_weights.json": {
            "case": "invalid convolution parameters",
            "expected_code": "E_INVALID_STAGE",
            "shard_id": "small_odd_pad",
            "operator_index": 0,
            "operator": {"name": "conv2d", "params": {"stride": [1, 1], "padding": "same", "dilation": [1, 1]}},
        },
        "invalid_scale_out_of_bounds.json": {
            "case": "interpolation scale outside [1/64, 64]",
            "expected_code": "E_INVALID_STAGE",
            "shard_id": "small_odd_pad",
            "operator_index": 1,
            "operator": {
                "name": "interpolate",
                "params": {"mode": "bilinear", "scale": [129.0, 1.0], "align_corners": False},
            },
        },
    }
    for filename, payload in cases.items():
        atomic_write_json(root / "invalid_cases" / filename, payload, pretty=True)

