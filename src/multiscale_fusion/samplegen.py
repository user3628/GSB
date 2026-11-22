from __future__ import annotations

import math
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from .errors import FusionError
from .kernels import build_conv_params, conv_output_hw
from .manifest import Manifest, TaskSpec
from .util import pretty_json_bytes, sha256_array, sha256_file, write_bytes, write_json, write_npy


def _scalar_conv(x: np.ndarray, weight: np.ndarray, bias: np.ndarray, stride: tuple[int, int], padding: str) -> np.ndarray:
    n, c, h, w = x.shape
    out_c, _, kh, kw = weight.shape
    sh, sw = stride
    oh, ow = conv_output_hw((h, w), (kh, kw), stride, padding)
    if padding == "same":
        out_h = int(math.ceil(h / sh))
        out_w = int(math.ceil(w / sw))
        pad_h = max((out_h - 1) * sh + kh - h, 0)
        pad_w = max((out_w - 1) * sw + kw - w, 0)
        pt, pl = pad_h // 2, pad_w // 2
        padded = np.pad(x, ((0, 0), (0, 0), (pt, pad_h - pt), (pl, pad_w - pl)), mode="constant")
    else:
        padded = x
    out = np.zeros((n, out_c, oh, ow), dtype=np.float64)
    for ni in range(n):
        for oi in range(out_c):
            for y in range(oh):
                for xi in range(ow):
                    acc = 0.0
                    for ci in range(c):
                        for ki in range(kh):
                            for kj in range(kw):
                                acc += float(padded[ni, ci, y * sh + ki, xi * sw + kj]) * float(weight[oi, ci, ki, kj])
                    out[ni, oi, y, xi] = acc + float(bias[oi])
    return out


def _scalar_resize(x: np.ndarray, target_h: int, target_w: int, scale: int, mode: str, align_corners: bool) -> np.ndarray:
    n, c, src_h, src_w = x.shape
    out = np.zeros((n, c, target_h, target_w), dtype=np.float64)
    if mode == "nearest":
        for oh in range(target_h):
            sy = min(oh // scale, src_h - 1)
            for ow in range(target_w):
                sx = min(ow // scale, src_w - 1)
                out[:, :, oh, ow] = x[:, :, sy, sx]
        return out
    if align_corners:
        rows = [0.0 if target_h == 1 else i * (src_h - 1) / max(target_h - 1, 1) for i in range(target_h)]
        cols = [0.0 if target_w == 1 else i * (src_w - 1) / max(target_w - 1, 1) for i in range(target_w)]
    else:
        rows = [min(max((i + 0.5) / scale - 0.5, 0.0), float(max(src_h - 1, 0))) for i in range(target_h)]
        cols = [min(max((i + 0.5) / scale - 0.5, 0.0), float(max(src_w - 1, 0))) for i in range(target_w)]
    for oh in range(target_h):
        y0 = int(math.floor(rows[oh]))
        y1 = min(y0 + 1, src_h - 1)
        wy = rows[oh] - y0
        for ow in range(target_w):
            x0 = int(math.floor(cols[ow]))
            x1 = min(x0 + 1, src_w - 1)
            wx = cols[ow] - x0
            top_left = x[:, :, y0, x0] * (1.0 - wx) + x[:, :, y0, x1] * wx
            bottom_left = x[:, :, y1, x0] * (1.0 - wx) + x[:, :, y1, x1] * wx
            out[:, :, oh, ow] = top_left * (1.0 - wy) + bottom_left * wy
    return out


def _scalar_reference_with_inputs(task: TaskSpec, inputs: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    conv_params: dict[str, tuple[np.ndarray, np.ndarray, dict[str, Any]]] = {}
    for layer in task.layers:
        in_channels = int(inputs[layer.name].shape[1])
        conv_params[layer.name] = build_conv_params(layer.name, in_channels, layer.lateral, task.seed)
    outputs: dict[str, np.ndarray] = {}
    previous: np.ndarray | None = None
    for layer in task.processing_layers:
        weight, bias, _ = conv_params[layer.name]
        lateral = _scalar_conv(inputs[layer.name], weight, bias, layer.lateral.stride, layer.lateral.padding)
        raw_sum = math.fsum(layer.attention.weights)
        if layer.attention.normalization == "sum_to_one":
            attention = [value / raw_sum for value in layer.attention.weights]
        else:
            denom = math.fsum(abs(value) for value in layer.attention.weights)
            attention = [value / denom for value in layer.attention.weights]
        weighted = np.empty_like(lateral)
        for ci, weight_value in enumerate(attention):
            weighted[:, ci, :, :] = lateral[:, ci, :, :] * weight_value
        coeffs = layer.fusion.coeffs
        if previous is None:
            fused = coeffs["lateral"] * weighted
        else:
            assert layer.upsample is not None
            target_h, target_w = weighted.shape[2], weighted.shape[3]
            aligned = _scalar_resize(
                previous,
                target_h,
                target_w,
                layer.upsample.scale,
                layer.upsample.mode,
                layer.upsample.align_corners,
            )
            fused = coeffs["lateral"] * weighted + coeffs["topdown"] * aligned
        outputs[layer.name] = np.ascontiguousarray(fused, dtype=np.float64)
        previous = fused
    return outputs


def _manifest_dict(seed: int) -> dict[str, Any]:
    layers = [
        {
            "name": "p3",
            "input": "shards/p3.npy",
            "reference": "reference/p3.npy",
            "expected_output_shape": [2, 7, 17, 21],
            "allowed_range": [-50000.0, 50000.0],
            "lateral": {
                "kernel_size": [3, 3],
                "stride": [1, 1],
                "out_channels": 7,
                "padding": "same",
                "weight_scale": 0.72,
                "kernel_seed": 3101,
            },
            "upsample": {"scale": 2, "mode": "bilinear", "align_corners": False, "max_tail": 1},
            "attention": {
                "weights": [0.20, 0.10, 0.15, 0.05, 0.30, 0.20, 0.12],
                "normalization": "sum_to_one",
            },
            "fusion": {"mode": "weighted_sum", "coeffs": {"lateral": 0.57, "topdown": 0.43}},
            "quantization": {"bits": 10},
        },
        {
            "name": "p4",
            "input": "shards/p4.npy",
            "reference": "reference/p4.npy",
            "expected_output_shape": [2, 7, 8, 10],
            "allowed_range": [-20000.0, 20000.0],
            "lateral": {
                "kernel_size": [3, 5],
                "stride": [1, 1],
                "out_channels": 7,
                "padding": "same",
                "weight_scale": 0.68,
                "kernel_seed": 3102,
            },
            "upsample": {"scale": 2, "mode": "nearest", "align_corners": False, "max_tail": 1},
            "attention": {
                "weights": [0.10, 0.20, 0.15, 0.10, 0.25, 0.10, 0.10],
                "normalization": "sum_to_one",
            },
            "fusion": {"mode": "weighted_sum", "coeffs": {"lateral": 0.62, "topdown": 0.38}},
            "quantization": {"bits": 12},
        },
        {
            "name": "p5",
            "input": "shards/p5.npy",
            "reference": "reference/p5.npy",
            "expected_output_shape": [2, 7, 4, 5],
            "allowed_range": [-20000.0, 20000.0],
            "lateral": {
                "kernel_size": [1, 1],
                "stride": [1, 1],
                "out_channels": 7,
                "padding": "same",
                "weight_scale": 0.80,
                "kernel_seed": 3103,
            },
            "attention": {
                "weights": [0.05, 0.10, 0.20, 0.25, 0.15, 0.15, 0.10],
                "normalization": "sum_to_one",
            },
            "fusion": {"mode": "weighted_sum", "coeffs": {"lateral": 1.0}},
            "quantization": {"bits": 8},
        },
    ]
    return {
        "schema_version": 1,
        "tasks": [
            {
                "id": "demo_fpn",
                "seed": seed,
                "layers": layers,
                "final_reference": "reference/final.npy",
                "sensitivity": {
                    "amplitude": 0.001,
                    "tolerance": 0.25,
                    "first_order_probe": 0.00001,
                },
            }
        ],
    }


def _make_features(seed: int) -> dict[str, np.ndarray]:
    rng = np.random.default_rng(seed)
    p3 = rng.standard_normal((2, 5, 17, 21), dtype=np.float64) * 1.7
    p4 = rng.standard_normal((2, 3, 8, 10), dtype=np.float64) * 1.2
    p5 = rng.standard_normal((2, 9, 4, 5), dtype=np.float64) * 0.8
    # Fixed extreme activations exercise dynamic-range handling without
    # relying on the random draw to happen to generate them.
    p3[0, 0, 0, 0] = 24000.0
    p3[0, 1, 3, 4] = -31000.0
    p3[1, 4, -1, -1] = 17500.0
    p4[0, 0, 0, 0] = 7200.0
    p4[1, 2, -1, -1] = -9100.0
    p5[0, 3, 0, 0] = 4100.0
    p5[1, 8, -1, -1] = -5300.0
    return {"p3": np.ascontiguousarray(p3), "p4": np.ascontiguousarray(p4), "p5": np.ascontiguousarray(p5)}


def generate_demo_bundle(out_dir: str | Path, *, seed: int = 20261123, force: bool = False) -> dict[str, Any]:
    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        if not force:
            raise FusionError(
                "E_INVALID_PARAM",
                f"output directory is not empty: {out_dir}",
                details={"path": str(out_dir), "hint": "pass force=True or choose a new directory"},
            )
        shutil.rmtree(out_dir)
    (out_dir / "shards").mkdir(parents=True, exist_ok=True)
    (out_dir / "reference").mkdir(parents=True, exist_ok=True)
    inputs = _make_features(seed)
    for name, array in inputs.items():
        write_npy(out_dir / "shards" / f"{name}.npy", array)
    manifest_dict = _manifest_dict(seed)
    manifest_path = out_dir / "manifest.json"
    write_bytes(manifest_path, pretty_json_bytes(manifest_dict))
    manifest = Manifest.load(manifest_path)
    task = manifest.by_id("demo_fpn")
    reference = _scalar_reference_with_inputs(task, inputs)
    for name in ("p3", "p4", "p5"):
        write_npy(out_dir / "reference" / f"{name}.npy", reference[name])
    write_npy(out_dir / "reference" / "final.npy", reference[task.finest_layer.name])
    metadata = {
        "generator": "multiscale_fusion.samplegen",
        "generator_version": 1,
        "fixed_seed": seed,
        "reference_implementation": "independent scalar loops, float64",
        "task_id": task.id,
        "shards": {
            name: {
                "path": f"shards/{name}.npy",
                "shape": list(array.shape),
                "sha256": sha256_array(array),
                "min": float(np.min(array)),
                "max": float(np.max(array)),
            }
            for name, array in sorted(inputs.items())
        },
        "references": {
            name: {
                "path": f"reference/{name}.npy",
                "shape": list(array.shape),
                "sha256": sha256_array(array),
            }
            for name, array in sorted({**reference, "final": reference[task.finest_layer.name]}.items())
        },
        "manifest_sha256": sha256_file(manifest_path),
    }
    write_json(out_dir / "reference" / "metadata.json", metadata)
    return metadata
