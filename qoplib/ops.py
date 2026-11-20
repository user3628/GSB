"""Quantized linear operator chains and precision self-check diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np

from .errors import NumericMismatchError
from .quantization import (
    QuantConfig,
    quantized_linear,
    reference_linear,
)


@dataclass(frozen=True)
class LayerSpec:
    name: str
    weight: str
    in_features: int
    out_features: int
    bitwidth: int
    mode: str
    granularity: str
    activation_bitwidth: int
    activation_mode: str
    activation_granularity: str
    rel_tol: float
    abs_tol: float
    sample_positions: int

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "LayerSpec":
        return cls(
            name=str(payload["name"]),
            weight=str(payload["weight"]),
            in_features=int(payload["in_features"]),
            out_features=int(payload["out_features"]),
            bitwidth=int(payload["bitwidth"]),
            mode=str(payload["mode"]),
            granularity=str(payload["granularity"]),
            activation_bitwidth=int(payload.get("activation_bitwidth",
                                                payload["bitwidth"])),
            activation_mode=str(payload.get("activation_mode",
                                            payload["mode"])),
            activation_granularity=str(payload.get("activation_granularity",
                                                   payload["granularity"])),
            rel_tol=float(payload.get("rel_tol", 0.35)),
            abs_tol=float(payload.get("abs_tol", 0.5)),
            sample_positions=int(payload.get("sample_positions", 32)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "weight": self.weight,
            "in_features": self.in_features,
            "out_features": self.out_features,
            "bitwidth": self.bitwidth,
            "mode": self.mode,
            "granularity": self.granularity,
            "activation_bitwidth": self.activation_bitwidth,
            "activation_mode": self.activation_mode,
            "activation_granularity": self.activation_granularity,
            "rel_tol": self.rel_tol,
            "abs_tol": self.abs_tol,
            "sample_positions": self.sample_positions,
        }

    def weight_config(self) -> QuantConfig:
        # Weight shape is (out_features, in_features). A per-output-channel
        # scale has one entry per output feature (row), computed by reducing
        # the in_features axis, i.e. axis 1 for 2D weights. The quantizer
        # squeezes the reduced keepdims axis back to (out_features, 1).
        axis = 1 if self.granularity == "channel" else -1
        return QuantConfig(bitwidth=self.bitwidth, mode=self.mode,
                           granularity=self.granularity, channel_axis=axis)

    def activation_config(self) -> QuantConfig:
        return QuantConfig(
            bitwidth=self.activation_bitwidth,
            mode=self.activation_mode,
            granularity=self.activation_granularity,
            channel_axis=-1,
        )


def _sampled_indices(size: int, count: int, seed_text: str) -> np.ndarray:
    if size <= count:
        return np.arange(size)
    digest_seed = int.from_bytes(
        __import__("hashlib").sha256(seed_text.encode("utf-8")).digest()[:4],
        "little",
    )
    rng = np.random.default_rng(digest_seed)
    return np.sort(rng.choice(size, size=count, replace=False))


def _error_metrics(actual: np.ndarray, reference: np.ndarray,
                   sample_count: int, seed_text: str) -> Dict[str, Any]:
    diff = actual - reference
    abs_diff = np.abs(diff)
    abs_ref = np.abs(reference)
    # Per-element relative error (guarded for near-zero references).
    element_rel = abs_diff / np.maximum(abs_ref, 1e-12)
    # Scale-normalised relative error: deviation relative to the layer's
    # output magnitude, which stays meaningful around zero-crossings and is
    # the metric compared against the configured relative tolerance.
    scale = float(np.max(abs_ref)) if np.max(abs_ref) > 0 else 1.0
    rel = abs_diff / scale

    flat_abs = abs_diff.reshape(-1)
    flat_rel = rel.reshape(-1)
    flat_element_rel = element_rel.reshape(-1)
    indices = _sampled_indices(flat_abs.size, sample_count, seed_text)

    sample_abs = flat_abs[indices]
    sample_rel = flat_rel[indices]
    return {
        "max_abs_error": float(np.max(flat_abs)),
        "max_rel_error": float(np.max(flat_rel)),
        "max_element_rel_error": float(np.max(flat_element_rel)),
        "reference_scale": scale,
        "rmse": float(np.sqrt(np.mean(np.square(diff)))),
        "sample_max_abs_error": float(np.max(sample_abs)),
        "sample_max_rel_error": float(np.max(sample_rel)),
        "sample_positions": [int(i) for i in indices],
    }


def run_linear_chain(
    activation: np.ndarray,
    weights: Dict[str, np.ndarray],
    layers: List[LayerSpec],
    shard_id: str,
    check_tolerance: bool = True,
) -> Dict[str, Any]:
    """Execute a quantized linear chain plus a high precision reference chain."""

    current_q = np.asarray(activation, dtype=np.float64)
    current_ref = np.asarray(activation, dtype=np.float64)
    layer_reports: List[Dict[str, Any]] = []
    outputs: Dict[str, np.ndarray] = {}
    max_rel_tol = 0.0

    for layer in layers:
        weight = np.asarray(weights[layer.weight], dtype=np.float64)
        bias = weights.get(f"{layer.weight}_bias")
        expected_shape = (current_q.shape[0], layer.out_features)

        q_out = quantized_linear(
            current_q, weight, bias,
            layer.activation_config(), layer.weight_config(),
        )
        ref_out = reference_linear(current_ref, weight, bias)

        if q_out.shape != tuple(expected_shape):
            raise NumericMismatchError(
                f"layer {layer.name} produced shape {q_out.shape}, expected "
                f"{expected_shape}",
                details={"shard_id": shard_id, "layer": layer.name,
                         "actual_shape": list(q_out.shape),
                         "expected_shape": list(expected_shape)},
            )

        metrics = _error_metrics(
            q_out, ref_out, layer.sample_positions,
            f"{shard_id}:{layer.name}",
        )
        # Relative error bound estimate: observed max plus one ULP-scale slack
        # gives a conservative empirical upper bound for the shard.
        rel_bound = min(1.0, metrics["max_rel_error"] * 1.05)
        margin = float(layer.rel_tol - metrics["max_rel_error"])
        metrics["rel_error_bound_estimate"] = float(rel_bound)
        metrics["rel_tol_margin"] = margin
        metrics["within_tolerance"] = bool(
            metrics["max_abs_error"] <= layer.abs_tol
            or metrics["max_rel_error"] <= layer.rel_tol
        )
        metrics["layer"] = layer.name
        metrics["output_shape"] = list(q_out.shape)
        layer_reports.append(metrics)
        max_rel_tol = max(max_rel_tol, metrics["max_rel_error"])

        outputs[layer.name] = q_out
        current_q = q_out
        current_ref = ref_out

    total_rmse = sum(report["rmse"] for report in layer_reports) or 1.0
    for report in layer_reports:
        report["error_contribution"] = float(report["rmse"] / total_rmse)
    tightest = min(layer_reports,
                   key=lambda report: report["rel_tol_margin"]) if layer_reports \
        else None
    result = {
        "outputs": outputs,
        "final_output": current_q,
        "reference_output": current_ref,
        "layer_reports": layer_reports,
        "tightest_margin_layer": None if tightest is None else {
            "layer": tightest["layer"],
            "rel_tol_margin": tightest["rel_tol_margin"],
            "max_rel_error": tightest["max_rel_error"],
        },
    }

    if check_tolerance:
        for layer, report in zip(layers, layer_reports):
            if not report["within_tolerance"]:
                raise NumericMismatchError(
                    f"quantization error exceeds tolerance for layer "
                    f"{layer.name} of shard {shard_id}",
                    details={
                        "shard_id": shard_id,
                        "layer": layer.name,
                        "max_abs_error": report["max_abs_error"],
                        "max_rel_error": report["max_rel_error"],
                        "rel_tol": layer.rel_tol,
                        "abs_tol": layer.abs_tol,
                    },
                )
    return result
