from __future__ import annotations

import math

import numpy as np

from .errors import FusionError
from .manifest import AttentionSpec
from .util import sha256_array


def normalize_attention(spec: AttentionSpec, channels: int, *, layer: str) -> tuple[np.ndarray, dict[str, object]]:
    if len(spec.weights) != channels:
        raise FusionError.invalid_param(
            layer,
            len(spec.weights),
            "attention.weights",
            "length does not match lateral output channels",
            expected_length=channels,
            actual_length=len(spec.weights),
        )
    raw = np.asarray(spec.weights, dtype=np.float64)
    finite = np.isfinite(raw)
    if not np.all(finite):
        first = int(np.flatnonzero(~finite)[0])
        raise FusionError.invalid_param(
            layer,
            first,
            "attention.weights",
            "contains NaN or Inf",
            value=repr(float(raw[first])),
        )
    raw_sum = math.fsum(spec.weights)
    abs_sum = math.fsum(abs(value) for value in spec.weights)
    normalization_applied = False
    if spec.normalization == "sum_to_one":
        if abs(raw_sum) <= 1e-15:
            raise FusionError.invalid_param(
                layer,
                0,
                "attention.weights",
                "normalization denominator is zero",
                raw_sum=raw_sum,
                normalization=spec.normalization,
            )
        if not math.isclose(raw_sum, 1.0, rel_tol=0.0, abs_tol=1e-12):
            effective = raw / raw_sum
            normalization_applied = True
        else:
            effective = raw.copy()
    elif spec.normalization == "l1":
        if abs_sum <= 1e-15:
            raise FusionError.invalid_param(
                layer,
                0,
                "attention.weights",
                "l1 normalization denominator is zero",
                raw_sum=raw_sum,
                normalization=spec.normalization,
            )
        effective = raw / abs_sum
        normalization_applied = True
    else:
        raise FusionError.invalid_param(layer, None, "attention.normalization", f"unsupported rule {spec.normalization!r}")
    if not np.all(np.isfinite(effective)):
        raise FusionError.numeric(layer, "non_finite_attention", raw_sum=raw_sum)
    meta = {
        "channels": channels,
        "raw_sum": raw_sum,
        "abs_sum": abs_sum,
        "normalization": spec.normalization,
        "normalization_applied": normalization_applied,
        "effective_weights": [float(v) for v in effective],
        "effective_sha256": sha256_array(effective),
        "raw_sha256": sha256_array(raw),
    }
    return effective, meta


def apply_channel_attention(x: np.ndarray, weights: np.ndarray, *, layer: str) -> np.ndarray:
    if x.ndim != 4:
        raise FusionError.shape_mismatch(layer, [0, 0, 0, 0], x.shape, stage="attention")
    if weights.ndim != 1 or weights.shape[0] != x.shape[1]:
        raise FusionError.shape_mismatch(layer, [x.shape[1]], weights.shape, stage="attention_channels")
    out = x * weights[None, :, None, None]
    if not np.all(np.isfinite(out)):
        count = int(np.count_nonzero(~np.isfinite(out)))
        raise FusionError.numeric(layer, "non_finite_attention_output", non_finite_count=count)
    return out


def validate_fusion_coeffs(coeffs: dict[str, float], *, layer: str, require_topdown: bool) -> dict[str, float]:
    if not coeffs:
        raise FusionError.invalid_param(layer, 0, "fusion.coeffs", "must not be empty")
    ordered = dict(sorted(coeffs.items()))
    for index, (key, value) in enumerate(ordered.items()):
        if not math.isfinite(value):
            raise FusionError.invalid_param(layer, index, f"fusion.coeffs.{key}", "must be finite", value=repr(value))
    if require_topdown and "topdown" not in ordered:
        raise FusionError.invalid_param(layer, None, "fusion.coeffs", "missing topdown coefficient")
    if not require_topdown and "topdown" in ordered:
        raise FusionError.invalid_param(layer, None, "fusion.coeffs", "topdown coefficient is invalid for the deepest layer")
    if all(value == 0.0 for value in ordered.values()):
        first = next(iter(ordered))
        raise FusionError.invalid_param(layer, 0, f"fusion.coeffs.{first}", "all fusion coefficients are zero", coefficients=ordered)
    return ordered
