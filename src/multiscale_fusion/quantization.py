from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import FusionError
from .util import sha256_array


@dataclass
class QuantizationResult:
    layer: str
    bits: int
    scale: float
    qmax: int
    quantized: np.ndarray
    dequantized: np.ndarray
    error_bound: np.ndarray
    abs_reference_error: np.ndarray
    metadata: dict[str, Any]

    def artifact_arrays(self) -> dict[str, np.ndarray]:
        return {
            "quantized.npy": self.quantized,
            "dequantized.npy": self.dequantized,
            "error_bound.npy": self.error_bound,
            "abs_reference_error.npy": self.abs_reference_error,
        }


def quantize_symmetric(
    values: np.ndarray,
    bits: int,
    *,
    layer: str,
    reference: np.ndarray,
    scale_override: float | None = None,
) -> QuantizationResult:
    if bits < 2 or bits > 24:
        raise FusionError.invalid_param(layer, None, "quantization.bits", "must be in [2, 24]", bits=bits)
    array = np.asarray(values, dtype=np.float64)
    ref = np.asarray(reference, dtype=np.float64)
    if array.shape != ref.shape:
        raise FusionError.shape_mismatch(layer, ref.shape, array.shape, stage="quantization_reference")
    if not np.all(np.isfinite(array)):
        count = int(np.count_nonzero(~np.isfinite(array)))
        raise FusionError.numeric(layer, "non_finite_quantization_input", non_finite_count=count)
    if not np.all(np.isfinite(ref)):
        count = int(np.count_nonzero(~np.isfinite(ref)))
        raise FusionError.numeric(layer, "non_finite_reference", non_finite_count=count)
    max_abs = float(np.max(np.abs(array))) if array.size else 0.0
    qmax = (1 << (bits - 1)) - 1
    if scale_override is None:
        if max_abs <= 0.0:
            raise FusionError.numeric(
                layer,
                "zero_scale",
                scale=0.0,
                saturation_count=0,
                bits=bits,
            )
        scale = max_abs / float(qmax)
    else:
        scale = float(scale_override)
    if not math.isfinite(scale) or scale <= 0.0:
        raise FusionError.numeric(layer, "zero_scale", scale=scale, saturation_count=0, bits=bits)

    q_float = np.rint(array / scale)
    saturation_mask = np.abs(q_float) > float(qmax)
    saturation_count = int(np.count_nonzero(saturation_mask))
    if saturation_count == array.size and array.size > 0:
        raise FusionError.numeric(
            layer,
            "all_quantized_values_saturated",
            scale=scale,
            saturation_count=saturation_count,
            element_count=int(array.size),
        )
    quantized = np.clip(q_float, -qmax, qmax).astype(np.int32)
    dequantized = quantized.astype(np.float64) * scale
    reference_error = np.abs(dequantized - ref)
    numeric_slack = 1e-12 * max(1.0, max_abs, float(np.max(np.abs(ref))) if ref.size else 0.0)
    error_bound_value = 0.5 * scale + numeric_slack
    error_bound = np.full(array.shape, error_bound_value, dtype=np.float64)
    max_reference_error = float(np.max(reference_error)) if reference_error.size else 0.0
    reference_mismatch_count = int(np.count_nonzero(reference_error > error_bound_value))
    first_reference_mismatch = (
        int(np.flatnonzero(reference_error > error_bound_value)[0]) if reference_mismatch_count else None
    )
    effective_bits = 0.0 if max_abs <= 0.0 else math.log2(max_abs / (0.5 * scale) + 1.0)
    quant_error = np.abs(dequantized - array)
    metadata = {
        "layer": layer,
        "bits": bits,
        "qmax": qmax,
        "scale": scale,
        "scale_source": "manifest_override" if scale_override is not None else "derived_from_output_max_abs",
        "max_abs": max_abs,
        "effective_bits": effective_bits,
        "saturation_count": saturation_count,
        "zero_code_count": int(np.count_nonzero(quantized == 0)),
        "theoretical_error_bound": 0.5 * scale,
        "error_bound_with_reference_slack": error_bound_value,
        "numeric_slack": numeric_slack,
        "max_quantization_error": float(np.max(quant_error)) if quant_error.size else 0.0,
        "max_reference_error": max_reference_error,
        "mean_reference_error": float(np.mean(reference_error)) if reference_error.size else 0.0,
        "reference_error_within_bound": reference_mismatch_count == 0,
        "reference_mismatch_count": reference_mismatch_count,
        "first_reference_mismatch_index": first_reference_mismatch,
        "reference_sha256": sha256_array(ref),
        "dequantized_sha256": sha256_array(dequantized),
        "error_bound_sha256": sha256_array(error_bound),
        "shape": list(array.shape),
        "dtype": "int32",
    }
    return QuantizationResult(
        layer=layer,
        bits=bits,
        scale=scale,
        qmax=qmax,
        quantized=quantized,
        dequantized=dequantized,
        error_bound=error_bound,
        abs_reference_error=reference_error,
        metadata=metadata,
    )
