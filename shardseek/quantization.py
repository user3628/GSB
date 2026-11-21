"""Projection, fixed-point quantization, and explicit error propagation."""

from __future__ import annotations

import math
from decimal import Decimal, getcontext
from typing import Any, Iterable

import numpy as np

from .errors import E_DIM_MISMATCH, E_NUMERIC, error


getcontext().prec = 50
DECIMAL_ZERO = Decimal("0")
ROUNDOFF_MARGIN = Decimal("1e-15")


def project_and_normalize(
    raw_vector: Iterable[float] | np.ndarray,
    matrix: Iterable[Iterable[float]],
    *,
    route: str,
    item_id: str | None = None,
    stage: str = "projection",
) -> np.ndarray:
    vector = np.asarray(raw_vector, dtype=np.float64)
    projection = np.asarray(matrix, dtype=np.float64)
    if projection.ndim != 2 or vector.ndim != 1 or projection.shape[0] != vector.shape[0]:
        raise error(
            E_DIM_MISMATCH,
            "Feature dimension does not match projection matrix",
            stage=stage,
            item_id=item_id,
            details={
                "route": route,
                "feature_shape": list(vector.shape),
                "matrix_shape": list(projection.shape),
                "expected_matrix_rows": int(vector.shape[0]) if vector.ndim == 1 else None,
            },
        )
    finite_vector = np.isfinite(vector)
    if not np.all(finite_vector):
        positions = np.argwhere(~finite_vector).ravel().tolist()
        raise error(
            E_NUMERIC,
            "Raw feature contains NaN or Inf",
            stage=stage,
            item_id=item_id,
            details={"route": route, "dimension_positions": positions[:16]},
        )
    finite_matrix = np.isfinite(projection)
    if not np.all(finite_matrix):
        positions = np.argwhere(~finite_matrix)
        raise error(
            E_NUMERIC,
            "Projection matrix contains NaN or Inf",
            stage=stage,
            item_id=item_id,
            details={"route": route, "dimension_position": positions[0].tolist()},
        )
    projected = vector @ projection
    if not np.all(np.isfinite(projected)):
        positions = np.argwhere(~np.isfinite(projected)).ravel().tolist()
        raise error(
            E_NUMERIC,
            "Projected vector contains NaN or Inf",
            stage=stage,
            item_id=item_id,
            details={"route": route, "dimension_positions": positions[:16]},
        )
    norm = float(np.linalg.norm(projected))
    if not math.isfinite(norm) or norm <= 0.0:
        raise error(
            E_NUMERIC,
            "Projected vector has invalid L2 norm",
            stage=stage,
            item_id=item_id,
            details={"route": route, "norm": str(norm)},
        )
    normalized = projected / norm
    if not np.all(np.isfinite(normalized)):
        raise error(
            E_NUMERIC,
            "Normalized vector contains NaN or Inf",
            stage=stage,
            item_id=item_id,
            details={"route": route},
        )
    return normalized.astype(np.float64)


def _dims_from_scales(scales: list[float], *, qmin: int, qmax: int) -> list[dict[str, Any]]:
    dims: list[dict[str, Any]] = []
    for scale in scales:
        if not math.isfinite(scale) or scale < 0:
            raise error(
                E_NUMERIC,
                "Quantization scale must be finite and non-negative",
                stage="quantization",
                details={"scale": str(scale)},
            )
        if scale == 0:
            raise error(
                E_NUMERIC,
                "Quantization scale is zero",
                stage="quantization",
                details={"scale": "0", "qmin": qmin, "qmax": qmax},
            )
        dims.append(
            {
                "scale": float(scale),
                "zero": 0,
                "quant_step": float(scale),
                "error_bound": float(scale / 2.0),
            }
        )
    return dims


def build_per_dim_spec(vectors: list[np.ndarray], *, bits: int, modality: str) -> dict[str, Any]:
    if not vectors:
        raise error(
            E_NUMERIC,
            "No vectors available for quantization calibration",
            stage="quantization",
            details={"modality": modality},
        )
    matrix = np.vstack(vectors).astype(np.float64)
    if np.any(~np.isfinite(matrix)):
        position = np.argwhere(~np.isfinite(matrix))[0].tolist()
        raise error(
            E_NUMERIC,
            "Calibration vector contains NaN or Inf",
            stage="quantization",
            details={"modality": modality, "item_index": position[0], "dimension_position": position[1]},
        )
    qmax = (1 << (bits - 1)) - 1
    qmin = -(1 << (bits - 1))
    max_abs = np.max(np.abs(matrix), axis=0)
    zero_dims = np.argwhere(max_abs == 0.0).ravel().tolist()
    if zero_dims:
        raise error(
            E_NUMERIC,
            "Zero quantization scale would be required",
            stage="quantization",
            details={"modality": modality, "dimension_positions": zero_dims[:16]},
        )
    scales = (max_abs / float(qmax)).astype(np.float64)
    dims = _dims_from_scales([float(v) for v in scales], qmin=qmin, qmax=qmax)
    error_l2 = math.sqrt(sum(float(d["error_bound"]) ** 2 for d in dims))
    return {
        "bits": bits,
        "granularity": "per_dim",
        "modality": modality,
        "dimension": len(dims),
        "qmin": qmin,
        "qmax": qmax,
        "dims": dims,
        "error_l2_bound": error_l2,
    }


def build_per_vector_spec(vector: np.ndarray, *, bits: int, modality: str) -> dict[str, Any]:
    values = np.asarray(vector, dtype=np.float64)
    if values.ndim != 1 or values.size == 0:
        raise error(
            E_NUMERIC,
            "Vector for quantization has invalid shape",
            stage="quantization",
            details={"modality": modality, "shape": list(values.shape)},
        )
    if np.any(~np.isfinite(values)):
        positions = np.argwhere(~np.isfinite(values)).ravel().tolist()
        raise error(
            E_NUMERIC,
            "Vector for quantization contains NaN or Inf",
            stage="quantization",
            details={"modality": modality, "dimension_positions": positions[:16]},
        )
    qmax = (1 << (bits - 1)) - 1
    qmin = -(1 << (bits - 1))
    max_abs = float(np.max(np.abs(values)))
    if max_abs == 0.0:
        raise error(
            E_NUMERIC,
            "Quantization scale is zero",
            stage="quantization",
            details={"modality": modality, "scale": "0"},
        )
    scale = max_abs / float(qmax)
    dims = _dims_from_scales([scale] * int(values.size), qmin=qmin, qmax=qmax)
    error_l2 = math.sqrt(sum(float(d["error_bound"]) ** 2 for d in dims))
    return {
        "bits": bits,
        "granularity": "per_vector",
        "modality": modality,
        "dimension": int(values.size),
        "qmin": qmin,
        "qmax": qmax,
        "dims": dims,
        "error_l2_bound": error_l2,
    }


def build_corpus_quantization(
    visual_vectors: list[np.ndarray],
    text_vectors: list[np.ndarray],
    *,
    bits: int,
    granularity: str,
) -> dict[str, Any]:
    if granularity == "per_dim":
        return {
            "bits": bits,
            "granularity": "per_dim",
            "visual": build_per_dim_spec(visual_vectors, bits=bits, modality="visual"),
            "text": build_per_dim_spec(text_vectors, bits=bits, modality="text"),
        }
    if granularity == "per_vector":
        return {"bits": bits, "granularity": "per_vector"}
    raise error(
        E_NUMERIC,
        "Unknown quantization granularity",
        stage="quantization",
        details={"granularity": granularity},
    )


def validate_quant_spec(
    spec: dict[str, Any],
    *,
    item_id: str | None = None,
    stage: str = "quantization",
) -> None:
    dims = spec.get("dims")
    if not isinstance(dims, list) or not dims:
        raise error(
            E_NUMERIC,
            "Quantization specification has no dimensions",
            stage=stage,
            item_id=item_id,
            details={"modality": spec.get("modality")},
        )
    for index, dim in enumerate(dims):
        scale = dim.get("scale")
        if not isinstance(scale, (int, float)) or not math.isfinite(float(scale)) or float(scale) < 0:
            raise error(
                E_NUMERIC,
                "Quantization scale is invalid",
                stage=stage,
                item_id=item_id,
                details={
                    "modality": spec.get("modality"),
                    "dimension_position": index,
                    "scale": str(scale),
                },
            )
        if float(scale) == 0.0:
            raise error(
                E_NUMERIC,
                "Quantization scale is zero",
                stage=stage,
                item_id=item_id,
                details={
                    "modality": spec.get("modality"),
                    "dimension_position": index,
                    "scale": "0",
                },
            )


def quantize_vector(
    vector: np.ndarray,
    spec: dict[str, Any],
    *,
    item_id: str | None = None,
    stage: str = "quantization",
) -> list[int]:
    values = np.asarray(vector, dtype=np.float64)
    validate_quant_spec(spec, item_id=item_id, stage=stage)
    dims = spec["dims"]
    if values.ndim != 1 or values.size != len(dims):
        raise error(
            E_DIM_MISMATCH,
            "Vector dimension does not match quantization specification",
            stage=stage,
            item_id=item_id,
            details={"expected": len(dims), "actual": list(values.shape)},
        )
    if np.any(~np.isfinite(values)):
        positions = np.argwhere(~np.isfinite(values)).ravel().tolist()
        raise error(
            E_NUMERIC,
            "Vector contains NaN or Inf before quantization",
            stage=stage,
            item_id=item_id,
            details={"dimension_positions": positions[:16]},
        )
    scales = np.asarray([float(dim["scale"]) for dim in dims], dtype=np.float64)
    zeros = np.asarray([int(dim.get("zero", 0)) for dim in dims], dtype=np.float64)
    qmin = int(spec["qmin"])
    qmax = int(spec["qmax"])
    quantized = np.rint(values / scales + zeros)
    quantized = np.clip(quantized, qmin, qmax).astype(np.int64)
    return [int(v) for v in quantized.tolist()]


def dequantize_vector(quantized: Iterable[int], spec: dict[str, Any]) -> np.ndarray:
    values = np.asarray(list(quantized), dtype=np.float64)
    dims = spec["dims"]
    if values.size != len(dims):
        raise error(
            E_DIM_MISMATCH,
            "Quantized vector dimension does not match specification",
            stage="dequantization",
            details={"expected": len(dims), "actual": int(values.size)},
        )
    scales = np.asarray([float(dim["scale"]) for dim in dims], dtype=np.float64)
    zeros = np.asarray([int(dim.get("zero", 0)) for dim in dims], dtype=np.float64)
    return (values - zeros) * scales


def vector_error_envelope(
    reference_vector: np.ndarray,
    dequantized_vector: np.ndarray,
    spec: dict[str, Any],
    *,
    roundoff_margin: float = 1e-12,
) -> float:
    """Explicit L2 upper envelope including out-of-calibration clipping.

    The half-step term is the rounding bound. The residual term is required for
    query vectors outside the corpus calibration interval, where clamping can be
    larger than half a step. Both terms use recorded scales/steps only.
    """
    reference = np.asarray(reference_vector, dtype=np.float64)
    dequantized = np.asarray(dequantized_vector, dtype=np.float64)
    if reference.shape != dequantized.shape:
        raise error(
            E_DIM_MISMATCH,
            "Reference and dequantized vectors have different shapes",
            stage="diagnostics",
            details={"reference": list(reference.shape), "dequantized": list(dequantized.shape)},
        )
    dims = spec["dims"]
    if reference.size != len(dims):
        raise error(
            E_DIM_MISMATCH,
            "Vector dimension does not match recorded quantization dimensions",
            stage="diagnostics",
            details={"expected": len(dims), "actual": int(reference.size)},
        )
    step_bounds = np.asarray([float(dim["error_bound"]) for dim in dims], dtype=np.float64)
    residuals = np.abs(reference - dequantized)
    per_dim = np.maximum(step_bounds, residuals)
    return float(np.sqrt(np.sum(per_dim * per_dim, dtype=np.float64))) + roundoff_margin


def quantization_for_query(
    reference_vector: np.ndarray,
    corpus_quantization: dict[str, Any],
    route: str,
    *,
    item_id: str | None = None,
) -> dict[str, Any]:
    if corpus_quantization["granularity"] == "per_dim":
        return corpus_quantization[route]
    return build_per_vector_spec(reference_vector, bits=int(corpus_quantization["bits"]), modality=route)


def decimal_vector(vector: Iterable[float]) -> list[str]:
    return [format(float(v), ".17g") for v in vector]


def decimal_cosine(left: Iterable[str], right: Iterable[str]) -> Decimal:
    left_values = [Decimal(v) for v in left]
    right_values = [Decimal(v) for v in right]
    if len(left_values) != len(right_values):
        raise error(
            E_DIM_MISMATCH,
            "High-precision reference vectors have different dimensions",
            stage="diagnostics",
            details={"left": len(left_values), "right": len(right_values)},
        )
    dot = sum((a * b for a, b in zip(left_values, right_values)), DECIMAL_ZERO)
    norm_left = sum((a * a for a in left_values), DECIMAL_ZERO).sqrt()
    norm_right = sum((b * b for b in right_values), DECIMAL_ZERO).sqrt()
    if norm_left == 0 or norm_right == 0:
        raise error(
            E_NUMERIC,
            "High-precision reference vector has zero norm",
            stage="diagnostics",
            details={},
        )
    return dot / (norm_left * norm_right)


def similarity_error_bound_decimal(
    query_error_l2_bound: float,
    target_error_l2_bound: float,
    reference_cosine: Decimal,
) -> Decimal:
    alpha_query = Decimal(str(query_error_l2_bound))
    alpha_target = Decimal(str(target_error_l2_bound))
    if alpha_query < 0 or alpha_target < 0:
        raise error(
            E_NUMERIC,
            "Quantization error bound is negative",
            stage="diagnostics",
            details={"query": str(alpha_query), "target": str(alpha_target)},
        )
    combined = alpha_query + alpha_target + alpha_query * alpha_target
    denominator_floor = (Decimal(1) - alpha_query) * (Decimal(1) - alpha_target)
    if denominator_floor <= 0:
        denominator_floor = Decimal("1e-30")
    bound = (combined + abs(reference_cosine) * combined) / denominator_floor
    return bound + ROUNDOFF_MARGIN


def effective_digits(reference_cosine: Decimal, error_bound: Decimal) -> int:
    if error_bound <= 0:
        return 15
    scale = abs(reference_cosine) if reference_cosine != 0 else Decimal(1)
    ratio = scale / error_bound
    if ratio <= 1:
        return 0
    return max(0, min(15, int(ratio.log10())))


def decimal_to_float(value: Decimal) -> float:
    return float(value)


def decimal_text(value: Decimal, digits: int = 21) -> str:
    if value == 0:
        return "0"
    return format(value, f".{digits}g")

