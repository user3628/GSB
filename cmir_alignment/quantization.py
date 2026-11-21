"""Fixed-point calibration, quantization, and explicit error bookkeeping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .canonical import digest_obj
from .configs import QuantizationConfig
from .errors import (
    CMIRError,
    E_QUANTIZATION_ZERO_SCALE,
    numeric_error,
)


@dataclass
class Calibration:
    modality: str
    bits: int
    granularity: str
    dim: int
    item_ids: list[str]
    mins: np.ndarray
    maxs: np.ndarray
    scales: np.ndarray
    zero_points: np.ndarray

    @property
    def qmin(self) -> int:
        return 0

    @property
    def qmax(self) -> int:
        return (1 << self.bits) - 1

    def row_for(self, index: int) -> int:
        return 0 if self.granularity == "per_dim" else index

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "cmir.quant-calibration.v1",
            "modality": self.modality,
            "bits": self.bits,
            "granularity": self.granularity,
            "dim": self.dim,
            "item_ids": self.item_ids,
            "qmin": self.qmin,
            "qmax": self.qmax,
            "mins": self.mins.tolist(),
            "maxs": self.maxs.tolist(),
            "scales": self.scales.tolist(),
            "zero_points": self.zero_points.tolist(),
        }

    def digest(self) -> str:
        return digest_obj(self.to_dict())

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Calibration":
        return cls(
            modality=str(value["modality"]),
            bits=int(value["bits"]),
            granularity=str(value["granularity"]),
            dim=int(value["dim"]),
            item_ids=[str(item) for item in value["item_ids"]],
            mins=np.asarray(value["mins"], dtype=np.float64),
            maxs=np.asarray(value["maxs"], dtype=np.float64),
            scales=np.asarray(value["scales"], dtype=np.float64),
            zero_points=np.asarray(value["zero_points"], dtype=np.int64),
        )


def _validate_vectors(
    vectors: list[tuple[str, np.ndarray]], *, modality: str
) -> tuple[list[str], np.ndarray]:
    if not vectors:
        raise CMIRError(
            E_QUANTIZATION_ZERO_SCALE,
            "cannot calibrate quantization without vectors",
            item_id="__global__",
            modality=modality,
            stage="quantization_calibration",
        )
    item_ids: list[str] = []
    rows: list[np.ndarray] = []
    expected_dim: int | None = None
    for item_id, raw in vectors:
        array = np.asarray(raw, dtype=np.float64)
        if array.ndim != 1:
            raise numeric_error(
                item_id,
                "quantization_calibration",
                "vector must be one-dimensional",
                modality=modality,
                shape=list(array.shape),
            )
        if expected_dim is None:
            expected_dim = int(array.size)
        if array.size != expected_dim:
            raise numeric_error(
                item_id,
                "quantization_calibration",
                "vector dimension mismatch",
                modality=modality,
                expected_dim=expected_dim,
                actual_dim=int(array.size),
            )
        if not np.all(np.isfinite(array)):
            raise numeric_error(
                item_id,
                "quantization_calibration",
                "vector contains NaN or Inf",
                modality=modality,
            )
        item_ids.append(item_id)
        rows.append(array)
    return item_ids, np.stack(rows, axis=0)


def fit_calibration(
    vectors: list[tuple[str, np.ndarray]],
    config: QuantizationConfig,
    *,
    modality: str,
) -> Calibration:
    item_ids, matrix = _validate_vectors(vectors, modality=modality)
    dim = int(matrix.shape[1])
    if config.granularity == "per_dim":
        mins = matrix.min(axis=0, keepdims=True)
        maxs = matrix.max(axis=0, keepdims=True)
    else:
        mins = matrix.min(axis=1, keepdims=True)
        maxs = matrix.max(axis=1, keepdims=True)
    levels = float(config.levels)
    ranges = maxs - mins
    scales = ranges / levels
    zero_rows, zero_dims = np.where(scales <= 0.0)
    if zero_rows.size:
        if config.granularity == "per_dim":
            item_id = item_ids[0]
        else:
            item_id = item_ids[int(zero_rows[0])]
        raise CMIRError(
            E_QUANTIZATION_ZERO_SCALE,
            "quantization scale is zero; refusing to quantize",
            item_id=item_id,
            stage="quantization_calibration",
            modality=modality,
            granularity=config.granularity,
            dim=int(zero_dims[0]),
            scales=scales.reshape(-1).tolist(),
        )
    # Use the widest no-clip affine interval whose zero point remains representable.
    # For strictly positive ranges, zp=0 and scale=max/L; for strictly negative ranges,
    # zp=L and scale=-min/L. Ranges crossing zero use the full min/max span.
    positive_only = mins >= 0.0
    negative_only = maxs <= 0.0
    scales = np.where(positive_only, maxs / levels, scales)
    scales = np.where(negative_only, -mins / levels, scales)
    zero_points = np.rint(-mins / scales).astype(np.int64)
    zero_points = np.where(positive_only, 0, zero_points)
    zero_points = np.where(negative_only, config.levels, zero_points)
    zero_points = np.clip(zero_points, 0, config.levels)
    return Calibration(
        modality=modality,
        bits=config.bits,
        granularity=config.granularity,
        dim=dim,
        item_ids=item_ids,
        mins=mins,
        maxs=maxs,
        scales=scales,
        zero_points=zero_points,
    )


def quantize_vector(
    vector: np.ndarray,
    calibration: Calibration,
    *,
    row_index: int,
    item_id: str,
    component: str,
    record_dtype: str = "int16",
) -> dict[str, Any]:
    reference = np.asarray(vector, dtype=np.float64)
    if reference.ndim != 1 or reference.size != calibration.dim:
        raise numeric_error(
            item_id,
            "projection_quantization",
            "vector dimension does not match calibration",
            component=component,
            expected_dim=calibration.dim,
            actual_dim=list(reference.shape),
        )
    if not np.all(np.isfinite(reference)):
        raise numeric_error(
            item_id,
            "projection_quantization",
            "vector contains NaN or Inf",
            component=component,
        )
    calibration_row = calibration.row_for(row_index)
    scales = calibration.scales[calibration_row]
    zero_points = calibration.zero_points[calibration_row]
    zero_dims = np.where(scales <= 0.0)[0]
    if zero_dims.size:
        raise CMIRError(
            E_QUANTIZATION_ZERO_SCALE,
            "quantization scale is zero; refusing to quantize",
            item_id=item_id,
            stage="projection_quantization",
            component=component,
            dim=int(zero_dims[0]),
        )
    raw_codes = np.rint(reference / scales + zero_points)
    clipped_codes = np.clip(raw_codes, calibration.qmin, calibration.qmax)
    codes = clipped_codes.astype(np.int64)
    dequantized = (codes.astype(np.float64) - zero_points) * scales
    actual_error = np.abs(reference - dequantized)
    half_step = 0.5 * scales
    component_bounds = np.maximum(half_step, actual_error)
    clipped_mask = (raw_codes < calibration.qmin) | (raw_codes > calibration.qmax)
    clipped_dims = np.where(clipped_mask)[0].astype(int).tolist()
    step_error_l2 = float(np.linalg.norm(half_step))
    checked_error_l2 = float(np.linalg.norm(component_bounds))
    actual_error_l2 = float(np.linalg.norm(actual_error))
    dequant_norm = float(np.linalg.norm(dequantized))
    if not all(
        np.isfinite(value)
        for value in (step_error_l2, checked_error_l2, actual_error_l2, dequant_norm)
    ):
        raise numeric_error(
            item_id,
            "projection_quantization",
            "quantization diagnostics contain NaN or Inf",
            component=component,
        )
    return {
        "item_id": item_id,
        "component": component,
        "dim": calibration.dim,
        "bits": calibration.bits,
        "granularity": calibration.granularity,
        "qmin": calibration.qmin,
        "qmax": calibration.qmax,
        "codes": codes.astype(np.dtype(record_dtype)).tolist(),
        "scales": scales.tolist(),
        "zero_points": zero_points.astype(np.int64).tolist(),
        "error_bounds": component_bounds.tolist(),
        "reference": reference.tolist(),
        "dequantized": dequantized.tolist(),
        "dequantized_norm": dequant_norm,
        "step_error_l2": step_error_l2,
        "checked_error_l2": checked_error_l2,
        "actual_error_l2": actual_error_l2,
        "clipped_dims": clipped_dims,
        "calibration_digest": calibration.digest(),
    }


def cosine_similarity(left: np.ndarray, right: np.ndarray) -> float:
    left_array = np.asarray(left, dtype=np.float64)
    right_array = np.asarray(right, dtype=np.float64)
    left_norm = float(np.linalg.norm(left_array))
    right_norm = float(np.linalg.norm(right_array))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return float("nan")
    return float(np.dot(left_array, right_array) / (left_norm * right_norm))


def normalized_perturbation_bounds(record: dict[str, Any]) -> tuple[float, float]:
    """Return (reference-checked, worst-case-step) normalized perturbations.

    The reference-checked value is the exact distance between the unit high-precision
    reference and the unit-normalized dequantized vector. The step value is the
    conservative scale/step-only bound 2*e/||v|| for ||u-v|| <= e.
    """
    reference = np.asarray(record["reference"], dtype=np.float64)
    dequantized = np.asarray(record["dequantized"], dtype=np.float64)
    norm = float(np.linalg.norm(dequantized))
    if norm <= 0.0:
        return float("inf"), float("inf")
    normalized = dequantized / norm
    checked = float(np.linalg.norm(reference - normalized))
    error_l2 = max(
        float(record.get("checked_error_l2", 0.0)),
        float(record.get("actual_error_l2", 0.0)),
    )
    step = 2.0 * error_l2 / norm
    return checked, step


def normalized_cosine_error_bound(
    left_record: dict[str, Any], right_record: dict[str, Any]
) -> float:
    """Reference-checked cosine difference bound for two unit reference vectors.

    If uq, ux are unit references and vq, vx are separately unit-normalized dequantized
    vectors, then |uq.ux - vq.vx| <= ||uq-vq|| + ||ux-vx||. Both distances are computed
    explicitly against local high-precision references.
    """
    left_checked, _ = normalized_perturbation_bounds(left_record)
    right_checked, _ = normalized_perturbation_bounds(right_record)
    return left_checked + right_checked + 1e-15


def conservative_step_cosine_error_bound(
    left_record: dict[str, Any], right_record: dict[str, Any]
) -> float:
    """Scale/step-only worst-case cosine bound, kept separate for auditability."""
    _, left_step = normalized_perturbation_bounds(left_record)
    _, right_step = normalized_perturbation_bounds(right_record)
    return left_step + right_step


def effective_digits(error_bound: float) -> int:
    if not np.isfinite(error_bound) or error_bound <= 0.0:
        return 15
    return max(0, int(np.floor(-np.log10(error_bound))))


def query_calibration(
    stored: Calibration,
    query_vector: np.ndarray,
    *,
    item_id: str,
) -> Calibration:
    if stored.granularity == "per_dim":
        return stored
    return fit_calibration(
        [(item_id, query_vector)],
        QuantizationConfig(bits=stored.bits, granularity="per_vector"),
        modality=stored.modality,
    )
