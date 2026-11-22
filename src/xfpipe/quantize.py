"""Fixed-point quantisation of feature matrices.

Reduced features and cross-combination counts are persisted as fixed-point
integers together with a per-column scale factor.  Standardisation and the
projection accumulation are carried out in the scaled integer domain so the
stored artefact *is* the value used downstream (no hidden float drift), and
a deterministic error bound versus a float64 reference can be reported.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

QUANT_SCALE = 1_000_000  # 10^6 -> six decimal places of fixed-point precision


@dataclass(frozen=True)
class QuantizedMatrix:
    """A matrix stored as int64 fixed-point values plus per-column scales."""

    values: list[list[int]]  # int64 fixed-point
    scales: list[float]  # per-column scale factor
    shape: tuple[int, int]

    def to_float(self) -> np.ndarray:
        arr = np.asarray(self.values, dtype=np.float64)
        scale = np.asarray(self.scales, dtype=np.float64)
        return arr / scale

    def digest_obj(self) -> dict:
        return {
            "values": self.values,
            "scales": [float(s) for s in self.scales],
            "shape": list(self.shape),
        }


def quantize(matrix: np.ndarray, scale: float = QUANT_SCALE) -> QuantizedMatrix:
    """Round ``matrix`` to fixed-point with a shared per-column scale factor."""
    arr = np.asarray(matrix, dtype=np.float64)
    if arr.ndim != 2:
        raise ValueError("quantize expects a 2-D matrix")
    q = np.rint(arr * scale).astype(np.int64)
    scales = [float(scale)] * arr.shape[1]
    return QuantizedMatrix(
        values=[[int(v) for v in row] for row in q.tolist()],
        scales=scales,
        shape=(int(arr.shape[0]), int(arr.shape[1])),
    )


def error_bound(quantized: QuantizedMatrix, reference: np.ndarray) -> dict:
    """Compare the fixed-point artefact against a float64 reference.

    Returns the observed max/mean absolute error and a deterministic
    theoretical bound (half a quantum per cell plus float64 accumulation
    noise estimated from the matrix dimensions).
    """
    ref = np.asarray(reference, dtype=np.float64)
    approx = quantized.to_float()
    if approx.shape != ref.shape:
        raise ValueError(f"shape mismatch {approx.shape} vs {ref.shape}")
    diff = np.abs(approx - ref)
    n, k = ref.shape
    scale = quantized.scales[0] if quantized.scales else float(QUANT_SCALE)
    quantum = 0.5 / scale
    ref_norm = float(np.max(np.abs(ref))) if ref.size else 0.0
    accum_bound = float(np.finfo(np.float64).eps * max(n, 1) * max(ref_norm, 1.0) * 4)
    return {
        "max_abs_error": float(diff.max()) if diff.size else 0.0,
        "mean_abs_error": float(diff.mean()) if diff.size else 0.0,
        "quantum_bound": quantum,
        "accumulation_bound": accum_bound,
        "total_bound": quantum + accum_bound,
        "scale_per_column": [float(s) for s in quantized.scales],
    }
