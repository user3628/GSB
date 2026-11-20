from __future__ import annotations

import math
from typing import Any

import numpy as np

from .errors import E_NUMERIC, E_VALIDATION, WorkbenchError


def ensure_finite(array: np.ndarray, *, fragment_id: str, field: str) -> None:
    finite = np.isfinite(array)
    if finite.all():
        return
    location = np.argwhere(~finite)[0].tolist()
    if len(location) >= 2:
        y, x = int(location[0]), int(location[1])
    else:
        y = x = 0
    raise WorkbenchError(
        E_NUMERIC,
        f"{field} contains NaN or Inf",
        fragment_id=fragment_id,
        pixel=[x, y],
        details={"field": field, "value": str(array[tuple(location)])},
    )


def validate_strength(value: Any, *, fragment_id: str, field: str = "style_strength") -> float:
    try:
        strength = float(value)
    except (TypeError, ValueError) as exc:
        raise WorkbenchError(
            E_NUMERIC,
            "Style strength must be numeric",
            fragment_id=fragment_id,
            pixel=[0, 0],
            details={"field": field, "value": repr(value)},
        ) from exc
    if not math.isfinite(strength) or strength < 0.0 or strength > 1.0:
        raise WorkbenchError(
            E_NUMERIC,
            "Style strength must be finite and within [0, 1]",
            fragment_id=fragment_id,
            pixel=[0, 0],
            details={"field": field, "value": repr(value)},
        )
    return strength


def quantize_u8(array: np.ndarray, *, fragment_id: str, field: str) -> np.ndarray:
    """Validate finiteness, clamp to the 8-bit range, then round half up."""
    ensure_finite(array, fragment_id=fragment_id, field=field)
    clamped = np.clip(array, 0.0, 255.0)
    return np.floor(clamped + 0.5).astype(np.uint8)


def validate_u8(array: np.ndarray, *, fragment_id: str) -> np.ndarray:
    if array.dtype != np.uint8:
        if np.issubdtype(array.dtype, np.floating):
            ensure_finite(array, fragment_id=fragment_id, field="pixels")
        raise WorkbenchError(
            E_VALIDATION,
            "Final pixel array must be uint8",
            fragment_id=fragment_id,
            details={"dtype": str(array.dtype)},
        )
    if array.ndim != 3 or array.shape[2] != 3:
        raise WorkbenchError(
            E_VALIDATION,
            "Final pixel array must have shape (height, width, 3)",
            fragment_id=fragment_id,
        )
    return array

