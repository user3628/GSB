"""Numeric reporting helpers shared by all pipeline stages."""

from __future__ import annotations

import math

from .errors import E_NUMERIC, MatgenError


MAX_REPORTED_DIGITS = 15


def assert_finite_report_value(
    value: float,
    *,
    material_id: str,
    stage: str,
    octave_index: int | None = None,
    label: str,
) -> float:
    numeric = float(value)
    if not math.isfinite(numeric):
        raise MatgenError(
            code=E_NUMERIC,
            message=f"non-finite diagnostic value: {label}",
            material_id=material_id,
            stage=stage,
            octave_index=octave_index,
            details={"label": label, "value": repr(value)},
        )
    return numeric


def significant_digits(value: float, error_bound: float) -> int:
    """Estimate reliable significant decimal digits for an absolute error."""

    value = float(value)
    error_bound = float(error_bound)
    if not math.isfinite(value) or not math.isfinite(error_bound) or error_bound < 0.0:
        raise ValueError("value and error_bound must be finite; error_bound must be non-negative")
    if error_bound == 0.0:
        return MAX_REPORTED_DIGITS
    digits = math.floor(-math.log10(2.0 * error_bound))
    return max(0, min(MAX_REPORTED_DIGITS, int(digits)))


def safe_relative_error(value: float, reference: float) -> float:
    denominator = max(abs(float(reference)), 2.220446049250313e-16)
    return abs(float(value) - float(reference)) / denominator


def canonical_decimal_string(value: object) -> str:
    """Stable decimal text for JSON diagnostics without binary float noise."""

    return format(value, "f") if not isinstance(value, float) else repr(value)

