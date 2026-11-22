"""Exact rational helpers with explicit, deterministic rounding.

The public contract stores every threshold and derived comparison value as an
integer at a declared decimal scale.  No pass/fail decision is made from a
binary floating point value.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Any


SUPPORTED_ROUNDING = {"ROUND_HALF_EVEN", "ROUND_HALF_UP", "ROUND_DOWN", "ROUND_UP"}


def scale_for(decimals: int) -> int:
    if not isinstance(decimals, int) or isinstance(decimals, bool) or decimals < 0:
        raise ValueError("decimal places must be a non-negative integer")
    if decimals > 12:
        raise ValueError("decimal places must be <= 12")
    return 10**decimals


def parse_decimal(value: Any, field: str) -> Fraction:
    """Parse a JSON numeric value without first passing through binary float."""

    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field} must be a decimal value")
    if isinstance(value, Fraction):
        return value
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, Decimal):
        return Fraction(value)
    if isinstance(value, float):
        value = repr(value)
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a decimal string, integer or Decimal")
    text = value.strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    try:
        dec = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{field} is not a valid decimal") from exc
    if not dec.is_finite():
        raise ValueError(f"{field} must be finite")
    return Fraction(dec)


def _round_ratio(numerator: int, denominator: int, mode: str) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    if mode not in SUPPORTED_ROUNDING:
        raise ValueError(f"unsupported rounding mode: {mode}")
    sign = -1 if numerator < 0 else 1
    q, r = divmod(abs(numerator), denominator)
    if r == 0:
        return sign * q
    if mode == "ROUND_DOWN":
        return sign * q
    if mode == "ROUND_UP":
        return sign * (q + 1)
    twice = 2 * r
    if mode == "ROUND_HALF_UP":
        return sign * (q + 1 if twice >= denominator else q)
    if twice > denominator or (twice == denominator and q % 2 == 1):
        return sign * (q + 1)
    return sign * q


def quantize(value: Fraction, scale: int, rounding: str) -> int:
    """Return round(value * scale) as an integer at the declared scale."""

    if scale <= 0:
        raise ValueError("scale must be positive")
    numerator = value.numerator * scale
    return _round_ratio(numerator, value.denominator, rounding)


def round_fraction(value: Fraction, rounding: str = "ROUND_HALF_EVEN") -> int:
    return _round_ratio(value.numerator, value.denominator, rounding)


def fixed_decimal(value: int, decimals: int) -> str:
    scale = scale_for(decimals)
    sign = "-" if value < 0 else ""
    absolute = abs(value)
    whole, fraction = divmod(absolute, scale)
    if decimals == 0:
        return f"{sign}{whole}"
    return f"{sign}{whole}.{fraction:0{decimals}d}"


def fixed_value(value: int, decimals: int) -> dict[str, Any]:
    return {
        "fixed": value,
        "decimals": decimals,
        "decimal": fixed_decimal(value, decimals),
    }


def ceil_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    return -((-numerator) // denominator)


def relative_allowance(threshold_fixed: int, ppm: int) -> int:
    """Relative tolerance in scaled units, rounded upward (never weakened)."""

    if threshold_fixed < 0 or ppm < 0:
        raise ValueError("threshold and tolerance must be non-negative")
    return ceil_div(threshold_fixed * ppm, 1_000_000)


def compare_at_most(
    actual_fixed: int,
    threshold_fixed: int,
    ppm: int,
    *,
    actual_decimals: int,
    threshold_decimals: int,
) -> dict[str, Any]:
    allowance = relative_allowance(threshold_fixed, ppm)
    boundary = threshold_fixed + allowance
    passed = actual_fixed <= boundary
    return _comparison(
        "LE",
        passed,
        actual_fixed,
        threshold_fixed,
        allowance,
        boundary,
        actual_decimals,
        threshold_decimals,
        ppm,
    )


def compare_at_least(
    actual_fixed: int,
    threshold_fixed: int,
    ppm: int,
    *,
    actual_decimals: int,
    threshold_decimals: int,
) -> dict[str, Any]:
    allowance = relative_allowance(threshold_fixed, ppm)
    boundary = threshold_fixed - allowance
    passed = actual_fixed >= boundary
    return _comparison(
        "GE",
        passed,
        actual_fixed,
        threshold_fixed,
        allowance,
        boundary,
        actual_decimals,
        threshold_decimals,
        ppm,
    )


def _comparison(
    relation: str,
    passed: bool,
    actual_fixed: int,
    threshold_fixed: int,
    allowance: int,
    boundary: int,
    actual_decimals: int,
    threshold_decimals: int,
    ppm: int,
) -> dict[str, Any]:
    return {
        "relation": relation,
        "passed": passed,
        "actual": fixed_value(actual_fixed, actual_decimals),
        "threshold": fixed_value(threshold_fixed, threshold_decimals),
        "relative_tolerance_ppm": ppm,
        "allowance_fixed": allowance,
        "pass_boundary_fixed": boundary,
        "comparison": (
            f"actual_fixed({relation})pass_boundary_fixed "
            f"[{actual_fixed} {relation} {boundary}]"
        ),
    }


def ceil_percentile_min_samples(p: Fraction) -> int:
    if p <= 0:
        return 1
    if p > 1:
        raise ValueError("percentile must be in [0,1]")
    if p == 1:
        return 1
    required = Fraction(1, 1) / (1 - p)
    return max(1, ceil_div(required.numerator, required.denominator))

