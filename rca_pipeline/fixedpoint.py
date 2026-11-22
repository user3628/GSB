"""Deterministic signed fixed-point arithmetic with checked overflow.

All root-cause score math is performed on plain Python ``int`` values scaled
by :data:`rca_pipeline.config.FP_SCALE` (10**6).  Python ints cannot overflow,
but we still enforce an explicit ceiling on every arithmetic step so that a
logic bug (e.g. an unbounded hop count) fails loudly instead of silently
producing a huge score.

Rounding contract
------------------
* Multiplication of two fixed-point numbers divides by the scale using
  *round-half-to-even* (banker's rounding), the only rounding performed during
  score composition.
* Integer quantities (hops, millisecond deltas) are exact.
* Final scores are rendered with truncation to six decimal places, which is
  lossless because every stored value already has at most six fractional
  digits; no extra rounding error is introduced at the boundary.
"""
from __future__ import annotations

from .config import FP_MAX, FP_SCALE

SCALE = 10**FP_SCALE


class FixedPointError(OverflowError):
    """Raised when a checked fixed-point operation would leave the safe range."""


def _check(value: int) -> int:
    if value > FP_MAX or value < -FP_MAX:
        raise FixedPointError(f"fixed-point value {value} exceeds bound +/-{FP_MAX}")
    return value


def from_int(value: int) -> int:
    """Promote a small integer (weight, bound) to fixed-point representation."""
    return _check(int(value) * SCALE)


def from_float(value: float) -> int:
    """Convert a finite float to fixed-point using round-half-to-even."""
    # Python's built-in round() on floats implements banker's rounding.
    return _check(int(round(value * SCALE)))


def to_float(value: int) -> float:
    return value / SCALE


def to_string(value: int) -> str:
    """Render a fixed-point int with exactly ``FP_SCALE`` decimals (truncating)."""
    sign = "-" if value < 0 else ""
    magnitude = abs(value)
    whole, frac = divmod(magnitude, SCALE)
    return f"{sign}{whole}.{frac:0{FP_SCALE}d}"


def add(a: int, b: int) -> int:
    return _check(a + b)


def sub(a: int, b: int) -> int:
    return _check(a - b)


def mul(a: int, b: int) -> int:
    """Multiply two fixed-point values, round-half-to-even after rescaling."""
    product = a * b
    # Half-even rescale without floating point.
    quotient, remainder = divmod(abs(product), SCALE)
    half = SCALE // 2
    if remainder > half or (remainder == half and quotient % 2 == 1):
        quotient += 1
    result = -quotient if product < 0 else quotient
    return _check(result)


def divide_small(value: int, divisor: int) -> int:
    """Exact integer division by a small positive integer, half-to-even."""
    if divisor <= 0:
        raise FixedPointError("divisor must be positive")
    sign = -1 if value < 0 else 1
    magnitude = abs(value)
    quotient, remainder = divmod(magnitude, divisor)
    half = divisor // 2
    if remainder * 2 > divisor or (remainder * 2 == divisor and quotient % 2 == 1):
        quotient += 1
    return _check(sign * quotient)


def clamp(value: int, low: int = 0, high: int | None = None) -> int:
    high = SCALE if high is None else high
    return max(low, min(value, high))


def nearly_equal(a: int, b: int, rel_tol: int) -> bool:
    """Fixed-point relative-tolerance comparison.

    ``rel_tol`` is itself a fixed-point fraction (e.g. 1000 = 1e-3).  Two
    values are considered equal when ``|a-b| <= rel_tol * max(|a|, |b|)``.
    """
    if a == b:
        return True
    diff = abs(a - b)
    tolerance = mul(rel_tol, max(abs(a), abs(b)))
    return diff <= tolerance
