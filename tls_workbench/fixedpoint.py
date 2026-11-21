"""Deterministic fixed-point numeric contract.

All business scores, margin factors and canonical budgets are represented as
Python ``int`` values scaled by :data:`SCALE`. Python integer arithmetic is
exact on every platform, so repeated runs and different process invocation
orders produce bit-for-bit identical results.

Rounding rule for every division/multiplication that cannot be represented
exactly at the fixed-point scale is banker's rounding (round half to even),
applied on the exact rational value before truncation.
"""

from __future__ import annotations

SCALE = 1_000_000_000
DECIMALS = 9
ROUNDING = "ROUND_HALF_EVEN"


def from_int(value: int) -> int:
    return int(value) * SCALE


def _round_half_even(numerator: int, denominator: int) -> int:
    if denominator == 0:
        raise ZeroDivisionError("fixed-point division by zero")
    negative = (numerator < 0) ^ (denominator < 0)
    num = abs(numerator)
    den = abs(denominator)
    quotient, remainder = divmod(num, den)
    if remainder * 2 > den or (remainder * 2 == den and quotient % 2 == 1):
        quotient += 1
    return -quotient if negative else quotient


def from_fraction(numerator: int, denominator: int) -> int:
    """Exact rational -> fixed point, rounded half to even."""
    return _round_half_even(numerator * SCALE, denominator)


def add(left: int, right: int) -> int:
    return left + right


def sub(left: int, right: int) -> int:
    return left - right


def neg(value: int) -> int:
    return -value


def mul(left: int, right: int) -> int:
    """Fixed-point multiplication with explicit half-even rescaling."""
    return _round_half_even(left * right, SCALE)


def div(left: int, right: int) -> int:
    """Fixed-point division with explicit half-even rescaling."""
    return _round_half_even(left * SCALE, right)


def to_float(value: int) -> float:
    """For display only; never feed back into business comparisons."""
    return value / SCALE


def to_str(value: int) -> str:
    sign = "-" if value < 0 else ""
    whole, fraction = divmod(abs(value), SCALE)
    text = f"{sign}{whole}.{str(fraction).zfill(DECIMALS)}"
    return text


def clamp(value: int, lower: int, upper: int) -> int:
    if lower > upper:
        raise ValueError("clamp lower bound exceeds upper bound")
    return max(lower, min(upper, value))


def within_relative_tolerance(left: int, right: int, rel_tol: int) -> bool:
    """Fixed-point relative tolerance comparison.

    ``rel_tol`` is itself a fixed-point value (e.g. 1e-6 => from_fraction(1,
    1_000_000)). The tolerance is computed against the larger magnitude so
    the test is symmetric.
    """
    difference = abs(left - right)
    magnitude = max(abs(left), abs(right))
    if magnitude == 0:
        return difference == 0
    tolerance = mul(magnitude, rel_tol)
    return difference <= tolerance
