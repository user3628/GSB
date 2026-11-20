"""Exact integer arithmetic helpers for the cost ledger.

No monetary decision is made through binary floating point.  Ratios are only
rounded for presentation; comparisons use exact cross multiplication.
"""

from __future__ import annotations


SCALE = 1_000_000
PPM = 1_000_000


def round_half_up_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if remainder * 2 >= denominator:
        quotient += 1
    return sign * quotient


def format_fixed(value: int, places: int = 6, scale: int = SCALE) -> str:
    """Format an integer scaled by ``scale`` without binary floating point."""

    if places != 6 or scale != SCALE:
        raise ValueError("this ledger uses a fixed scale of 1,000,000")
    sign = "-" if value < 0 else ""
    absolute = abs(value)
    whole, fraction = divmod(absolute, scale)
    return f"{sign}{whole}.{fraction:06d}"


def format_cents(cents: int) -> str:
    return format_fixed(cents * SCALE, places=6, scale=SCALE)


def format_ppm(ppm: int) -> str:
    return format_fixed(ppm, places=6, scale=PPM)


def relative_exceeds(
    actual_numerator: int,
    actual_denominator: int,
    limit_numerator: int,
    limit_denominator: int,
    tolerance_ppm: int,
) -> bool:
    """Return whether actual > limit * (1 + tolerance), exactly.

    ``tolerance_ppm`` is an integer number of millionths.  The comparison is a
    cross multiplication, so it remains stable at a one-unit boundary.
    """

    if actual_denominator <= 0 or limit_denominator <= 0:
        raise ValueError("comparison denominators must be positive")
    if tolerance_ppm < 0:
        raise ValueError("tolerance_ppm must be non-negative")
    left = actual_numerator * limit_denominator * PPM
    right = limit_numerator * actual_denominator * (PPM + tolerance_ppm)
    return left > right


def raw_exceeds(
    actual_numerator: int,
    actual_denominator: int,
    limit_numerator: int,
    limit_denominator: int,
) -> bool:
    return actual_numerator * limit_denominator > limit_numerator * actual_denominator


def exact_ratio_text(numerator: int, denominator: int) -> str:
    return f"{numerator}/{denominator}"

