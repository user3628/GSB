"""Declared-precision fixed-point arithmetic and compensated summation.

Canonical values are integers in units of 1e-9 (``Q``).  No canonical path
uses binary floating point.  Floats are used only by explicit audit routines
which are labelled as non-canonical in their output.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
from typing import Iterable, Mapping

SCALE = 1_000_000_000
FRACTIONAL_DIGITS = 9
ROUNDING_RULE = "ROUND_HALF_EVEN"
SUMMATION_RULE = "Kahan compensated summation over integer Q units using Decimal(60)"


def q_to_decimal(q: int) -> Decimal:
    return Decimal(q) / Decimal(SCALE)


def q_to_string(q: int) -> str:
    """Render a Q value with exactly nine fractional digits."""

    sign = "-" if q < 0 else ""
    magnitude = abs(q)
    whole, fraction = divmod(magnitude, SCALE)
    return f"{sign}{whole}.{fraction:0{FRACTIONAL_DIGITS}d}"


def decimal_to_q(value: Decimal | str | int | float) -> tuple[int, Decimal]:
    """Quantize an input ratio to Q units.

    Returns ``(q, error_in_q_units)`` where error is the exact scaled input
    minus the selected integer.  Input floats are converted through ``str`` so
    JSON's decimal spelling is preserved as far as the caller supplied it.
    """

    with localcontext() as ctx:
        ctx.prec = 60
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        if not decimal_value.is_finite():
            raise ValueError("value must be finite")
        scaled = decimal_value * SCALE
        q = int(scaled.to_integral_value(rounding=ROUND_HALF_EVEN))
        return q, scaled - Decimal(q)


def q_mul(a: int, b: int) -> tuple[int, Fraction]:
    """Multiply two Q values using half-even rounding at the final step."""

    numerator = a * b
    q = round_fraction_half_even(numerator, SCALE)
    return q, Fraction(numerator, SCALE) - q


def q_div(a: int, b: int) -> tuple[int, Fraction]:
    """Divide two Q values using half-even rounding at the final step."""

    if b == 0:
        return 0, Fraction(0)
    numerator = a * SCALE
    q = round_fraction_half_even(numerator, b)
    return q, Fraction(numerator, b) - q


def q_div_int(numerator: int, denominator: int) -> tuple[int, Fraction]:
    """Convert an integer numerator/denominator ratio to Q units."""

    if denominator == 0:
        return 0, Fraction(0)
    scaled = numerator * SCALE
    q = round_fraction_half_even(scaled, denominator)
    return q, Fraction(scaled, denominator) - q


def round_fraction_half_even(numerator: int, denominator: int) -> int:
    """Round an exact rational to an integer with ties-to-even."""

    if denominator == 0:
        raise ZeroDivisionError("denominator is zero")
    if denominator < 0:
        numerator, denominator = -numerator, -denominator
    sign = -1 if numerator < 0 else 1
    magnitude = abs(numerator)
    quotient, remainder = divmod(magnitude, denominator)
    twice = remainder * 2
    if twice > denominator or (twice == denominator and quotient % 2 == 1):
        quotient += 1
    return sign * quotient


def normalize_integer_weights(
    weights: Mapping[str, int], *, trace_name: str = "normalization"
) -> tuple[dict[str, int], dict[str, object]]:
    """Normalize non-negative integer weights to Q exactly.

    The Hamilton/largest-remainder method is used.  The residual one-unit
    allocations are ordered by ``(remainder desc, holder_id asc)``, which makes
    the result independent of mapping insertion order.
    """

    if not weights:
        return {}, {
            "name": trace_name,
            "rule": "largest remainder; remainder desc; holder_id asc",
            "items": [],
            "cumulative_abs_rounding_error_q": "0.000000000",
            "exact_sum_q": 0,
        }
    if any(value < 0 for value in weights.values()):
        raise ValueError("weights must be non-negative")
    total = sum(weights.values())
    if total <= 0:
        return {holder: 0 for holder in sorted(weights)}, {
            "name": trace_name,
            "rule": "zero total weight",
            "items": [],
            "cumulative_abs_rounding_error_q": "0.000000000",
            "exact_sum_q": 0,
        }

    floors: dict[str, int] = {}
    remainders: dict[str, int] = {}
    for holder in sorted(weights):
        numerator = weights[holder] * SCALE
        floor_q, remainder = divmod(numerator, total)
        floors[holder] = floor_q
        remainders[holder] = remainder

    residual = SCALE - sum(floors.values())
    winners = sorted(remainders, key=lambda h: (-remainders[h], h))[:residual]
    normalized = {holder: floors[holder] + (1 if holder in winners else 0) for holder in sorted(weights)}

    items: list[dict[str, object]] = []
    cumulative_error = Fraction(0)
    for holder in sorted(weights):
        exact = Fraction(weights[holder] * SCALE, total)
        error = exact - normalized[holder]
        cumulative_error += abs(error)
        items.append(
            {
                "holder_id": holder,
                "weight_q": weights[holder],
                "exact_q": fraction_to_decimal_string(exact),
                "rounded_q": normalized[holder],
                "rounding_error_q": fraction_to_decimal_string(error),
                "residual_allocated": normalized[holder] - floors[holder],
            }
        )
    return normalized, {
        "name": trace_name,
        "rule": "largest remainder; remainder desc; holder_id asc",
        "total_weight_q": total,
        "items": items,
        "normalized_sum_q": sum(normalized.values()),
        "cumulative_abs_rounding_error_q": fraction_to_decimal_string(cumulative_error),
        "conservative_bound_q": str(len(weights)),
    }


@dataclass(slots=True)
class CompensatedSum:
    """Kahan compensated sum for explicit Q-unit terms."""

    total: Decimal = field(default_factory=lambda: Decimal(0))
    compensation: Decimal = field(default_factory=lambda: Decimal(0))

    def add_q(self, value_q: int) -> None:
        with localcontext() as ctx:
            ctx.prec = 60
            adjusted = Decimal(value_q) - self.compensation
            updated = self.total + adjusted
            self.compensation = (updated - self.total) - adjusted
            self.total = updated

    def add_fraction_q(self, value_q: Fraction) -> None:
        with localcontext() as ctx:
            ctx.prec = 60
            decimal_value = Decimal(value_q.numerator) / Decimal(value_q.denominator)
            adjusted = decimal_value - self.compensation
            updated = self.total + adjusted
            self.compensation = (updated - self.total) - adjusted
            self.total = updated

    def rounded_q(self) -> int:
        with localcontext() as ctx:
            ctx.prec = 60
            return int(self.total.to_integral_value(rounding=ROUND_HALF_EVEN))


def kahan_float_sum(values: Iterable[float]) -> float:
    total = 0.0
    compensation = 0.0
    for value in values:
        adjusted = value - compensation
        updated = total + adjusted
        compensation = (updated - total) - adjusted
        total = updated
    return total


def float_summation_cross_check(terms_q: Iterable[int]) -> dict[str, object]:
    """Audit canonical Q sums against naive and compensated floating sums.

    This function is deliberately outside the canonical calculation path.
    """

    terms = [int(value) for value in terms_q]
    decimal_terms = [q_to_decimal(value) for value in terms]
    canonical_q = sum(terms)
    canonical_decimal = q_to_decimal(canonical_q)
    naive_float = sum(float(value) / SCALE for value in terms)
    compensated_float = kahan_float_sum(float(value) / SCALE for value in terms)
    numpy_naive_float: float | None = None
    try:
        import numpy as np

        numpy_naive_float = float(np.sum(np.asarray(terms, dtype=np.float64) / SCALE))
    except ImportError:  # pragma: no cover - numpy is a declared dependency
        pass
    count = len(terms)
    unit_roundoff = 2.0**-53
    naive_bound = 0.0
    if count > 1 and (count - 1) * unit_roundoff < 1.0:
        gamma = ((count - 1) * unit_roundoff) / (1.0 - (count - 1) * unit_roundoff)
        naive_bound = gamma * math.fsum(abs(float(value) / SCALE) for value in terms)
    kahan_bound = 2.0 * unit_roundoff * math.fsum(abs(float(value) / SCALE) for value in terms)
    return {
        "non_canonical": True,
        "purpose": "audit only; canonical values are integer Q units",
        "term_count": count,
        "terms_q": terms,
        "canonical_decimal": q_to_string(canonical_q),
        "kahan_decimal_compensated": q_to_string(DecimalSum(decimal_terms).rounded_q()),
        "naive_float": repr(naive_float),
        "numpy_naive_float": None if numpy_naive_float is None else repr(numpy_naive_float),
        "naive_float_kahan": repr(compensated_float),
        "naive_error_q_estimate": int(round((Decimal(str(naive_float)) - canonical_decimal) * SCALE)),
        "numpy_naive_error_q_estimate": None
        if numpy_naive_float is None
        else int(round((Decimal(str(numpy_naive_float)) - canonical_decimal) * SCALE)),
        "kahan_float_error_q_estimate": int(round((Decimal(str(compensated_float)) - canonical_decimal) * SCALE)),
        "naive_absolute_error_bound_decimal": repr(naive_bound),
        "kahan_absolute_error_bound_decimal": repr(kahan_bound),
        "bound_formula": {
            "naive": "gamma_(n-1) * sum(abs(x_i)); gamma_n = n*u/(1-n*u), u=2^-53",
            "kahan": "2*u * sum(abs(x_i)) + O(n*u^2)",
            "canonical": "exact integer addition; 0 Q-unit summation error",
        },
    }


class DecimalSum:
    """Exact-decimal compensated sum used by the non-canonical cross-check."""

    def __init__(self, values: Iterable[Decimal]) -> None:
        self._sum = CompensatedSum()
        for value in values:
            self._sum.add_fraction_q(Fraction(value))

    def rounded_q(self) -> int:
        with localcontext() as ctx:
            ctx.prec = 60
            return int((self._sum.total * SCALE).to_integral_value(rounding=ROUND_HALF_EVEN))


def fraction_to_decimal_string(value: Fraction, digits: int = 9) -> str:
    """Render a rational with ``digits`` fractional places, truncated toward zero."""

    with localcontext() as ctx:
        ctx.prec = 60
        decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        quantum = Decimal(1).scaleb(-digits)
        truncated = decimal_value.quantize(quantum, rounding=ROUND_DOWN)
    return f"{truncated:.{digits}f}"

