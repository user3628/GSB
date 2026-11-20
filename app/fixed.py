from __future__ import annotations

import sys
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Iterable, Sequence


SCALE = 1_000_000
DECIMAL_PLACES = 6
ROUNDING_MODE = "ROUND_HALF_EVEN"


def round_half_even_fraction(numerator: int, denominator: int) -> int:
    """Round an exact rational to the nearest integer, ties to even."""
    if denominator == 0:
        raise ZeroDivisionError("cannot round a fraction with zero denominator")
    if denominator < 0:
        numerator, denominator = -numerator, -denominator
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    doubled = remainder * 2
    if doubled > denominator or (doubled == denominator and quotient % 2 == 1):
        quotient += 1
    return sign * quotient


def integer_ceil_fraction(value: Fraction) -> int:
    return -((-value.numerator) // value.denominator)


def format_fixed(micros: int) -> str:
    sign = "-" if micros < 0 else ""
    absolute = abs(micros)
    whole, fraction = divmod(absolute, SCALE)
    return f"{sign}{whole}.{fraction:0{DECIMAL_PLACES}d}"


def format_error_bound(error: Fraction) -> str:
    return format_fixed(integer_ceil_fraction(error))


@dataclass(frozen=True)
class TrackedFixed:
    """A fixed-point value with a conservative absolute error bound in micros."""

    micros: int
    error: Fraction = Fraction(0)

    @property
    def error_micros(self) -> int:
        return integer_ceil_fraction(self.error)


def fixed_from_int(value: int) -> TrackedFixed:
    return TrackedFixed(value * SCALE, Fraction(0))


def fixed_from_decimal(value: Decimal) -> TrackedFixed:
    exact = Fraction(value) * SCALE
    rounded = round_half_even_fraction(exact.numerator, exact.denominator)
    return TrackedFixed(rounded, abs(Fraction(rounded) - exact))


def fixed_zero() -> TrackedFixed:
    return TrackedFixed(0, Fraction(0))


def fixed_one() -> TrackedFixed:
    return TrackedFixed(SCALE, Fraction(0))


def add_fixed(left: TrackedFixed, right: TrackedFixed) -> TrackedFixed:
    return TrackedFixed(left.micros + right.micros, left.error + right.error)


def sub_fixed(left: TrackedFixed, right: TrackedFixed) -> TrackedFixed:
    return TrackedFixed(left.micros - right.micros, left.error + right.error)


def abs_fixed(value: TrackedFixed) -> TrackedFixed:
    return TrackedFixed(abs(value.micros), value.error)


def neg_fixed(value: TrackedFixed) -> TrackedFixed:
    return TrackedFixed(-value.micros, value.error)


def div_to_dimensionless(numerator: TrackedFixed, denominator: TrackedFixed) -> TrackedFixed:
    """Compute numerator/denominator as a fixed-point dimensionless value."""
    if denominator.micros == 0:
        raise ZeroDivisionError("fixed-point denominator is zero")
    denominator_abs = abs(denominator.micros)
    if denominator.error >= denominator_abs:
        raise ArithmeticError("denominator error bound reaches zero")

    exact = Fraction(numerator.micros * SCALE, denominator.micros)
    rounded = round_half_even_fraction(exact.numerator, exact.denominator)
    rounding_error = abs(Fraction(rounded) - exact)

    denominator_sq = denominator_abs * denominator_abs
    first_order = (
        numerator.error * SCALE / denominator_abs
        + abs(numerator.micros) * SCALE * denominator.error / denominator_sq
    )
    second_order = numerator.error * SCALE * denominator.error / denominator_sq
    return TrackedFixed(rounded, rounding_error + first_order + second_order)


def mul_dimensionless(left: TrackedFixed, right: TrackedFixed) -> TrackedFixed:
    """Multiply two dimensionless fixed-point values and return one fixed value."""
    exact = Fraction(left.micros * right.micros, SCALE)
    rounded = round_half_even_fraction(exact.numerator, exact.denominator)
    rounding_error = abs(Fraction(rounded) - exact)

    propagation = (
        left.error * abs(right.micros) / SCALE
        + right.error * abs(left.micros) / SCALE
        + left.error * right.error / SCALE
    )
    return TrackedFixed(rounded, rounding_error + propagation)


def clamp_unit(value: TrackedFixed) -> TrackedFixed:
    if value.micros < 0:
        return TrackedFixed(0, value.error)
    if value.micros > SCALE:
        return TrackedFixed(SCALE, value.error)
    return value


@dataclass(frozen=True)
class FixedSum:
    total_micros: int
    compensation_micros: int
    error: Fraction
    term_count: int


def kahan_sum_fixed(values: Sequence[TrackedFixed]) -> FixedSum:
    """Compensated summation over exact integer micros.

    Integer additions are exact in Python, so the compensated residual remains
    zero. Keeping Kahan's compensation term makes the invariant explicit and
    prevents a future change to an inexact accumulator from silently changing
    decisions.
    """
    total = 0
    compensation = 0
    error = Fraction(0)
    for value in values:
        adjusted = value.micros - compensation
        next_total = total + adjusted
        compensation = (next_total - total) - adjusted
        total = next_total
        error += value.error
    return FixedSum(total, compensation, error, len(values))


def naive_sum_fixed(values: Sequence[TrackedFixed]) -> FixedSum:
    total = 0
    error = Fraction(0)
    for value in values:
        total += value.micros
        error += value.error
    return FixedSum(total, 0, error, len(values))


def relative_tolerance_micros(reference_micros: int, value_micros: int, relative_tolerance_micros: int) -> int:
    """Return ceil(rel_tol * max(|reference|, |value|, 1.0)) in micros."""
    magnitude = max(abs(reference_micros), abs(value_micros), SCALE)
    exact = Fraction(relative_tolerance_micros * magnitude, SCALE)
    return integer_ceil_fraction(exact)


def compare_ge(value_micros: int, lower_bound_micros: int, rel_tol_micros: int) -> bool:
    tolerance = relative_tolerance_micros(lower_bound_micros, value_micros, rel_tol_micros)
    return value_micros >= lower_bound_micros - tolerance


def compare_le(value_micros: int, upper_bound_micros: int, rel_tol_micros: int) -> bool:
    tolerance = relative_tolerance_micros(upper_bound_micros, value_micros, rel_tol_micros)
    return value_micros <= upper_bound_micros + tolerance


def compare_in_interval(
    value_micros: int,
    lower_bound_micros: int,
    upper_bound_micros: int,
    rel_tol_micros: int,
) -> tuple[bool, int, int]:
    lower_tolerance = relative_tolerance_micros(lower_bound_micros, value_micros, rel_tol_micros)
    upper_tolerance = relative_tolerance_micros(upper_bound_micros, value_micros, rel_tol_micros)
    effective_lower = lower_bound_micros - lower_tolerance
    effective_upper = upper_bound_micros + upper_tolerance
    return effective_lower <= value_micros <= effective_upper, effective_lower, effective_upper


def fraction_from_decimal_string(value: str) -> Fraction:
    return Fraction(Decimal(value))


def exact_float_error(value: float, exact: Fraction) -> Fraction:
    return abs(Fraction.from_float(value) - exact)


def naive_float_sum(values: Iterable[Decimal]) -> float:
    total = 0.0
    for value in values:
        total += float(value)
    return total


def kahan_float_sum(values: Iterable[Decimal]) -> float:
    total = 0.0
    compensation = 0.0
    for value in values:
        adjusted = float(value) - compensation
        next_total = total + adjusted
        compensation = (next_total - total) - adjusted
        total = next_total
    return total


def float_sum_error_bound(sum_abs_micros: int, term_count: int) -> Fraction:
    """Conservative gamma_(n-1) * sum(|x|) binary64 rounding bound in micros."""
    if term_count <= 1:
        return Fraction(0)
    n = term_count - 1
    epsilon = Fraction(sys.float_info.epsilon)
    gamma = Fraction(n) * epsilon / (1 - Fraction(n) * epsilon)
    return gamma * sum_abs_micros


def format_float_units_as_fixed(value: float) -> str:
    exact_micros = Fraction.from_float(value) * SCALE
    return format_fixed(round_half_even_fraction(exact_micros.numerator, exact_micros.denominator))


def mixed_sum_diagnostics(values: Sequence[Decimal]) -> dict[str, object]:
    exact = sum((Fraction(value) for value in values), Fraction(0))
    micros = [round_half_even_fraction((Fraction(value) * SCALE).numerator, (Fraction(value) * SCALE).denominator) for value in values]
    fixed_kahan = kahan_sum_fixed([TrackedFixed(value, Fraction(0)) for value in micros])
    fixed_naive = naive_sum_fixed([TrackedFixed(value, Fraction(0)) for value in micros])

    naive_float = naive_float_sum(values)
    kahan_float = kahan_float_sum(values)
    naive_error = exact_float_error(naive_float, exact) * SCALE
    kahan_error = exact_float_error(kahan_float, exact) * SCALE
    sum_abs_micros = sum(abs(value) for value in micros)
    bound = float_sum_error_bound(sum_abs_micros, len(values))
    kahan_bound = bound * 2
    exact_micros = exact * SCALE

    return {
        "terms": [format_fixed(value) for value in micros],
        "term_count": len(values),
        "exact_sum": format_fixed(
            round_half_even_fraction(exact_micros.numerator, exact_micros.denominator)
        ),
        "fixed_integer_naive_sum": format_fixed(fixed_naive.total_micros),
        "fixed_integer_kahan_sum": format_fixed(fixed_kahan.total_micros),
        "fixed_kahan_compensation": format_fixed(fixed_kahan.compensation_micros),
        "fixed_integer_error_bound": "0.000000",
        "float64_naive_sum": format_float_units_as_fixed(naive_float),
        "float64_kahan_sum": format_float_units_as_fixed(kahan_float),
        "float64_naive_measured_error": format_error_bound(naive_error),
        "float64_kahan_measured_error": format_error_bound(kahan_error),
        "float64_naive_error_bound": format_error_bound(bound),
        "float64_kahan_error_bound": format_error_bound(kahan_bound),
        "naive_within_bound": naive_error <= bound,
        "kahan_within_bound": kahan_error <= kahan_bound,
        "bound_rounding": "CEILING_TO_MICRO",
        "bound_formula": "naive <= gamma_(n-1)*sum(abs(x)); kahan <= 2*gamma_(n-1)*sum(abs(x)); "
        "gamma_n=n*eps/(1-n*eps), eps=sys.float_info.epsilon",
    }
