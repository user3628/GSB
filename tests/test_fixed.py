from __future__ import annotations

import json
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

from app.fixed import (
    SCALE,
    TrackedFixed,
    compare_ge,
    compare_in_interval,
    compare_le,
    fixed_from_decimal,
    format_fixed,
    kahan_sum_fixed,
    mixed_sum_diagnostics,
    relative_tolerance_micros,
    round_half_even_fraction,
)
from app.snapshot import load_json_decimal


def test_round_half_even_ties_to_even() -> None:
    assert round_half_even_fraction(5, 10) == 0
    assert round_half_even_fraction(15, 10) == 2
    assert round_half_even_fraction(-5, 10) == 0
    assert round_half_even_fraction(-15, 10) == -2
    assert fixed_from_decimal(Decimal("0.0000005")).micros == 0
    assert fixed_from_decimal(Decimal("0.0000015")).micros == 2


def test_fixed_format_is_six_places() -> None:
    assert format_fixed(0) == "0.000000"
    assert format_fixed(1) == "0.000001"
    assert format_fixed(-1) == "-0.000001"
    assert format_fixed(1234567) == "1.234567"


def test_relative_tolerance_boundaries_are_inclusive() -> None:
    rel_tol = 10
    assert compare_ge(999_990, SCALE, rel_tol)
    assert compare_le(1_000_010, SCALE, rel_tol)
    assert not compare_ge(999_989, SCALE, rel_tol)
    assert not compare_le(1_000_012, SCALE, rel_tol)
    inside, effective_lower, effective_upper = compare_in_interval(
        1_000_010, 900_000, 1_000_000, rel_tol
    )
    assert inside
    assert effective_lower <= 900_000
    assert effective_upper >= 1_000_010
    assert relative_tolerance_micros(SCALE, SCALE, rel_tol) == 10


def test_kahan_fixed_sum_is_exact_for_integer_micros() -> None:
    values = [TrackedFixed(10**15, Fraction(0)), TrackedFixed(1, Fraction(0)), TrackedFixed(-10**15, Fraction(0))]
    result = kahan_sum_fixed(values)
    assert result.total_micros == 1
    assert result.compensation_micros == 0
    assert result.error == 0


def test_mixed_large_small_sum_comparison_has_quantified_bounds(root: Path) -> None:
    values = load_json_decimal((root / "data" / "mixed_sum.json").read_text(encoding="utf-8"))["values"]
    report = mixed_sum_diagnostics(values)
    assert report["exact_sum"] == "2.000000"
    assert report["fixed_integer_kahan_sum"] == "2.000000"
    assert report["fixed_integer_naive_sum"] == "2.000000"
    assert report["fixed_integer_error_bound"] == "0.000000"
    assert report["float64_naive_measured_error"] != "0.000000"
    assert report["float64_kahan_measured_error"] == "0.000000"
    assert report["naive_within_bound"] is True
    assert report["kahan_within_bound"] is True
    assert report["float64_kahan_measured_error"] != report["float64_naive_measured_error"]
    json.dumps(report, sort_keys=True)
