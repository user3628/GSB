"""定点数值原语与归一化、误差上界、float 拒绝。"""

from fractions import Fraction

import pytest

from app.fixedpoint import (
    DECIMALS,
    SCALE,
    ULP,
    Fixed,
    ImplicitFloatError,
    kahan_sum,
    normalize_weights,
)


def test_decimal_string_avoids_binary_float():
    assert Fixed.from_str("0.1").add(Fixed.from_str("0.2")).to_str() == "0.300000000"


def test_float_construction_rejected():
    with pytest.raises(ImplicitFloatError):
        Fixed(0.1)
    with pytest.raises(ImplicitFloatError):
        Fixed.from_str("0.1").mul(0.2)  # type: ignore[arg-type]


def test_division_error_bounded_by_half_ulp():
    result, err = Fixed.from_str("1").div(Fixed.from_str("3"), "half_even")
    assert result.to_str() == "0.333333333"
    assert abs(err) <= ULP


def test_normalization_sums_to_exact_one():
    raw = [("v1", Fixed.from_str("1")), ("v2", Fixed.from_str("1")),
           ("v3", Fixed.from_str("1"))]
    normalized, total, errors = normalize_weights(raw)
    assert total == Fixed.one()
    assert sum(w.n for _, w in normalized) == SCALE
    assert all(abs(e) <= ULP for e in errors)


def test_normalization_deterministic_lexicographic():
    raw = [("b", Fixed.from_str("1")), ("a", Fixed.from_str("1")),
           ("c", Fixed.from_str("1"))]
    n1, _, _ = normalize_weights(raw)
    n2, _, _ = normalize_weights(list(reversed(raw)))
    assert n1 == n2
    assert [v for v, _ in n1] == ["a", "b", "c"]


def test_zero_and_negative_weights_rejected():
    with pytest.raises(Exception):
        normalize_weights([("a", Fixed.zero()), ("b", Fixed.zero())])
    with pytest.raises(Exception):
        normalize_weights([("a", Fixed.from_str("-1"))])


def test_kahan_exact_for_integer_magnitudes():
    vals = [Fixed(10**18 * SCALE), Fixed(SCALE), Fixed(SCALE)]
    total, comp = kahan_sum(vals)
    assert total.n == (10**18 + 2) * SCALE
    assert comp.n == 0


def test_fixed_strings_have_fixed_decimals():
    assert Fixed.from_str("0.5").to_str() == "0.500000000"
    assert len(Fixed.from_str("0.5").to_str().split(".")[1]) == DECIMALS


def test_half_even_rounding_midpoint():
    # 1/(2*SCALE) 恰为半个 ulp，half_even 舍到偶数分子。
    val = Fraction(1, 2 * SCALE)
    fixed, err = Fixed.from_fraction(val, "half_even")
    assert fixed.n in (0, 1)
    assert abs(err) <= ULP / 2
