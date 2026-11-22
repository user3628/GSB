from __future__ import annotations

import json
from fractions import Fraction

from leasefair.fixedpoint import (
    SCALE,
    float_summation_cross_check,
    normalize_integer_weights,
    q_div,
    q_mul,
    q_to_string,
)
from leasefair.time_contract import expiry_from_grant, renewal_margin_q, ttl_precision_report
from tests.conftest import SAMPLE_PATH


def test_normalization_is_exact_and_insertion_order_independent() -> None:
    left = {"zeta": 17, "alpha": 5, "middle": 3}
    right = {"middle": 3, "alpha": 5, "zeta": 17}
    normalized_left, trace_left = normalize_integer_weights(left)
    normalized_right, trace_right = normalize_integer_weights(right)
    assert normalized_left == normalized_right
    assert trace_left == trace_right
    assert sum(normalized_left.values()) == SCALE
    assert set(normalized_left) == {"alpha", "middle", "zeta"}


def test_q_arithmetic_declares_final_rounding() -> None:
    product, product_error = q_mul(333_333_333, 3 * SCALE)
    assert product == 999_999_999
    assert product_error == Fraction(0)
    quotient, quotient_error = q_div(SCALE, 3 * SCALE)
    assert quotient == 333_333_333
    assert quotient_error == Fraction(1, 3)
    assert q_to_string(quotient) == "0.333333333"


def test_time_contract_is_integer_ms_and_offset_is_explicit() -> None:
    report = ttl_precision_report(
        [300, 30_000, 300_000, 3_600_000, 7_200_000],
        granted_at_ms=1_700_000_000_000,
        now_ms=1_700_000_000_000,
        clock_offset_ms=123,
    )
    for row in report["rows"]:
        ttl = int(row["ttl_ms"])
        assert row["expires_at_ms"] == 1_700_000_000_000 + ttl
        assert row["remaining_at_now_ms"] == ttl
        assert row["offset_remaining_ms"] == max(0, ttl - 123)
    assert expiry_from_grant(10, 300) == 310
    margin, remaining = renewal_margin_q(1_700_000_000_300, 300, 1_700_000_000_200)
    assert remaining == 100
    assert q_to_string(margin) == "0.333333333"


def test_numerical_stress_shows_naive_float_error_and_kahan_repair() -> None:
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    terms = sample["numerical_benchmark"]["stress_terms_q"]
    report = float_summation_cross_check(terms)
    assert report["canonical_decimal"] == "10000000.000000100"
    assert report["naive_error_q_estimate"] > 0
    assert report["numpy_naive_error_q_estimate"] is not None
    assert abs(report["kahan_float_error_q_estimate"]) < abs(report["naive_error_q_estimate"])
    ttls = sample["numerical_benchmark"]["ttl_values_ms"]
    assert max(ttls) / min(ttls) >= 1000

