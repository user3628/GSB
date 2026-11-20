from decimal import Decimal

import pytest

from app.errors import InsufficientSamplesError
from app.fixedpoint import compensated_sum_us, nearest_rank, output_conversion_trace
from app.units import (
    MAX_I64,
    QuantityError,
    format_fixed_ms,
    parse_bytes,
    parse_duration_us,
    relative_ppm,
    round_half_up_div,
)


def test_parse_binary_and_decimal_byte_units():
    assert parse_bytes("512Mi") == 512 * 1024**2
    assert parse_bytes("2Gi") == 2 * 1024**3
    assert parse_bytes("1GiB") == 1024**3
    assert parse_bytes("1.5GB") == 1_500_000_000
    assert parse_bytes(0) == 0


def test_parse_duration_units_exactly():
    assert parse_duration_us("1500us") == 1_500
    assert parse_duration_us("3ms") == 3_000
    assert parse_duration_us("1.5ms") == 1_500
    assert parse_duration_us("2s") == 2_000_000
    assert parse_duration_us("3000ns") == 3
    with pytest.raises(QuantityError):
        parse_duration_us("2500ns")


@pytest.mark.parametrize(
    "value",
    ["-512Mi", "512GG", "abc", "1.1B", str(MAX_I64) + "Gi"],
)
def test_invalid_bytes_are_rejected(value):
    with pytest.raises(QuantityError):
        parse_bytes(value)


def test_overflow_and_negative_duration_are_rejected():
    with pytest.raises(QuantityError):
        parse_duration_us(f"{MAX_I64}S")
    with pytest.raises(QuantityError):
        parse_duration_us("-1ms")


def test_fixedpoint_half_up_and_rounding_evidence():
    assert round_half_up_div(5, 10) == 1
    assert round_half_up_div(4, 10) == 0
    assert round_half_up_div(15, 10) == 2
    trace = output_conversion_trace(1_505)
    assert trace["output_ms_fixed"] == 151
    assert trace["rounded_us"] == 1_510
    assert trace["rounding_error_us"] == 5
    assert trace["rounding_bit"] is True
    assert trace["rounding_direction"] == "up"
    assert format_fixed_ms(151) == "1.51"


def test_nearest_rank_declares_rank_and_rejects_empty_values():
    quantile = nearest_rank([10, 20, 30, 40], 95, "P95")
    assert quantile.rank == 4
    assert quantile.sample_count == 4
    assert quantile.raw_us == 40
    with pytest.raises(InsufficientSamplesError) as raised:
        nearest_rank([], 99, "P99")
    assert raised.value.code == "E_INSUFFICIENT_SAMPLES"
    assert raised.value.details["sample_count"] == 0


def test_compensated_sum_uses_integer_fixedpoint_and_is_exact():
    result = compensated_sum_us([1_500, 2_000, 3_500, 1])
    assert result.total == 7_001
    assert result.compensation == 0
    assert result.value == 7_001
    assert result.terms == [1_500, 2_000, 3_500, 1]


def test_relative_ppm_never_uses_floats():
    assert relative_ppm(5, 1_000) == 5_000
    assert relative_ppm(0, 0) == 0
    assert relative_ppm(1, 0) is None
    assert Decimal(relative_ppm(1, 3)) == Decimal(333333)
