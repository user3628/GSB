"""Timestamp normalization with signed cumulative rounding accounting."""

from __future__ import annotations

from .errors import SloError
from .models import RoundingInfo
from .policy import TRUNCATION_RULES


def normalize_us_to_ms(
    source_us: int,
    *,
    rule: str,
    key: tuple[str, str, int],
    source_field: str,
    offset: int,
    path: str,
) -> tuple[int, RoundingInfo]:
    if rule not in TRUNCATION_RULES:
        raise SloError(
            "E_CONFIG_INVALID",
            f"unsupported truncation rule: {rule}",
            details={"allowed": sorted(TRUNCATION_RULES)},
        )
    if isinstance(source_us, bool) or not isinstance(source_us, int):
        raise SloError(
            "E_RECORD_CORRUPT",
            f"{source_field} must be an integer number of microseconds",
            path=path,
            offset=offset,
        )
    if source_us < 0:
        raise SloError(
            "E_RECORD_CORRUPT",
            f"{source_field} must be non-negative",
            path=path,
            offset=offset,
            details={"value": source_us},
        )

    floor_ms, remainder_us = divmod(source_us, 1000)
    if rule == "floor":
        normalized_ms = floor_ms
    elif rule == "ceil":
        normalized_ms = floor_ms + (1 if remainder_us else 0)
    else:
        normalized_ms = (source_us + 500) // 1000

    rounding_error_us = normalized_ms * 1000 - source_us
    info = RoundingInfo(
        key=key,
        source_field=source_field,
        source_value=source_us,
        source_unit="us",
        normalized_ms=normalized_ms,
        rule=rule,
        remainder_us=remainder_us,
        rounding_bit=1 if remainder_us else 0,
        rounding_error_us=rounding_error_us,
        offset=offset,
    )
    return normalized_ms, info


def binary_ms_rounding_info(
    *,
    key: tuple[str, str, int],
    source_field: str,
    source_value: int,
    normalized_ms: int,
    offset: int,
) -> RoundingInfo:
    return RoundingInfo(
        key=key,
        source_field=source_field,
        source_value=source_value,
        source_unit="ms",
        normalized_ms=normalized_ms,
        rule="exact_ms",
        remainder_us=0,
        rounding_bit=0,
        rounding_error_us=0,
        offset=offset,
    )
