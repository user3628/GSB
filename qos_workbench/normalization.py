from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, ROUND_DOWN
from typing import Any

from .errors import E_REQUEST_INVALID, E_UNIT_OVERFLOW, E_UNIT_PRECISION


UINT64_MAX = (1 << 64) - 1
TOKEN_SCALE = 1_000_000  # integer sub-units per bit

_TIMESTAMP_RULES = {
    "preserve": "preserve_us",
    "preserve_us": "preserve_us",
    "truncate_us": "preserve_us",
    "floor_ms": "floor_ms",
    "truncate_ms": "floor_ms",
    "truncate_ms_floor": "floor_ms",
    "ceil_ms": "ceil_ms",
    "nearest_ms": "nearest_ms",
    "round_ms": "nearest_ms",
    "toward_zero_ms": "toward_zero_ms",
    "truncate_ms_zero": "toward_zero_ms",
}


def business_error(code: str, message: str, **details: Any):
    # Kept as a tiny local import helper to avoid repeating exception plumbing.
    from .errors import BusinessError

    return BusinessError(code, message, details)


def as_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise business_error(E_REQUEST_INVALID, f"{field} must be numeric")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        result = Decimal(str(value))
    elif isinstance(value, str):
        try:
            result = Decimal(value.strip())
        except InvalidOperation as exc:
            raise business_error(E_REQUEST_INVALID, f"{field} is not a valid number") from exc
    else:
        raise business_error(E_REQUEST_INVALID, f"{field} must be numeric")
    if not result.is_finite():
        raise business_error(E_REQUEST_INVALID, f"{field} must be finite")
    return result


def _exact_int(value: Decimal, field: str) -> int:
    integral = value.to_integral_value(rounding=ROUND_DOWN)
    if value != integral:
        raise business_error(
            E_UNIT_PRECISION,
            f"{field} cannot be represented as an integer in the internal unit",
            value=str(value),
            field=field,
        )
    return int(integral)


def _checked_u64(value: int, field: str) -> int:
    if value < 0 or value > UINT64_MAX:
        raise business_error(
            E_UNIT_OVERFLOW,
            f"{field} exceeds the unsigned 64-bit internal range",
            value=str(value),
            max=str(UINT64_MAX),
            field=field,
        )
    return value


def kbps_to_bps(value: Any, field: str = "cir_kbps") -> int:
    return _checked_u64(_exact_int(as_decimal(value, field) * 1000, field), field)


def kb_to_bytes(value: Any, field: str = "cbs_kb") -> int:
    return _checked_u64(_exact_int(as_decimal(value, field) * 1024, field), field)


def bps_to_kbps_string(value: int) -> str:
    return format(Decimal(value) / Decimal(1000), "f")


def bytes_to_kb_string(value: int) -> str:
    return format(Decimal(value) / Decimal(1024), "f")


def exact_us_from_ms(value: Any, field: str = "timestamp_ms") -> Decimal:
    return as_decimal(value, field) * 1000


def canonical_timestamp_rule(value: str | None) -> str:
    if not value:
        return "preserve_us"
    canonical = _TIMESTAMP_RULES.get(value.strip().lower())
    if not canonical:
        raise business_error(
            E_REQUEST_INVALID,
            "unknown timestamp truncation rule",
            rule=value,
            supported=sorted(_TIMESTAMP_RULES),
        )
    return canonical


@dataclass(frozen=True, slots=True)
class TimestampRounding:
    timestamp_us: int
    exact_us: str
    rounding_applied: bool
    rounding_remainder_us: str
    rounding_direction: str


def normalize_timestamp(
    exact_us: Decimal,
    rule: str = "preserve_us",
    *,
    field: str = "timestamp",
) -> TimestampRounding:
    rule = canonical_timestamp_rule(rule)
    if exact_us < 0:
        raise business_error(E_REQUEST_INVALID, f"{field} must not be negative", field=field)
    if rule == "preserve_us":
        if exact_us == exact_us.to_integral_value():
            normalized = int(exact_us)
        else:
            normalized = int(exact_us.to_integral_value(rounding=ROUND_DOWN))
    elif rule == "floor_ms":
        normalized = int((exact_us / 1000).to_integral_value(rounding=ROUND_FLOOR)) * 1000
    elif rule == "ceil_ms":
        normalized = int((exact_us / 1000).to_integral_value(rounding=ROUND_CEILING)) * 1000
    elif rule == "nearest_ms":
        normalized = int((exact_us / 1000).to_integral_value(rounding=ROUND_HALF_UP)) * 1000
    elif rule == "toward_zero_ms":
        normalized = int((exact_us / 1000).to_integral_value(rounding=ROUND_DOWN)) * 1000
    else:  # pragma: no cover - guarded by canonical_timestamp_rule
        raise AssertionError(rule)
    remainder = Decimal(normalized) - exact_us
    if remainder == 0:
        direction = "none"
    elif remainder < 0:
        direction = "down"
    else:
        direction = "up"
    _checked_u64(normalized, field)
    return TimestampRounding(
        timestamp_us=normalized,
        exact_us=serialize_decimal(exact_us),
        rounding_applied=remainder != 0,
        rounding_remainder_us=serialize_decimal(remainder),
        rounding_direction=direction,
    )


def serialize_decimal(value: Decimal) -> str:
    """Stable JSON numeric token for a finite Decimal (no exponent)."""
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"

