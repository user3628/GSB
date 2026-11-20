from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any

MAX_I64 = 2**63 - 1
US_PER_MS = 1_000

_QUANTITY_RE = re.compile(r"^\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*([A-Za-z\u00b5\u03bc]*)\s*$")

_BYTE_SUFFIXES = {
    "": 1,
    "b": 1,
    "k": 1_000,
    "kb": 1_000,
    "m": 1_000_000,
    "mb": 1_000_000,
    "g": 1_000_000_000,
    "gb": 1_000_000_000,
    "t": 1_000_000_000_000,
    "tb": 1_000_000_000_000,
    "ki": 1_024,
    "kib": 1_024,
    "mi": 1_024**2,
    "mib": 1_024**2,
    "gi": 1_024**3,
    "gib": 1_024**3,
    "ti": 1_024**4,
    "tib": 1_024**4,
}

_DURATION_SUFFIXES = {
    "ns": Decimal(1) / Decimal(1_000),
    "us": Decimal(1),
    "ms": Decimal(1_000),
    "s": Decimal(1_000_000),
}


class QuantityError(ValueError):
    pass


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise QuantityError(f"{field}: expected a number or quantity string")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise QuantityError(f"{field}: non-finite number is not allowed")
        text = str(value)
    else:
        text = str(value)
    try:
        parsed = Decimal(text.strip())
    except (InvalidOperation, ValueError) as exc:
        raise QuantityError(f"{field}: cannot parse {value!r}") from exc
    if not parsed.is_finite():
        raise QuantityError(f"{field}: non-finite number is not allowed")
    if parsed < 0:
        raise QuantityError(f"{field}: negative values are not allowed")
    return parsed


def _split_quantity(value: Any, field: str) -> tuple[Decimal, str]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _decimal(value, field), ""
    if not isinstance(value, str):
        raise QuantityError(f"{field}: expected a number or quantity string")
    match = _QUANTITY_RE.match(value)
    if not match:
        raise QuantityError(f"{field}: invalid quantity {value!r}")
    suffix = match.group(2).lower().replace("\u00b5", "u").replace("\u03bc", "u")
    return _decimal(match.group(1), field), suffix


def _checked_integer(decimal_value: Decimal, field: str) -> int:
    integral = decimal_value.to_integral_value()
    if decimal_value != integral:
        raise QuantityError(f"{field}: value is not an integral number of base units")
    result = int(integral)
    if result < 0:
        raise QuantityError(f"{field}: negative values are not allowed")
    if result > MAX_I64:
        raise QuantityError(f"{field}: value overflows signed 64-bit range")
    return result


def parse_bytes(value: Any, field: str = "bytes") -> int:
    number, suffix = _split_quantity(value, field)
    multiplier = _BYTE_SUFFIXES.get(suffix)
    if multiplier is None:
        raise QuantityError(f"{field}: unknown byte suffix {suffix!r}")
    return _checked_integer(number * multiplier, field)


def parse_duration_us(value: Any, field: str = "duration") -> int:
    number, suffix = _split_quantity(value, field)
    if suffix == "" and isinstance(value, (int, float)) and not isinstance(value, bool):
        multiplier = Decimal(1)
    else:
        multiplier = _DURATION_SUFFIXES.get(suffix)
    if multiplier is None:
        raise QuantityError(f"{field}: unknown duration suffix {suffix!r}")
    return _checked_integer(number * multiplier, field)


def parse_arrival_us(value: Any, field: str = "arrival_ms") -> int:
    number = _decimal(value, field)
    return _checked_integer(number * US_PER_MS, field)


def checked_add(left: int, right: int, field: str = "sum") -> int:
    result = left + right
    if result < 0:
        raise QuantityError(f"{field}: negative values are not allowed")
    if result > MAX_I64:
        raise QuantityError(f"{field}: value overflows signed 64-bit range")
    return result


def checked_mul(left: int, right: int, field: str = "product") -> int:
    if left < 0 or right < 0:
        raise QuantityError(f"{field}: negative values are not allowed")
    if left and right > MAX_I64 // left:
        raise QuantityError(f"{field}: value overflows signed 64-bit range")
    return left * right


def round_half_up_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    if numerator >= 0:
        return (numerator * 2 + denominator) // (denominator * 2)
    return -((-numerator * 2 + denominator) // (denominator * 2))


def us_to_ms_x100(value_us: int) -> int:
    """Convert microseconds to fixed hundredths of a millisecond, half-up."""

    return round_half_up_div(value_us, 10)


def format_fixed_ms(value_x100: int) -> str:
    sign = "-" if value_x100 < 0 else ""
    absolute = abs(value_x100)
    return f"{sign}{absolute // 100}.{absolute % 100:02d}"


def format_bytes(value: int) -> str:
    units = (("TiB", 1024**4), ("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024))
    for suffix, size in units:
        if value and value % size == 0:
            return f"{value // size}{suffix}"
    return f"{value}B"


def relative_ppm(absolute_deviation: int, truth: int) -> int | None:
    if truth == 0:
        return 0 if absolute_deviation == 0 else None
    return round_half_up_div(absolute_deviation * 1_000_000, truth)


def ppm_to_string(value_ppm: int | None) -> str | None:
    if value_ppm is None:
        return None
    sign = "-" if value_ppm < 0 else ""
    absolute = abs(value_ppm)
    return f"{sign}{absolute // 10_000}.{absolute % 10_000:04d}%"
