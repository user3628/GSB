"""Dimension-string parsing with explicit integer-overflow checks."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import spec_error


MAX_INTERNAL_INT = (1 << 63) - 1

_BINARY_MEMORY_UNITS = {
    "": 1,
    "b": 1,
    "k": 1 << 10,
    "ki": 1 << 10,
    "kib": 1 << 10,
    "m": 1 << 20,
    "mi": 1 << 20,
    "mib": 1 << 20,
    "g": 1 << 30,
    "gi": 1 << 30,
    "gib": 1 << 30,
    "t": 1 << 40,
    "ti": 1 << 40,
    "tib": 1 << 40,
}

_DECIMAL_MEMORY_UNITS = {
    "kb": 1_000,
    "mb": 1_000_000,
    "gb": 1_000_000_000,
    "tb": 1_000_000_000_000,
}

_TIME_MS_UNITS = {
    "": 1,
    "m": 1,
    "ms": 1,
    "milli": 1,
    "millis": 1,
    "millisecond": 1,
    "milliseconds": 1,
    "s": 1_000,
    "sec": 1_000,
    "secs": 1_000,
    "second": 1_000,
    "seconds": 1_000,
    "min": 60_000,
    "mins": 60_000,
    "minute": 60_000,
    "minutes": 60_000,
}


def _split_number_and_unit(value: str) -> tuple[str, str]:
    text = value.strip()
    if not text:
        raise ValueError("empty quantity")
    split_at = len(text)
    for index, char in enumerate(text):
        if index > 0 and char.isalpha():
            split_at = index
            break
    number = text[:split_at].replace("_", "")
    unit = text[split_at:].strip().lower().replace("_", "")
    return number, unit


def _require_plain_integer(value: Any, field: str, label: str) -> int:
    if isinstance(value, bool):
        raise spec_error(field, f"{label} must be an integer, not a boolean")
    if isinstance(value, int):
        if value < 0:
            raise spec_error(field, f"{label} must not be negative", value=value)
        if value > MAX_INTERNAL_INT:
            raise spec_error(
                field,
                f"{label} overflows signed 64-bit integer",
                value=value,
                max=MAX_INTERNAL_INT,
            )
        return value
    if isinstance(value, str):
        return _parse_decimal_integer(value, field, label, scale=1)
    raise spec_error(
        field,
        f"{label} must be an integer or a dimension string",
        value=value,
        value_type=type(value).__name__,
    )


def _parse_decimal_integer(
    text: str, field: str, label: str, *, scale: int
) -> int:
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError):
        raise spec_error(field, f"{label} is not a valid number", value=text) from None
    if not number.is_finite():
        raise spec_error(field, f"{label} must be finite", value=text)
    if number < 0:
        raise spec_error(field, f"{label} must not be negative", value=text)
    scaled = number * scale
    if scaled != scaled.to_integral_value():
        raise spec_error(
            field,
            f"{label} must resolve to an integer internal value",
            value=text,
        )
    result = int(scaled)
    if result > MAX_INTERNAL_INT:
        raise spec_error(
            field,
            f"{label} overflows signed 64-bit integer",
            value=text,
            max=MAX_INTERNAL_INT,
        )
    return result


def parse_memory_bytes(value: Any, field: str) -> int:
    """Parse bytes into an integer number of bytes.

    Binary suffixes ``Ki/Mi/Gi/Ti`` use powers of 1024.  Plain ``K/M/G/T``
    and suffixes containing ``B`` (except ``KiB/MiB/GiB/TiB``) use powers of
    1024 as well for deterministic local samples.  Decimal ``KB/MB/GB/TB``
    spellings use powers of 1000.
    """

    if isinstance(value, (int, bool)):
        return _require_plain_integer(value, field, "memory")
    if not isinstance(value, str):
        raise spec_error(
            field,
            "memory must be an integer byte count or dimension string",
            value=value,
            value_type=type(value).__name__,
        )
    text = value.strip()
    if not text:
        raise spec_error(field, "memory must not be empty", value=value)
    number_text, unit = _split_number_and_unit(text)
    if not number_text:
        raise spec_error(field, "memory is missing a number", value=value)
    if unit in _DECIMAL_MEMORY_UNITS:
        scale = _DECIMAL_MEMORY_UNITS[unit]
    elif unit in _BINARY_MEMORY_UNITS:
        scale = _BINARY_MEMORY_UNITS[unit]
    else:
        raise spec_error(
            field,
            f"unknown memory unit {unit!r}",
            value=value,
            allowed_units=["B", "Ki", "Mi", "Gi", "Ti", "KB", "MB", "GB", "TB"],
        )
    return _parse_decimal_integer(number_text, field, "memory", scale=scale)


def parse_millis(value: Any, field: str) -> int:
    """Parse a compute share or duration into integer thousandths.

    ``250m`` is 250 internal units.  A bare integer is already in internal
    thousandths.  ``1s`` is 1000 internal units and is mainly useful for
    latency samples.
    """

    if isinstance(value, (int, bool)):
        return _require_plain_integer(value, field, "value")
    if not isinstance(value, str):
        raise spec_error(
            field,
            "value must be an integer or dimension string",
            value=value,
            value_type=type(value).__name__,
        )
    text = value.strip()
    if not text:
        raise spec_error(field, "value must not be empty", value=value)
    number_text, unit = _split_number_and_unit(text)
    if not number_text:
        raise spec_error(field, "value is missing a number", value=value)
    if unit not in _TIME_MS_UNITS:
        raise spec_error(
            field,
            f"unknown compute/time unit {unit!r}",
            value=value,
            allowed_units=["m", "ms", "s", "min"],
        )
    return _parse_decimal_integer(
        number_text, field, "value", scale=_TIME_MS_UNITS[unit]
    )


def checked_product(left: int, right: int, field: str, label: str) -> int:
    if left < 0 or right < 0:
        raise spec_error(field, f"{label} must not be negative")
    if left and right > MAX_INTERNAL_INT // left:
        raise spec_error(
            field,
            f"{label} overflows signed 64-bit integer",
            left=left,
            right=right,
            max=MAX_INTERNAL_INT,
        )
    return left * right


def checked_add(left: int, right: int, field: str, label: str) -> int:
    if left < 0 or right < 0:
        raise spec_error(field, f"{label} must not be negative")
    if left > MAX_INTERNAL_INT - right:
        raise spec_error(
            field,
            f"{label} overflows signed 64-bit integer",
            left=left,
            right=right,
            max=MAX_INTERNAL_INT,
        )
    return left + right


def format_bytes(value: int) -> str:
    for suffix, scale in (
        ("Ti", 1 << 40),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ):
        if value and value % scale == 0:
            return f"{value // scale}{suffix}"
    return f"{value}B"

