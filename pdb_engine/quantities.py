from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import spec_invalid

MAX_INT64 = 9_223_372_036_854_775_807

_MEMORY_SUFFIXES = {
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
}


def _number(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise spec_invalid("boolean is not a numeric quantity", field=field)
    if isinstance(value, int):
        number = Decimal(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise spec_invalid("empty quantity", field=field, value=value)
        try:
            number = Decimal(text)
        except InvalidOperation as exc:
            raise spec_invalid("invalid numeric quantity", field=field, value=value) from exc
    else:
        raise spec_invalid("quantity must be a string or integer", field=field, value=value)
    if not number.is_finite() or number < 0:
        raise spec_invalid("quantity must be finite and non-negative", field=field, value=value)
    return number


def _checked_integer(number: Decimal, field: str, value: Any) -> int:
    if number != number.to_integral_value():
        raise spec_invalid("quantity does not resolve to an integer", field=field, value=value)
    result = int(number)
    if result > MAX_INT64:
        raise spec_invalid("quantity overflows signed 64-bit range", field=field, value=value)
    return result


def parse_cpu_millicores(value: Any, field: str = "cpu") -> int:
    """Parse Kubernetes-style CPU input to integer millicores.

    ``2`` means two cores and becomes ``2000`` millicores. ``500m`` becomes
    ``500``. Fractional core strings such as ``0.25`` are accepted when they
    resolve exactly to an integer number of millicores.
    """

    if isinstance(value, bool):
        raise spec_invalid("boolean is not a CPU quantity", field=field)
    if isinstance(value, int):
        result = Decimal(value) * 1000
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise spec_invalid("empty CPU quantity", field=field, value=value)
        if text.endswith("m"):
            result = _number(text[:-1], field)
        else:
            result = _number(text, field) * 1000
    else:
        raise spec_invalid("CPU quantity must be a string or integer", field=field, value=value)
    return _checked_integer(result, field, value)


def parse_memory_bytes(value: Any, field: str = "memory") -> int:
    """Parse Kubernetes-style memory input to integer bytes."""

    if isinstance(value, bool):
        raise spec_invalid("boolean is not a memory quantity", field=field)
    if isinstance(value, int):
        return _checked_integer(Decimal(value), field, value)
    if not isinstance(value, str):
        raise spec_invalid("memory quantity must be a string or integer", field=field, value=value)

    text = value.strip()
    if not text:
        raise spec_invalid("empty memory quantity", field=field, value=value)

    multiplier = 1
    numeric = text
    for suffix in ("Ki", "Mi", "Gi", "Ti", "K", "M", "G", "T"):
        if text.endswith(suffix):
            multiplier = _MEMORY_SUFFIXES[suffix]
            numeric = text[: -len(suffix)]
            break
    if not numeric:
        raise spec_invalid("memory quantity has no numeric component", field=field, value=value)
    return _checked_integer(_number(numeric, field) * multiplier, field, value)


def checked_add(left: int, right: int, field: str) -> int:
    result = left + right
    if result > MAX_INT64:
        raise spec_invalid("integer addition overflows signed 64-bit range", field=field)
    return result

