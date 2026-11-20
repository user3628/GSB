from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from app.errors import invalid_quantity

MAX_INT64 = 2**63 - 1

_CPU_RE = re.compile(r"^(?P<number>\d+(?:\.\d+)?)(?P<suffix>m)?$")
_MEMORY_RE = re.compile(r"^(?P<number>\d+(?:\.\d+)?)(?P<suffix>Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$")

_MEMORY_BINARY = {
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
    "Ei": 1024**6,
}
_MEMORY_DECIMAL = {
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "P": 1000**5,
    "E": 1000**6,
}


def _checked_non_negative_int(value: object, *, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise invalid_quantity("quantity must be a non-negative integer or scalar string", path=path, value=value)
    if value < 0:
        raise invalid_quantity("quantity must be non-negative", path=path, value=value)
    if value > MAX_INT64:
        raise invalid_quantity(f"quantity exceeds signed 64-bit integer limit {MAX_INT64}", path=path, value=value)
    return value


def parse_cpu_millicores(value: object, *, path: str) -> int:
    """Parse a CPU scalar into exact millicores.

    Accepted examples: 500m, 2, 1.5, 0.25. Fractional millicores are rejected
    because the internal representation is an integer.
    """

    if isinstance(value, int) and not isinstance(value, bool):
        millicores = _checked_non_negative_int(value, path=path) * 1000
        if millicores > MAX_INT64:
            raise invalid_quantity(f"CPU quantity exceeds signed 64-bit integer limit {MAX_INT64}", path=path, value=value)
        return millicores
    if not isinstance(value, str):
        raise invalid_quantity("CPU quantity must be an integer or string", path=path, value=value)

    text = value.strip()
    match = _CPU_RE.fullmatch(text)
    if not match:
        raise invalid_quantity("invalid CPU quantity or unit", path=path, value=value)
    number = Decimal(match.group("number"))
    millicores_decimal = number if match.group("suffix") == "m" else number * 1000
    if number == 0:
        return 0
    if millicores_decimal != millicores_decimal.to_integral_value():
        raise invalid_quantity("CPU quantity is below one millicore (fractional millicores are unsupported)", path=path, value=value)
    millicores = int(millicores_decimal)
    if millicores > MAX_INT64:
        raise invalid_quantity(f"CPU quantity exceeds signed 64-bit integer limit {MAX_INT64}", path=path, value=value)
    return millicores


def parse_memory_bytes(value: object, *, path: str) -> int:
    """Parse a memory scalar into exact bytes using binary or decimal units."""

    if isinstance(value, int) and not isinstance(value, bool):
        return _checked_non_negative_int(value, path=path)
    if not isinstance(value, str):
        raise invalid_quantity("memory quantity must be an integer or string", path=path, value=value)

    text = value.strip()
    match = _MEMORY_RE.fullmatch(text)
    if not match:
        raise invalid_quantity("invalid memory quantity or unit", path=path, value=value)
    suffix = match.group("suffix") or ""
    multiplier = _MEMORY_BINARY.get(suffix) or _MEMORY_DECIMAL.get(suffix) or 1
    try:
        amount = Decimal(match.group("number")) * multiplier
    except InvalidOperation as exc:  # pragma: no cover - regex already constrains the input
        raise invalid_quantity("invalid memory quantity", path=path, value=value) from exc
    if amount != amount.to_integral_value():
        raise invalid_quantity("memory quantity resolves to a fractional byte", path=path, value=value)
    memory = int(amount)
    if memory > MAX_INT64:
        raise invalid_quantity(f"memory quantity exceeds signed 64-bit integer limit {MAX_INT64}", path=path, value=value)
    return memory


def format_cpu(millicores: int) -> str:
    if millicores % 1000 == 0:
        return str(millicores // 1000)
    return f"{millicores}m"


def format_memory(size: int) -> str:
    for suffix, multiplier in (("Gi", 1024**3), ("Mi", 1024**2), ("Ki", 1024)):
        if size % multiplier == 0:
            return f"{size // multiplier}{suffix}"
    return f"{size}B"
