"""Exact Kubernetes-style CPU and memory quantity parsing.

Internally CPU is always integer millicores and memory is integer bytes.  The
parser deliberately rejects values that cannot be represented exactly instead
of silently rounding them.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

MAX_CPU_MILLICORES = (1 << 63) - 1
MAX_MEMORY_BYTES = (1 << 63) - 1

_CPU_RE = re.compile(r"^(?P<number>[0-9]+(?:\.[0-9]+)?)(?P<suffix>m?)$")
_MEMORY_RE = re.compile(
    r"^(?P<number>[0-9]+(?:\.[0-9]+)?)(?P<suffix>Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$"
)

_MEMORY_MULTIPLIERS: dict[str, int] = {
    "": 1,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
    "Ei": 1024**6,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "P": 1000**5,
    "E": 1000**6,
}


class QuantityError(ValueError):
    """A quantity could not be converted exactly under the supported grammar."""


def _number(value: object) -> str:
    if not isinstance(value, str):
        raise QuantityError("quantity must be a string")
    if not value:
        raise QuantityError("quantity must not be empty")
    if value != value.strip():
        raise QuantityError("quantity must not contain surrounding whitespace")
    if value.startswith("-"):
        raise QuantityError("quantity must not be negative")
    return value


def parse_cpu_millicores(value: object) -> int:
    """Parse CPU into exact integer millicores.

    Examples: ``500m`` -> 500, ``2`` -> 2000, ``1.5`` -> 1500.
    """

    text = _number(value)
    match = _CPU_RE.fullmatch(text)
    if not match:
        raise QuantityError("invalid CPU quantity or unit; expected cores or millicores")
    try:
        amount = Decimal(match.group("number"))
    except InvalidOperation as exc:  # pragma: no cover - regex already guards this
        raise QuantityError("invalid CPU number") from exc
    millicores = amount if match.group("suffix") == "m" else amount * 1000
    if millicores != millicores.to_integral_value():
        raise QuantityError("CPU quantity is not an exact whole number of millicores")
    result = int(millicores)
    if result < 0:
        raise QuantityError("quantity must not be negative")
    if result > MAX_CPU_MILLICORES:
        raise QuantityError("CPU quantity overflows signed 64-bit millicores")
    return result


def parse_memory_bytes(value: object) -> int:
    """Parse memory into exact integer bytes, including binary and SI suffixes."""

    text = _number(value)
    match = _MEMORY_RE.fullmatch(text)
    if not match:
        raise QuantityError("invalid memory quantity or unit; expected Ki/Mi/Gi/Ti/Pi/Ei")
    multiplier = _MEMORY_MULTIPLIERS[match.group("suffix") or ""]
    try:
        amount = Decimal(match.group("number"))
    except InvalidOperation as exc:  # pragma: no cover - regex already guards this
        raise QuantityError("invalid memory number") from exc
    byte_count = amount * multiplier
    if byte_count != byte_count.to_integral_value():
        raise QuantityError("memory quantity is not an exact whole number of bytes")
    result = int(byte_count)
    if result < 0:
        raise QuantityError("quantity must not be negative")
    if result > MAX_MEMORY_BYTES:
        raise QuantityError("memory quantity overflows signed 64-bit bytes")
    return result


def cpu_human(millicores: int) -> str:
    if millicores % 1000 == 0:
        return str(millicores // 1000)
    return f"{millicores}m"


def memory_human(byte_count: int) -> str:
    for suffix, multiplier in (("Ei", 1024**6), ("Pi", 1024**5), ("Ti", 1024**4), ("Gi", 1024**3), ("Mi", 1024**2), ("Ki", 1024)):
        if byte_count >= multiplier and byte_count % multiplier == 0:
            return f"{byte_count // multiplier}{suffix}"
    return str(byte_count)


def checked_add(left: int, right: int, *, field: str = "value") -> int:
    result = left + right
    if result > MAX_MEMORY_BYTES:
        raise QuantityError(f"{field} overflows signed 64-bit integer")
    return result


def subtract_nonnegative(value: int, amount: int, *, field: str) -> int:
    result = value - amount
    if result < 0:
        raise QuantityError(f"{field} would become negative")
    return result

