from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any

from .errors import spec_invalid

INT64_MAX = 9_223_372_036_854_775_807

_MEMORY_RE = re.compile(r"^(?P<number>[+-]?\d+)(?P<suffix>Ki|Mi|Gi|Ti|Pi|K|M|G|T|P)?$")
_COMPUTE_RE = re.compile(r"^(?P<number>[+-]?\d+)(?P<suffix>m)?$")

_MEMORY_FACTORS = {
    None: 1,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "P": 1000**5,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
}


def _checked_nonnegative(value: int, field: str) -> int:
    if value < 0:
        raise spec_invalid(f"{field} must be non-negative", field=field, value=value)
    if value > INT64_MAX:
        raise spec_invalid(
            f"{field} overflows signed 64-bit range",
            field=field,
            value=value,
            max=INT64_MAX,
        )
    return value


def parse_memory(value: Any, field: str = "memory") -> int:
    """Parse a byte quantity such as 2Gi, 512Mi, or a plain byte integer."""

    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise spec_invalid(f"{field} must be a byte integer or quantity string", field=field)
    if isinstance(value, int):
        return _checked_nonnegative(value, field)

    text = value.strip()
    match = _MEMORY_RE.fullmatch(text)
    if not match:
        raise spec_invalid(
            f"{field} has an unsupported memory quantity {value!r}",
            field=field,
            value=value,
            examples=["2Gi", "512Mi", "1024"],
        )
    number = int(match.group("number"))
    if number < 0:
        raise spec_invalid(f"{field} must be non-negative", field=field, value=value)
    factor = _MEMORY_FACTORS[match.group("suffix")]
    if number > INT64_MAX // factor:
        raise spec_invalid(
            f"{field} overflows signed 64-bit range",
            field=field,
            value=value,
            max=INT64_MAX,
        )
    return number * factor


def parse_compute_milli(value: Any, field: str = "compute") -> int:
    """Parse compute as thousandths.

    An integer is already in milli-units.  A string ending in ``m`` is already
    milli-units; a unitless string follows Kubernetes style (``2`` == 2000m).
    """

    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise spec_invalid(
            f"{field} must be a milli-integer or quantity string", field=field
        )
    if isinstance(value, int):
        return _checked_nonnegative(value, field)

    text = value.strip()
    match = _COMPUTE_RE.fullmatch(text)
    if not match:
        raise spec_invalid(
            f"{field} has an unsupported compute quantity {value!r}",
            field=field,
            value=value,
            examples=["250m", "2", "1500"],
        )
    number = int(match.group("number"))
    if number < 0:
        raise spec_invalid(f"{field} must be non-negative", field=field, value=value)
    milli = number if match.group("suffix") == "m" else number * 1000
    return _checked_nonnegative(milli, field)


def require_int(value: Any, field: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise spec_invalid(f"{field} must be an integer", field=field, value=value)
    if minimum is not None and value < minimum:
        raise spec_invalid(
            f"{field} must be >= {minimum}", field=field, value=value, minimum=minimum
        )
    if value > INT64_MAX or value < -INT64_MAX - 1:
        raise spec_invalid(f"{field} is outside signed 64-bit range", field=field, value=value)
    return value


def require_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise spec_invalid(f"{field} must be a non-empty string", field=field)
    return value.strip()


def parse_timestamp(value: Any, field: str) -> str:
    text = require_text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise spec_invalid(
            f"{field} must be an ISO-8601 timestamp", field=field, value=text
        ) from exc
    if parsed.tzinfo is None:
        raise spec_invalid(
            f"{field} must include a timezone offset", field=field, value=text
        )
    return text


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def request_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def ceil_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    return (numerator + denominator - 1) // denominator
