from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import BusinessError, E_GRAPH_INVALID, E_SNAPSHOT_INVALID


RATIO_SCALE = 1_000_000


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def pretty_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json(value))


def parse_generated_at(value: Any, field: str = "generated_at") -> str:
    if not isinstance(value, str) or not value.strip():
        raise BusinessError(
            E_SNAPSHOT_INVALID,
            "snapshot must carry a non-empty generated_at timestamp",
            {"field": field},
        )
    candidate = value.strip()
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BusinessError(
            E_SNAPSHOT_INVALID,
            "generated_at must be an ISO-8601 timestamp",
            {"field": field, "value": value},
        ) from exc
    if parsed.tzinfo is None:
        raise BusinessError(
            E_SNAPSHOT_INVALID,
            "generated_at must include a timezone",
            {"field": field, "value": value},
        )
    return candidate


def require_positive_int(value: Any, field: str, node_id: str | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BusinessError(
            E_GRAPH_INVALID,
            f"{field} must be a positive integer",
            {"field": field, "node_id": node_id, "value": value},
        )
    return value


def normalize_ratio(ratio: Any) -> dict[str, Any]:
    """Normalize a ratio into millionths using deterministic half-even rounding."""
    if isinstance(ratio, bool) or ratio is None:
        raise BusinessError(
            "E_GRAY_RATIO_INVALID",
            "gray ratio must be a finite number in [0, 1]",
            {
                "ratio": ratio,
                "current_ratio": ratio,
                "legal_interval": [0, 1],
            },
        )
    try:
        decimal_ratio = Decimal(str(ratio))
    except (InvalidOperation, ValueError):
        raise BusinessError(
            "E_GRAY_RATIO_INVALID",
            "gray ratio must be a finite number in [0, 1]",
            {
                "ratio": ratio,
                "current_ratio": ratio,
                "legal_interval": [0, 1],
            },
        )
    if not decimal_ratio.is_finite() or decimal_ratio < 0 or decimal_ratio > 1:
        raise BusinessError(
            "E_GRAY_RATIO_INVALID",
            "gray ratio is outside the legal interval",
            {
                "ratio": str(ratio),
                "current_ratio": str(ratio),
                "legal_interval": [0, 1],
            },
        )
    units = int((decimal_ratio * RATIO_SCALE).to_integral_value(rounding="ROUND_HALF_EVEN"))
    normalized = Decimal(units) / Decimal(RATIO_SCALE)
    rounding_error = decimal_ratio - normalized
    return {
        "input": format(decimal_ratio, "f"),
        "units": units,
        "scale": RATIO_SCALE,
        "normalized": format(normalized, "f"),
        "rounding_error": format(rounding_error, "f"),
    }


def decimal_text(value: Decimal | str | int | float) -> str:
    if isinstance(value, Decimal):
        decimal_value = value
    else:
        decimal_value = Decimal(str(value))
    if decimal_value == 0:
        return "0"
    return format(decimal_value.normalize(), "f")


def finite_float(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return float(value)
