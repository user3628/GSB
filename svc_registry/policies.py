from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, ROUND_DOWN, Decimal, InvalidOperation
from typing import Any, Literal

from .errors import (
    E_TIME_INVALID,
    E_TIME_POLICY_REQUIRED,
    E_WEIGHT_INVALID,
    E_WEIGHT_POLICY_REQUIRED,
    BusinessError,
)


WEIGHT_ROUNDING_RULES = ("half_up", "floor", "ceil", "truncate")
TIME_TRUNCATION_RULES = ("floor", "ceil", "half_up")

_DECIMAL_ROUNDING = {
    "half_up": ROUND_HALF_UP,
    "floor": ROUND_FLOOR,
    "ceil": ROUND_CEILING,
    "truncate": ROUND_DOWN,
}


@dataclass(frozen=True)
class WeightPolicy:
    """Caller-declared policy for interpreting weight values.

    Version 1 JSON snapshots contain a floating-point ratio.  Converting that
    ratio to permille is lossy, so both the source scale and the rounding rule
    must be declared by the caller.  Version 2 permille values do not need a
    policy, but a policy may be supplied and is then checked for consistency.
    """

    source_scale: Literal["ratio", "permille", "mixed"] | None = None
    rounding_rule: str = "half_up"
    declared: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "WeightPolicy":
        if value is None:
            return cls()
        source_scale = value.get("source_scale")
        rounding_rule = value.get("rounding_rule", "half_up")
        if source_scale not in (None, "ratio", "permille", "mixed"):
            raise BusinessError(
                E_WEIGHT_INVALID,
                "weight_policy.source_scale must be ratio, permille, or mixed",
                details={"source_scale": source_scale},
            )
        if rounding_rule not in WEIGHT_ROUNDING_RULES:
            raise BusinessError(
                E_WEIGHT_INVALID,
                "unsupported weight rounding rule",
                details={
                    "rounding_rule": rounding_rule,
                    "supported": list(WEIGHT_ROUNDING_RULES),
                },
            )
        declared = bool(value.get("declared", source_scale is not None))
        return cls(
            source_scale=source_scale,
            rounding_rule=rounding_rule,
            declared=declared,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_scale": self.source_scale,
            "rounding_rule": self.rounding_rule,
            "declared": self.declared,
        }


@dataclass(frozen=True)
class TimePolicy:
    """Declared rule for converting microsecond JSON timestamps to ms."""

    truncation_rule: Literal["floor", "ceil", "half_up"] = "floor"
    declared: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "TimePolicy":
        if value is None:
            return cls()
        rule = value.get("truncation_rule", "floor")
        if rule not in TIME_TRUNCATION_RULES:
            raise BusinessError(
                E_TIME_INVALID,
                "unsupported time truncation rule",
                details={
                    "truncation_rule": rule,
                    "supported": list(TIME_TRUNCATION_RULES),
                },
            )
        declared = bool(value.get("declared", True))
        return cls(truncation_rule=rule, declared=declared)

    def to_dict(self) -> dict[str, Any]:
        return {
            "truncation_rule": self.truncation_rule,
            "declared": self.declared,
        }


def convert_weight_to_permille(
    value: int | float,
    encoding: str,
    policy: WeightPolicy,
    *,
    record_index: int,
) -> tuple[int, float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BusinessError(
            E_WEIGHT_INVALID,
            "weight must be a number",
            details={"record_index": record_index, "weight": value},
        )
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        raise BusinessError(
            E_WEIGHT_INVALID,
            "weight must be finite",
            details={"record_index": record_index, "weight": str(value)},
        )
    if value < 0:
        raise BusinessError(
            E_WEIGHT_INVALID,
            "weight must not be negative",
            details={"record_index": record_index, "weight": value},
        )

    if encoding == "permille":
        if isinstance(value, bool) or not isinstance(value, int):
            raise BusinessError(
                E_WEIGHT_INVALID,
                "fixed-point weight must be an integer permille value",
                details={
                    "record_index": record_index,
                    "weight": value,
                    "expected_encoding": "permille",
                },
            )
        if not 1 <= value <= 1000:
            raise BusinessError(
                E_WEIGHT_INVALID,
                "permille weight must be within 1..1000",
                details={"record_index": record_index, "weight": value},
            )
        return value, 0.0

    if encoding != "ratio":
        raise BusinessError(
            E_WEIGHT_INVALID,
            "unknown weight encoding",
            details={"record_index": record_index, "encoding": encoding},
        )
    if not policy.declared or policy.source_scale not in ("ratio", "mixed"):
        raise BusinessError(
            E_WEIGHT_POLICY_REQUIRED,
            "a declared ratio-to-permille policy is required for float weights",
            details={
                "record_index": record_index,
                "weight": value,
                "required_policy": {
                    "source_scale": "ratio",
                    "rounding_rule": list(WEIGHT_ROUNDING_RULES),
                },
            },
            http_status=422,
        )
    if value > 1:
        raise BusinessError(
            E_WEIGHT_INVALID,
            "ratio weight must be within 0..1",
            details={"record_index": record_index, "weight": value},
        )
    try:
        exact = Decimal(str(value)) * Decimal(1000)
        rounded_decimal = exact.quantize(Decimal("1"), rounding=_DECIMAL_ROUNDING[policy.rounding_rule])
        rounded = int(rounded_decimal)
    except (InvalidOperation, ValueError) as exc:
        raise BusinessError(
            E_WEIGHT_INVALID,
            "weight cannot be converted to fixed point",
            details={"record_index": record_index, "weight": value},
        ) from exc
    if rounded < 1 or rounded > 1000:
        raise BusinessError(
            E_WEIGHT_INVALID,
            "converted weight is outside 1..1000",
            details={
                "record_index": record_index,
                "weight": value,
                "weight_permille": rounded,
            },
        )
    return rounded, round(float(exact - rounded_decimal), 12)


def _checked_timestamp(value: int, *, field_name: str, record_index: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BusinessError(
            E_TIME_INVALID,
            f"{field_name} must be a non-negative integer",
            details={"record_index": record_index, "field": field_name, "value": value},
        )
    return value


def normalize_timestamp_to_ms(
    value: int,
    unit: str,
    policy: TimePolicy,
    *,
    field_name: str,
    record_index: int,
) -> tuple[int, int]:
    checked = _checked_timestamp(value, field_name=field_name, record_index=record_index)
    if unit == "ms":
        return checked, 0
    if unit != "us":
        raise BusinessError(
            E_TIME_INVALID,
            "unknown timestamp unit",
            details={"record_index": record_index, "unit": unit},
        )
    if not policy.declared:
        raise BusinessError(
            E_TIME_POLICY_REQUIRED,
            "a declared microsecond-to-millisecond policy is required for JSONL timestamps",
            details={
                "record_index": record_index,
                "field": field_name,
                "required_policy": {"truncation_rule": list(TIME_TRUNCATION_RULES)},
            },
            http_status=422,
        )
    residue = checked % 1000
    if policy.truncation_rule == "floor":
        return checked // 1000, residue
    if policy.truncation_rule == "ceil":
        return (checked + 999) // 1000, residue
    return (checked + 500) // 1000, residue
