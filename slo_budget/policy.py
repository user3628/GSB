"""Declared time, counting and presentation policies."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .errors import SloError


TRUNCATION_RULES = {"floor", "ceil", "half_up"}
ROUNDING_MODE = "ROUND_HALF_UP"


@dataclass(frozen=True)
class BoundaryPolicy:
    """Explicit interval semantics used by every aggregation path.

    Event membership uses ``window_start <= timestamp < window_end``.  A
    late-marked event remains eligible while
    ``as_of_ms - timestamp_ms <= late_tolerance_ms`` (inclusive).
    """

    window_start_inclusive: bool = True
    window_end_inclusive: bool = False
    late_tolerance_inclusive: bool = True
    late_tolerance_ms: int = 60 * 60 * 1000
    max_window_length_ms: int = 7 * 24 * 60 * 60 * 1000
    max_service_name_bytes: int = 255
    max_sli_type_bytes: int = 64

    def validate(self) -> None:
        if self.window_start_inclusive is not True:
            raise SloError(
                "E_CONFIG_INVALID",
                "window_start must be inclusive for the supported format",
            )
        if self.window_end_inclusive is not False:
            raise SloError(
                "E_CONFIG_INVALID",
                "window_end must be exclusive for the supported format",
            )
        if self.late_tolerance_ms < 0:
            raise SloError("E_CONFIG_INVALID", "late_tolerance_ms must be >= 0")
        if self.max_window_length_ms <= 0:
            raise SloError("E_CONFIG_INVALID", "max_window_length_ms must be > 0")
        if self.max_service_name_bytes <= 0 or self.max_sli_type_bytes <= 0:
            raise SloError("E_CONFIG_INVALID", "field length limits must be > 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "window": {
                "start": "inclusive",
                "end": "exclusive",
            },
            "late_tolerance": {
                "boundary": "inclusive" if self.late_tolerance_inclusive else "exclusive",
                "late_tolerance_ms": self.late_tolerance_ms,
                "rule": "as_of_ms - event_timestamp_ms <= late_tolerance_ms",
            },
            "limits": {
                "max_window_length_ms": self.max_window_length_ms,
                "max_service_name_bytes": self.max_service_name_bytes,
                "max_sli_type_bytes": self.max_sli_type_bytes,
            },
        }


@dataclass(frozen=True)
class CalculationPolicy:
    """SLO and multi-window alert policy.

    ``thresholds`` are ordered from most to least severe.  A level only fires
    when both the short and the long window cross that same threshold.
    """

    slo_target: Decimal = Decimal("0.99")
    min_samples: int = 100
    window_lengths_ms: tuple[int, ...] = (5 * 60 * 1000, 30 * 60 * 1000, 60 * 60 * 1000)
    short_window_ms: int = 5 * 60 * 1000
    long_window_ms: int = 60 * 60 * 1000
    thresholds: tuple[tuple[str, Decimal], ...] = (
        ("CRITICAL", Decimal("6.0")),
        ("WARNING", Decimal("2.0")),
        ("WATCH", Decimal("1.0")),
    )
    include_late_events: bool = True
    fixed_places: int = 6
    rounding_mode: str = ROUNDING_MODE
    require_complete_coverage: bool = False

    def validate(self) -> None:
        if not (Decimal("0") < self.slo_target < Decimal("1")):
            raise SloError(
                "E_CONFIG_INVALID",
                "slo_target must satisfy 0 < target < 1",
                details={"slo_target": str(self.slo_target)},
            )
        if self.min_samples < 0:
            raise SloError("E_CONFIG_INVALID", "min_samples must be >= 0")
        if self.fixed_places < 0 or self.fixed_places > 12:
            raise SloError("E_CONFIG_INVALID", "fixed_places must be within 0..12")
        if self.rounding_mode != ROUNDING_MODE:
            raise SloError(
                "E_CONFIG_INVALID",
                f"only {ROUNDING_MODE} is supported for SLA output",
            )
        if not self.window_lengths_ms:
            raise SloError("E_CONFIG_INVALID", "at least one window is required")
        if any(value <= 0 for value in self.window_lengths_ms):
            raise SloError("E_CONFIG_INVALID", "window lengths must be > 0")
        if self.short_window_ms not in self.window_lengths_ms:
            raise SloError("E_CONFIG_INVALID", "short_window_ms must be a declared window")
        if self.long_window_ms not in self.window_lengths_ms:
            raise SloError("E_CONFIG_INVALID", "long_window_ms must be a declared window")
        if self.short_window_ms >= self.long_window_ms:
            raise SloError("E_CONFIG_INVALID", "short window must be shorter than long window")
        previous = Decimal("Infinity")
        for name, threshold in self.thresholds:
            if not name:
                raise SloError("E_CONFIG_INVALID", "threshold names must not be empty")
            if threshold <= 0 or threshold >= previous:
                raise SloError(
                    "E_CONFIG_INVALID",
                    "thresholds must be positive and strictly descending",
                )
            previous = threshold

    def to_dict(self) -> dict[str, Any]:
        return {
            "slo_target": fixed_decimal(self.slo_target, self.fixed_places),
            "min_samples": self.min_samples,
            "windows_ms": list(self.window_lengths_ms),
            "short_window_ms": self.short_window_ms,
            "long_window_ms": self.long_window_ms,
            "thresholds": {
                name: fixed_decimal(value, self.fixed_places)
                for name, value in self.thresholds
            },
            "include_late_events": self.include_late_events,
            "presentation": {
                "fixed_places": self.fixed_places,
                "rounding_mode": self.rounding_mode,
                "zero_total_sli": fixed_decimal(Decimal("1"), self.fixed_places),
                "zero_total_burn_rate": fixed_decimal(Decimal("0"), self.fixed_places),
            },
            "require_complete_coverage": self.require_complete_coverage,
        }


def fixed_decimal(value: Decimal, places: int) -> str:
    """Render a Decimal with an exact number of fractional places."""

    from decimal import ROUND_HALF_UP

    quantum = Decimal(1).scaleb(-places)
    return format(value.quantize(quantum, rounding=ROUND_HALF_UP), f".{places}f")


def parse_decimal(value: Any, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:  # noqa: BLE001 - convert to a business error
        raise SloError(
            "E_CONFIG_INVALID",
            f"{field_name} must be a decimal",
            details={"value": str(value)},
        ) from exc
    if not parsed.is_finite():
        raise SloError(
            "E_CONFIG_INVALID",
            f"{field_name} must be finite",
            details={"value": str(value)},
        )
    return parsed
