from __future__ import annotations

import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import EngineError

_DECIMAL_RE = re.compile(r"^[+-]?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


@dataclass(frozen=True)
class ScaledValue:
    scaled: int
    scale: int
    kind: str

    @property
    def decimal(self) -> Decimal:
        return Decimal(self.scaled).scaleb(-self.scale)


def parse_event_measure(
    value: Any,
    kind: str,
    scale: int,
    *,
    batch_id: str,
    event_id: str,
    event_index: int,
) -> ScaledValue | None:
    """Validate and convert a declaration-bound measure without floating point."""
    if value is None:
        return None

    context = {
        "batch_id": batch_id,
        "event_id": event_id,
        "event_index": event_index,
        "declared_type": {"kind": kind, "scale": scale},
        "received": repr(value),
    }

    if kind == "integer":
        if type(value) is not int:
            raise EngineError(
                "E_MEASURE_TYPE_MISMATCH",
                "integer measure declaration requires a JSON integer or null",
                context,
            )
        return ScaledValue(scaled=value, scale=0, kind="integer")

    if kind != "decimal":
        raise EngineError(
            "E_INVALID_REQUEST",
            "measure type kind must be integer or decimal",
            {"kind": kind},
        )

    if type(value) is int:
        return ScaledValue(scaled=value * (10**scale), scale=scale, kind="decimal")

    if type(value) is float:
        number = Decimal(str(value))
    elif isinstance(value, str) and value and _DECIMAL_RE.fullmatch(value):
        number = Decimal(value)
    else:
        raise EngineError(
            "E_MEASURE_TYPE_MISMATCH",
            "decimal measure declaration requires an exact decimal number, string, integer, or null",
            context,
        )

    try:
        scaled_decimal = number.scaleb(scale)
        integral = scaled_decimal.to_integral_value()
        if not scaled_decimal.is_finite() or scaled_decimal != integral:
            raise InvalidOperation
        return ScaledValue(scaled=int(integral), scale=scale, kind="decimal")
    except (InvalidOperation, ValueError, OverflowError):
        raise EngineError(
            "E_MEASURE_TYPE_MISMATCH",
            "measure is not exactly representable at the declared decimal scale",
            context,
        ) from None


def rescale(scaled: int, from_scale: int, to_scale: int) -> int:
    if to_scale >= from_scale:
        return scaled * (10 ** (to_scale - from_scale))
    divisor = 10 ** (from_scale - to_scale)
    if scaled % divisor:
        raise EngineError(
            "E_INTERNAL",
            "inexact fixed-point rescale attempted",
            {"scaled": scaled, "from_scale": from_scale, "to_scale": to_scale},
        )
    return scaled // divisor


def format_scaled(scaled: int | None, scale: int) -> str | None:
    if scaled is None:
        return None
    if scale == 0:
        return str(scaled)
    sign = "-" if scaled < 0 else ""
    digits = str(abs(scaled)).rjust(scale + 1, "0")
    return f"{sign}{digits[:-scale]}.{digits[-scale:]}"


def format_measure(scaled: int | None, scale: int) -> int | str | None:
    if scaled is None:
        return None
    if scale == 0:
        return scaled
    return format_scaled(scaled, scale)


def nearest_rank(values: list[tuple[int, int]], quantile: float, target_scale: int) -> dict[str, Any]:
    if not values:
        return {
            "quantile": quantile,
            "value": None,
            "value_scaled": None,
            "scale": target_scale,
            "method": "nearest_rank",
        }
    ordered = sorted(values, key=lambda pair: Decimal(pair[0]).scaleb(-pair[1]))
    index = max(0, min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1))
    scaled, scale = ordered[index]
    scaled = rescale(scaled, scale, target_scale)
    return {
        "quantile": quantile,
        "value": format_measure(scaled, target_scale),
        "value_scaled": str(scaled),
        "scale": target_scale,
        "method": "nearest_rank",
    }


def canonical_key(kind: str, scale: int) -> str:
    return f"{kind}:{scale}"
