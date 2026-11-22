from __future__ import annotations

import math
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any

RATIO_SCALE = 1_000_000_000
CARD_SCALE = 1_000_000
COST_SCALE = 1_000_000
MIN_SELECTIVITY = 1_000
MAX_ROWS = (2**63) - 1
MAX_INTERMEDIATE_ROWS = (2**63) - 1
CARD_CAP = MAX_INTERMEDIATE_ROWS * CARD_SCALE


def clamp_int(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def decimal_from(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise ValueError("boolean is not a number")
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite number")
        return Decimal(str(value))
    if isinstance(value, str):
        return Decimal(value)
    raise ValueError(f"unsupported numeric value: {type(value).__name__}")


def decimal_places(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    return max(0, -int(exponent))


def quantize_ratio(value: Any) -> tuple[int, Decimal, int]:
    source = decimal_from(value)
    scaled = source * RATIO_SCALE
    quantized = int(scaled.to_integral_value(rounding=ROUND_HALF_EVEN))
    error = scaled - Decimal(quantized)
    return quantized, error, decimal_places(source)


def format_fixed(units: int, scale: int = COST_SCALE, places: int | None = None) -> str:
    if places is None:
        places = int(round(math.log10(scale)))
    if places < 0:
        raise ValueError("places must be non-negative")
    sign = "-" if units < 0 else ""
    absolute = abs(units)
    target = Decimal(10) ** places
    quantized = (Decimal(absolute) * target / Decimal(scale)).to_integral_value(
        rounding=ROUND_HALF_EVEN
    )
    whole, fraction = divmod(int(quantized), int(target))
    if places == 0:
        return f"{sign}{whole}"
    return f"{sign}{whole}.{fraction:0{places}d}"


def format_decimal(value: Decimal | float, places: int = 12) -> str:
    quant = Decimal(1).scaleb(-places)
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(decimal_value.quantize(quant, rounding=ROUND_HALF_EVEN), "f")


def tolerance_is_close(a: int, b: int, tolerance_ppm: int) -> bool:
    if a == b:
        return True
    high = max(a, b)
    if high <= 0:
        return False
    return abs(a - b) * 1_000_000 <= high * tolerance_ppm


def rows_to_card_fixed(rows: int) -> int:
    if rows <= 0:
        return 0
    return min(rows * CARD_SCALE, CARD_CAP)


def log2_to_card_fixed(log2_rows: float | None) -> tuple[int, int, bool, bool]:
    if log2_rows is None:
        return 0, 0, False, True
    if log2_rows <= 0:
        rows = 1
        fixed = CARD_SCALE
        return fixed, rows, False, False
    capped = False
    max_log = math.log2(MAX_INTERMEDIATE_ROWS)
    if log2_rows >= max_log:
        log2_rows = max_log
        capped = True
    rows_float = 2.0**log2_rows
    if rows_float < 1.0:
        rows = 1
    else:
        rows = min(int(math.ceil(rows_float)), MAX_INTERMEDIATE_ROWS)
    fixed_float = rows_float * CARD_SCALE
    fixed = int(Decimal(str(fixed_float)).to_integral_value(rounding=ROUND_HALF_EVEN))
    fixed = clamp_int(fixed, 0, CARD_CAP)
    return fixed, rows, capped, False

