from __future__ import annotations

SCALE = 1_000_000
MAX_SAFE_INT = (1 << 63) - 1


class FixedPointError(ValueError):
    pass


def clamp(value: int, low: int, high: int) -> int:
    if low > high:
        raise FixedPointError("invalid clamp interval")
    return low if value < low else high if value > high else value


def checked_mul_div(value: int, numerator: int, denominator: int) -> int:
    """Integer-only value * numerator / denominator with explicit bounds checks."""
    if denominator <= 0:
        raise FixedPointError("denominator must be positive")
    if value < 0 or numerator < 0:
        raise FixedPointError("fixed-point operands must be non-negative")
    if value and numerator > MAX_SAFE_INT // value:
        raise FixedPointError("fixed-point multiplication would overflow")
    result = (value * numerator) // denominator
    if result > MAX_SAFE_INT:
        raise FixedPointError("fixed-point result exceeds safe integer range")
    return result


def ratio_ppm(numerator: int, denominator: int, *, cap: int | None = None) -> int:
    if denominator <= 0:
        raise FixedPointError("ratio denominator must be positive")
    if numerator <= 0:
        return 0
    value = checked_mul_div(numerator, SCALE, denominator)
    if cap is not None:
        value = min(value, cap)
    return value


def damp_toward(previous: int, target: int, damping_ppm: int) -> int:
    if not (0 <= damping_ppm <= SCALE):
        raise FixedPointError("damping must be in [0, 1]")
    delta = target - previous
    if delta == 0:
        return previous
    if delta > 0:
        return previous + checked_mul_div(delta, damping_ppm, SCALE)
    return previous - checked_mul_div(-delta, damping_ppm, SCALE)


def apply_scale(base_value: int, scale_ppm: int, *, min_scale: int, max_scale: int) -> int:
    bounded = clamp(scale_ppm, min_scale, max_scale)
    return checked_mul_div(base_value, bounded, SCALE)
