"""Millisecond time contract and deterministic TTL normalization report."""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, localcontext
from fractions import Fraction
from typing import Iterable

from .fixedpoint import q_div_int, q_to_string

DEFAULT_TTLS_MS = (300, 30_000, 300_000, 3_600_000, 7_200_000)


def expiry_from_grant(granted_at_ms: int, ttl_ms: int) -> int:
    if granted_at_ms < 0:
        raise ValueError("granted_at_ms must be non-negative")
    if ttl_ms <= 0:
        raise ValueError("ttl_ms must be positive")
    return granted_at_ms + ttl_ms


def remaining_ms(expires_at_ms: int, now_ms: int) -> int:
    return max(0, expires_at_ms - now_ms)


def renewal_margin_q(expires_at_ms: int, ttl_ms: int, now_ms: int) -> tuple[int, int]:
    remaining = min(remaining_ms(expires_at_ms, now_ms), ttl_ms)
    if ttl_ms <= 0:
        return 0, 0
    q, _ = q_div_int(remaining, ttl_ms)
    return max(0, min(1_000_000_000, q)), remaining


def renewal_margin_detail(
    expires_at_ms: int, ttl_ms: int, now_ms: int
) -> tuple[int, int, Fraction]:
    remaining = min(remaining_ms(expires_at_ms, now_ms), ttl_ms)
    if ttl_ms <= 0:
        return 0, 0, Fraction(0)
    q, error = q_div_int(remaining, ttl_ms)
    return max(0, min(1_000_000_000, q)), remaining, error


def _truncate_ratio(value: int, divisor: int, digits: int) -> tuple[str, Decimal]:
    with localcontext() as ctx:
        ctx.prec = 60
        exact = Decimal(value) / Decimal(divisor)
        quantum = Decimal(1).scaleb(-digits)
        truncated = exact.quantize(quantum, rounding=ROUND_DOWN)
        return f"{truncated:.{digits}f}", exact - truncated


def ttl_precision_report(
    ttl_values: Iterable[int] = DEFAULT_TTLS_MS,
    *,
    granted_at_ms: int = 1_700_000_000_000,
    now_ms: int | None = None,
    clock_offset_ms: int = 123,
) -> dict[str, object]:
    """Describe every conversion and inject a clock offset without mutation."""

    now = granted_at_ms if now_ms is None else now_ms
    rows: list[dict[str, object]] = []
    for ttl_ms in ttl_values:
        expires = expiry_from_grant(granted_at_ms, ttl_ms)
        seconds, seconds_error = _truncate_ratio(ttl_ms, 1_000, 3)
        minutes, minutes_error = _truncate_ratio(ttl_ms, 60_000, 6)
        hours, hours_error = _truncate_ratio(ttl_ms, 3_600_000, 9)
        remaining = remaining_ms(expires, now)
        margin_q, capped_remaining, margin_error = renewal_margin_detail(
            expires, ttl_ms, now
        )
        offset_now = now + clock_offset_ms
        offset_remaining = remaining_ms(expires, offset_now)
        offset_margin_q, offset_capped_remaining, offset_margin_error = renewal_margin_detail(
            expires, ttl_ms, offset_now
        )
        rows.append(
            {
                "ttl_ms": ttl_ms,
                "expires_at_ms": expires,
                "expiry_formula": "granted_at_ms + ttl_ms (integer milliseconds)",
                "seconds_fixed_3": seconds,
                "seconds_truncation_error_seconds": f"{seconds_error:.15f}",
                "minutes_fixed_6": minutes,
                "minutes_truncation_error_minutes": f"{minutes_error:.18f}",
                "hours_fixed_9": hours,
                "hours_truncation_error_hours": f"{hours_error:.21f}",
                "remaining_at_now_ms": remaining,
                "remaining_clamped_for_margin_ms": capped_remaining,
                "margin_ratio_q": q_to_string(margin_q),
                "margin_ratio_q_units": margin_q,
                "margin_rounding_error_q": str(margin_error),
                "clock_offset_ms": clock_offset_ms,
                "offset_now_ms": offset_now,
                "offset_remaining_ms": offset_remaining,
                "offset_margin_ratio_q": q_to_string(offset_margin_q),
                "offset_margin_rounding_error_q": str(offset_margin_error),
                "offset_remaining_clamped_for_margin_ms": offset_capped_remaining,
            }
        )
    return {
        "contract": {
            "clock": "UTC Unix epoch milliseconds as integers",
            "lease_expiry": "granted_at_ms + ttl_ms, exact integer addition",
            "renewal_expiry": "renewal_now_ms + ttl_ms, exact integer addition; no float seconds",
            "seconds_display": "ttl_ms / 1000, fixed 3 decimals, ROUND_DOWN/truncate",
            "minutes_display": "ttl_ms / 60000, fixed 6 decimals, ROUND_DOWN/truncate",
            "hours_display": "ttl_ms / 3600000, fixed 9 decimals, ROUND_DOWN/truncate",
            "margin_ratio": "min(remaining_ms, ttl_ms) / ttl_ms in Q units, half-even final rounding",
            "clock_offset_injection": "add offset_ms to system now for reads and writes; stored timestamps are never rewritten",
            "offset_effect": "positive offset reduces remaining time and therefore margin; expiry <= now clamps remaining and margin to zero",
        },
        "granted_at_ms": granted_at_ms,
        "now_ms": now,
        "rows": rows,
    }

