"""Timestamp unit normalization, rounding accounting and retention math.

Canonical internal unit is integer microseconds (us). Binary metric
timestamps arrive as milliseconds (ms) and JSON-lines as microseconds (us).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

# Supported truncation rules for the sub-millisecond remainder when a us value
# is projected onto the ms grid (used when exporting to the binary format and
# when a caller explicitly asks for ms-grid normalization).
TRUNCATE = "truncate"  # floor to the ms grid, discarding the remainder
ROUND = "round"        # nearest ms, ties round half away from zero
PRESERVE = "preserve"  # keep full us precision (no projection)

_US_PER_MS = 1000


@dataclass
class RoundingRecord:
    """Per-record rounding bookkeeping for one timestamp projection."""

    run_id: str
    original_us: int
    projected_us: int
    rule: str
    rounded_digits: int          # sub-ms digits removed by the projection
    discarded_us: int            # signed remainder actually dropped
    cumulative_error_us: int     # running sum of discarded_us within a batch

    def to_dict(self):
        return {
            "run_id": self.run_id,
            "original_us": self.original_us,
            "projected_us": self.projected_us,
            "rule": self.rule,
            "rounded_digits": self.rounded_digits,
            "discarded_us": self.discarded_us,
            "cumulative_error_us": self.cumulative_error_us,
        }


def ms_to_us(ms: int) -> int:
    return int(ms) * _US_PER_MS


def us_to_ms_grid(time_us: int, rule: str = TRUNCATE) -> int:
    """Project a microsecond timestamp onto the millisecond grid."""
    if rule == PRESERVE:
        return time_us
    quotient, remainder = divmod(time_us, _US_PER_MS)
    if rule == TRUNCATE:
        return quotient * _US_PER_MS
    if rule == ROUND:
        if remainder * 2 >= _US_PER_MS:
            return (quotient + 1) * _US_PER_MS
        return quotient * _US_PER_MS
    raise ValueError(f"unknown truncation rule: {rule!r}")


def normalize_metric_time(run_id: str, time_us: int, rule: str) -> RoundingRecord:
    """Normalize one already-microsecond timestamp and account for rounding."""
    projected = us_to_ms_grid(time_us, rule)
    discarded = projected - time_us  # negative when truncating a positive rem
    # Number of sub-ms digits removed: a ms grid removes 3 decimal digits of us.
    rounded_digits = 3 if rule != PRESERVE and projected != time_us else 0
    return RoundingRecord(
        run_id=run_id,
        original_us=time_us,
        projected_us=projected,
        rule=rule,
        rounded_digits=rounded_digits,
        discarded_us=discarded,
        cumulative_error_us=discarded,
    )


def normalize_batch(records: List[tuple], rule: str) -> List[RoundingRecord]:
    """Normalize a batch of ``(run_id, time_us)`` tuples with running error."""
    out: List[RoundingRecord] = []
    cumulative = 0
    for run_id, time_us in records:
        record = normalize_metric_time(run_id, int(time_us), rule)
        cumulative += record.discarded_us
        record.cumulative_error_us = cumulative
        out.append(record)
    return out


def retention_to_expiry(base_time_us: int, retention_seconds: float) -> int:
    """Convert a relative retention period into an absolute expiry instant."""
    return int(base_time_us) + int(round(float(retention_seconds) * 1_000_000))
