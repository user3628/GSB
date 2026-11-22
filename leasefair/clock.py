"""Injectable millisecond clocks.

All lease arithmetic uses integer Unix epoch milliseconds.  The offset is the
only supported clock-injection mechanism; it never changes persisted timestamps
retroactively.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


def system_now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(slots=True)
class Clock:
    offset_ms: int = 0

    def now_ms(self) -> int:
        return system_now_ms() + self.offset_ms


@dataclass(slots=True)
class FixedClock:
    """A deterministic clock for samples and tests."""

    now: int

    def now_ms(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> int:
        self.now += delta_ms
        return self.now

