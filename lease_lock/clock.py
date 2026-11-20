from __future__ import annotations

import threading
import time


class LogicalClock:
    """Monotonic server clock with an explicitly persisted high-water mark.

    While a server process is alive, elapsed time comes from time.monotonic_ns().
    On restart, wall time and the persisted high-water mark are combined so the
    logical clock never moves backwards. Tests advance it through advance_ms()
    instead of sleeping.
    """

    def __init__(self, persisted_high_water_ms: int = 0, offset_ms: int = 0) -> None:
        wall_ms = time.time_ns() // 1_000_000
        self._anchor_mono_ns = time.monotonic_ns()
        self._base_ms = max(wall_ms, int(persisted_high_water_ms))
        self._offset_ms = max(0, int(offset_ms))
        self._last_ms = self._base_ms + self._offset_ms
        self._lock = threading.Lock()

    def now_ms(self) -> int:
        with self._lock:
            elapsed_ms = (time.monotonic_ns() - self._anchor_mono_ns) // 1_000_000
            candidate = self._base_ms + elapsed_ms + self._offset_ms
            if candidate < self._last_ms:
                candidate = self._last_ms
            self._last_ms = candidate
            return candidate

    def advance_ms(self, amount_ms: int) -> int:
        if amount_ms < 0:
            raise ValueError("clock advance must be non-negative")
        with self._lock:
            self._offset_ms += int(amount_ms)
            self._last_ms = max(self._last_ms, self._base_ms + self._offset_ms)
            return self._last_ms

    @property
    def high_water_ms(self) -> int:
        with self._lock:
            return self._last_ms

