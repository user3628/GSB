"""Small thread-safe counter set for the admin interface and tests."""

from __future__ import annotations

import threading
import time
from collections import Counter
from typing import Any


class Stats:
    def __init__(self) -> None:
        self.started_at = time.time()
        self._lock = threading.RLock()
        self._counters: Counter[str] = Counter()
        self._errors: Counter[str] = Counter()

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += int(amount)

    def error(self, code: str) -> None:
        with self._lock:
            self._errors[code] += 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "started_at": self.started_at,
                "uptime_seconds": max(0.0, time.time() - self.started_at),
                "counters": dict(self._counters),
                "errors": dict(self._errors),
            }
