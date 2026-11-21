"""Portable real file locks with a structured timeout.

POSIX uses fcntl.flock, Windows uses msvcrt.locking; both are genuine OS
locks. Acquisition retries the non-blocking syscall until the deadline and
then raises E_LOCK_TIMEOUT with structured details - the lock itself is
never simulated by fixed sleeps.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from .errors import E_LOCK_TIMEOUT, EngineError

if os.name == "nt":  # pragma: no cover - exercised on Windows only
    import msvcrt

    def _lock_nb(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock_nb(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


class FileLock:
    """Context manager acquiring an exclusive OS file lock with a timeout."""

    def __init__(self, path, timeout_ms: int = 5000, poll_interval_s: float = 0.05):
        self.path = Path(path)
        self.timeout_ms = int(timeout_ms)
        self.poll_interval_s = float(poll_interval_s)
        self._fh = None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+b")
        deadline = time.monotonic() + self.timeout_ms / 1000.0
        while True:
            try:
                _lock_nb(fh)
                self._fh = fh
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    fh.close()
                    raise EngineError(
                        E_LOCK_TIMEOUT,
                        f"timed out acquiring lock '{self.path.name}'",
                        {
                            "lock_path": str(self.path),
                            "timeout_ms": self.timeout_ms,
                        },
                    )
                time.sleep(self.poll_interval_s)

    def release(self):
        if self._fh is not None:
            try:
                _unlock(self._fh)
            finally:
                self._fh.close()
                self._fh = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False
