"""Cross-platform advisory file locking.

POSIX uses ``fcntl.flock``. Windows uses ``msvcrt.locking`` on a one-byte lock
file. Both are real OS file locks, not process-local locks.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from .util import ensure_dir


class FileLock:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._handle = None

    def __enter__(self) -> "FileLock":
        ensure_dir(self.path.parent)
        self._handle = self.path.open("a+b")
        if os.name == "nt":
            import msvcrt

            self._handle.seek(0, os.SEEK_END)
            if self._handle.tell() == 0:
                self._handle.write(b"\0")
                self._handle.flush()
            self._handle.seek(0)
            while True:
                try:
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None
