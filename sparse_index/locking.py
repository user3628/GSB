"""Portable advisory file locks for workspace publication."""

from __future__ import annotations

import os
from pathlib import Path
from types import TracebackType


class FileLock:
    """A small cross-platform exclusive file lock.

    POSIX uses ``fcntl.flock``.  Windows uses ``msvcrt.locking`` on a single
    byte.  The lock file itself is never rewritten, only opened and locked.
    """

    def __init__(self, path: Path):
        self.path = path
        self._handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._handle = self.path.open("r+b")
        except FileNotFoundError:
            self._handle = self.path.open("w+b")
        try:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                import msvcrt

                self._handle.seek(0)
                if self._handle.read(1) == b"":
                    self._handle.seek(0)
                    self._handle.write(b"\0")
                    self._handle.flush()
                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        except Exception:
            self._handle.close()
            self._handle = None
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None
