"""Cross-platform advisory file locks.

Windows uses :mod:`msvcrt` (``LK_LOCK`` region locking) while POSIX uses
:mod:`fcntl` (``flock``).  Both code paths are part of the standard library
so the project keeps its no-extra-dependency / no-GPU guarantee.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import TracebackType
from typing import Optional

_IS_WINDOWS = sys.platform == "win32"

if _IS_WINDOWS:  # pragma: no cover - exercised on Windows runners
    import msvcrt
else:
    import fcntl


class FileLock:
    """Context manager providing an exclusive lock backed by a lock file."""

    def __init__(self, path: os.PathLike[str] | str):
        self.path = Path(path)
        self._handle: Optional[int] = None

    def acquire(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o600)
        if _IS_WINDOWS:  # pragma: no cover
            # LK_LOCK retries internally for a short period then raises.
            msvcrt.locking(self._handle, msvcrt.LK_LOCK, 1)
        else:
            fcntl.flock(self._handle, fcntl.LOCK_EX)
        return self

    def release(self) -> None:
        if self._handle is None:
            return
        handle, self._handle = self._handle, None
        try:
            if _IS_WINDOWS:  # pragma: no cover
                # Seek back to the locked single byte before unlocking.
                os.lseek(handle, 0, os.SEEK_SET)
                msvcrt.locking(handle, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            os.close(handle)

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type: Optional[type[BaseException]],
                 exc: Optional[BaseException],
                 tb: Optional[TracebackType]) -> None:
        self.release()
