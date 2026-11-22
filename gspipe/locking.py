"""True cross-process file locks for Windows and POSIX hosts."""

from __future__ import annotations

import os
import time
from pathlib import Path

from .errors import E_LOCK_TIMEOUT, PipelineError
from .fsutil import ensure_dir


class FileLock:
    """Advisory exclusive lock backed by the operating system.

    POSIX uses ``fcntl.flock`` and Windows uses ``msvcrt.locking``.  The lock
    file contains one sentinel byte because Windows byte-range locking requires
    an existing byte.
    """

    def __init__(self, path: str | Path, timeout: float = 15.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._handle = None
        self._locked = False

    def acquire(self) -> "FileLock":
        ensure_dir(self.path.parent)
        handle = self.path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._locked = True
                self._handle = handle
                return self
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    handle.close()
                    raise PipelineError(
                        E_LOCK_TIMEOUT,
                        f"Timed out waiting for file lock {self.path}",
                        details={"path": str(self.path), "timeout_seconds": self.timeout},
                    )
                time.sleep(0.01)

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            if self._locked:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._locked = False
            self._handle = None
            handle.close()

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
