"""Small cross-platform advisory file lock."""

from __future__ import annotations

import os
import time
from pathlib import Path
from types import TracebackType
from typing import Self

from .errors import E_IO, AuditError


class FileLock:
    def __init__(self, path: str | Path, timeout: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._handle = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        deadline = time.monotonic() + self.timeout
        if os.name == "nt":
            import msvcrt

            self._handle.seek(0, os.SEEK_END)
            if self._handle.tell() == 0:
                self._handle.write(b"\0")
                self._handle.flush()
                os.fsync(self._handle.fileno())

            while True:
                try:
                    self._handle.seek(0)
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        self._handle.close()
                        self._handle = None
                        raise AuditError(E_IO, "timed out acquiring audit workspace lock", path=str(self.path))
                    time.sleep(0.05)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        self._handle.close()
                        self._handle = None
                        raise AuditError(E_IO, "timed out acquiring audit workspace lock", path=str(self.path))
                    time.sleep(0.05)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
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

