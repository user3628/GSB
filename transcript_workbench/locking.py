from __future__ import annotations

import os
import time
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

from .errors import WorkbenchError


class FileLock:
    """Cross-platform exclusive lock backed by a stable lock file."""

    def __init__(self, path: Path, timeout_s: float = 10.0) -> None:
        self.path = path
        self.timeout_s = timeout_s
        self._handle: BinaryIO | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        deadline = time.monotonic() + self.timeout_s
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._handle = handle
                return self
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    handle.close()
                    raise WorkbenchError(
                        "E_LOCK_TIMEOUT",
                        "Timed out waiting for the workspace lock",
                        {"lock_path": str(self.path), "timeout_s": self.timeout_s},
                    )
                time.sleep(0.025)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
