"""Cross-process file locks with a process-local guard for thread safety."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from types import TracebackType


_LOCKS_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[str, threading.RLock] = {}


def _process_lock(path: Path) -> threading.RLock:
    key = str(path.resolve())
    with _LOCKS_GUARD:
        lock = _PROCESS_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _PROCESS_LOCKS[key] = lock
        return lock


class FileLock:
    """An advisory exclusive lock backed by ``fcntl`` or ``msvcrt``."""

    def __init__(self, path: str | Path, timeout: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._file = None
        self._process = _process_lock(self.path)

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._process.acquire()
        try:
            self._file = open(self.path, "a+b")
            if os.path.getsize(self.path) == 0:
                self._file.write(b"\0")
                self._file.flush()
            self._file.seek(0)
            if os.name == "nt":
                self._lock_windows()
            else:
                self._lock_posix()
        except BaseException:
            if self._file is not None:
                self._file.close()
                self._file = None
            self._process.release()
            raise
        return self

    def _lock_posix(self) -> None:
        import fcntl

        assert self._file is not None
        fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)

    def _lock_windows(self) -> None:
        import msvcrt

        assert self._file is not None
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for file lock {self.path}")
                time.sleep(0.02)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if self._file is not None:
                try:
                    if os.name == "nt":
                        import msvcrt

                        self._file.seek(0)
                        msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
                finally:
                    self._file.close()
                    self._file = None
        finally:
            self._process.release()

