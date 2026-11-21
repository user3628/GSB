from __future__ import annotations

import fcntl
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .errors import CabinError


class FileLock:
    def __init__(self, path: str | Path, *, timeout_s: float = 10.0) -> None:
        self.path = Path(path)
        self.timeout_s = timeout_s
        self._fd: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        deadline = time.monotonic() + self.timeout_s
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise CabinError(
                            "E_LOCK_TIMEOUT",
                            "workspace lock acquisition timed out",
                            details={"lock": str(self.path), "timeout_s": self.timeout_s},
                            http_status=503,
                        )
                    time.sleep(0.01)
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode("ascii"))
            os.fsync(fd)
            self._fd = fd
        except BaseException:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
            raise

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()


class LockSet:
    def __init__(self, root: str | Path, *, timeout_s: float = 10.0) -> None:
        self.root = Path(root)
        self.timeout_s = timeout_s

    @contextmanager
    def hold(self, name: str) -> Iterator[None]:
        lock = FileLock(self.root / f"{name}.lock", timeout_s=self.timeout_s)
        with lock:
            yield
