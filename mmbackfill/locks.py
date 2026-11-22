from __future__ import annotations

import os
import threading
import time
from pathlib import Path


_REGISTRY_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}


def _thread_lock_for(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _REGISTRY_GUARD:
        lock = _THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _THREAD_LOCKS[key] = lock
        return lock


class AdvisoryFileLock:
    """Blocking inter-process exclusive lock backed by the OS.

    POSIX uses flock(2); Windows uses msvcrt.locking. The descriptor remains
    open until release so the kernel, not a retry heuristic, owns mutual
    exclusion.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle = None
        self._thread_lock = _thread_lock_for(path)

    def __enter__(self) -> "AdvisoryFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._thread_lock.acquire()
        self._handle = self.path.open("a+b")
        if self._handle.tell() == 0:
            self._handle.write(b"\0")
            self._handle.flush()
        if os.name == "nt":
            import msvcrt

            while True:
                self._handle.seek(0)
                try:
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self._handle is not None:
                if os.name == "nt":
                    import msvcrt

                    self._handle.seek(0)
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
                self._handle.close()
                self._handle = None
        finally:
            self._thread_lock.release()
