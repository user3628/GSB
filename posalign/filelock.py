"""Small cross-platform advisory file lock used by publication ledgers."""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .errors import BusinessError, E_STALE_CHECKPOINT


_THREAD_LOCKS: dict[str, threading.Lock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


def _thread_lock_for(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _THREAD_LOCKS_GUARD:
        return _THREAD_LOCKS.setdefault(key, threading.Lock())


@contextmanager
def exclusive_file_lock(
    path: str | Path,
    *,
    timeout: float = 30.0,
) -> Iterator[None]:
    """Take a real OS file lock plus an in-process lock.

    POSIX uses ``flock``. Windows uses ``msvcrt.locking``. The lock file is
    deliberately persistent; it carries no business state.
    """

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    thread_lock = _thread_lock_for(target)
    deadline = time.monotonic() + timeout
    if not thread_lock.acquire(timeout=timeout):
        raise BusinessError(
            E_STALE_CHECKPOINT,
            "timed out acquiring process lock",
            {"lock": str(target)},
        )
    handle = None
    try:
        handle = os.open(str(target), os.O_RDWR | os.O_CREAT, 0o600)
        if os.name == "nt":
            import msvcrt

            if os.fstat(handle).st_size == 0:
                os.write(handle, b"\0")
                os.fsync(handle)

            while True:
                try:
                    os.lseek(handle, 0, os.SEEK_SET)
                    msvcrt.locking(handle, msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise BusinessError(
                            E_STALE_CHECKPOINT,
                            "timed out acquiring file lock",
                            {"lock": str(target)},
                        )
                    time.sleep(0.01)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise BusinessError(
                            E_STALE_CHECKPOINT,
                            "timed out acquiring file lock",
                            {"lock": str(target)},
                        )
                    time.sleep(0.01)
        yield
    finally:
        if handle is not None:
            try:
                if os.name == "nt":
                    import msvcrt

                    os.lseek(handle, 0, os.SEEK_SET)
                    msvcrt.locking(handle, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle, fcntl.LOCK_UN)
            finally:
                os.close(handle)
        thread_lock.release()

