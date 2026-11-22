"""Atomic file writes and cross-platform advisory file locks.

* ``atomic_write_bytes`` writes to a temporary file in the same directory,
  fsyncs it and then publishes it with ``os.replace`` so a crash at any
  point never leaves a half-written record behind.
* ``file_lock`` provides an exclusive advisory lock using ``msvcrt`` on
  Windows and ``fcntl`` on POSIX systems.
"""
from __future__ import annotations

import contextlib
import os
import tempfile
from typing import Iterator

if os.name == "nt":  # pragma: no cover - exercised on Windows only
    import msvcrt

    def _lock(fh) -> None:
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock(fh) -> None:
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock(fh) -> None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

    def _unlock(fh) -> None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def atomic_write_bytes(path: str, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically (no half-written records)."""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".part")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


@contextlib.contextmanager
def file_lock(lock_path: str) -> Iterator[None]:
    """Exclusive advisory lock guarding concurrent checkpoint publication."""
    os.makedirs(os.path.dirname(os.path.abspath(lock_path)), exist_ok=True)
    with open(lock_path, "a+b") as fh:
        _lock(fh)
        try:
            yield
        finally:
            _unlock(fh)
