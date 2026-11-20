"""Cross-platform advisory file locking.

Windows uses msvcrt.locking, POSIX uses fcntl.flock. The lock file itself
is never deleted, so concurrent lockers always synchronise on the same
inode.
"""

from __future__ import annotations

import os
from contextlib import contextmanager

if os.name == "nt":  # pragma: no cover - exercised on Windows only
    import msvcrt

    @contextmanager
    def file_lock(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fh = open(path, "a+b")
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)
            yield fh
        finally:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            fh.close()

else:
    import fcntl

    @contextmanager
    def file_lock(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fh = open(path, "a+b")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            yield fh
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            fh.close()
