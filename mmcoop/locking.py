from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .errors import business_error


@contextmanager
def file_lock(path: Path, *, timeout_seconds: float = 30.0) -> Iterator[None]:
    """A real inter-process exclusive lock (flock on POSIX, msvcrt on Windows)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        start = time.monotonic()
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() - start >= timeout_seconds:
                        raise business_error(
                            "E_STALE_CHECKPOINT",
                            "publish",
                            "timed out waiting for the workspace publish lock",
                            diagnostics={"lock_path": str(path)},
                            http_status=409,
                        )
                    time.sleep(0.02)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() - start >= timeout_seconds:
                        raise business_error(
                            "E_STALE_CHECKPOINT",
                            "publish",
                            "timed out waiting for the workspace publish lock",
                            diagnostics={"lock_path": str(path)},
                            http_status=409,
                        )
                    time.sleep(0.02)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
