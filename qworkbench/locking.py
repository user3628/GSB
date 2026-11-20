from __future__ import annotations

import os
import multiprocessing
import time
from pathlib import Path
from typing import Any

if os.name == "nt":  # pragma: no cover - exercised on Windows
    import msvcrt
else:  # pragma: no cover - platform branch
    import fcntl


class FileLockTimeout(TimeoutError):
    pass


class FileLock:
    """Advisory process lock backed by fcntl on POSIX and msvcrt on Windows."""

    def __init__(self, path: Path, timeout: float = 10.0) -> None:
        self.path = path
        self.timeout = timeout
        self.fh: Any = None

    def acquire(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = self.path.open("a+b")
        if os.name == "nt":
            self.fh.seek(0, os.SEEK_END)
            if self.fh.tell() == 0:
                self.fh.write(b"\0")
                self.fh.flush()
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    self.fh.seek(0)
                    msvcrt.locking(self.fh.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        self.fh.close()
                        self.fh = None
                        raise FileLockTimeout(f"timed out acquiring {self.path}")
                    time.sleep(0.02)
        else:
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        self.fh.close()
                        self.fh = None
                        raise FileLockTimeout(f"timed out acquiring {self.path}")
                    time.sleep(0.02)
        return self

    def release(self) -> None:
        if self.fh is None:
            return
        try:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                self.fh.seek(0)
                msvcrt.locking(self.fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
        finally:
            self.fh.close()
            self.fh = None

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


def lock_backend() -> str:
    return "msvcrt.locking" if os.name == "nt" else "fcntl.flock"


def probe_lock(path: Path) -> dict[str, Any]:
    started = time.monotonic()
    with FileLock(path, timeout=2.0):
        held = True
    return {
        "ok": True,
        "backend": lock_backend(),
        "path": str(path),
        "held": held,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
    }


def _hold_lock_worker(path: str, hold_seconds: float, ready: Any) -> None:
    with FileLock(Path(path), timeout=5.0):
        ready.set()
        time.sleep(hold_seconds)


def probe_lock_contention(path: Path, hold_seconds: float = 0.35) -> dict[str, Any]:
    """Verify that a second process blocks while another process holds the lock."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    context = multiprocessing.get_context("spawn" if os.name == "nt" else "fork")
    ready = context.Event()
    process = context.Process(target=_hold_lock_worker, args=(str(path), hold_seconds, ready))
    started = time.monotonic()
    process.start()
    acquired = False
    wait_elapsed = 0.0
    try:
        if not ready.wait(5.0):
            raise FileLockTimeout("holder process did not acquire the probe lock")
        wait_started = time.monotonic()
        with FileLock(path, timeout=5.0):
            acquired = True
            wait_elapsed = time.monotonic() - wait_started
    finally:
        process.join(timeout=5.0)
        if process.is_alive():  # pragma: no cover - defensive cleanup
            process.terminate()
            process.join(timeout=2.0)
    return {
        "ok": acquired and process.exitcode == 0 and wait_elapsed >= hold_seconds * 0.5,
        "backend": lock_backend(),
        "path": str(path),
        "holder_pid": process.pid,
        "blocked_wait_ms": round(wait_elapsed * 1000, 3),
        "hold_seconds": hold_seconds,
        "total_elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "holder_exitcode": process.exitcode,
    }
