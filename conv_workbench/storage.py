"""Portable standard-library file locking and atomic persistence."""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

from .canonical import canonical_json


class FileLock:
    """An exclusive inter-process lock using only the standard library.

    POSIX uses ``fcntl.flock`` and Windows uses ``msvcrt.locking``.  A separate
    lock file is used per shard so unrelated shards can progress independently.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+b")
        if os.name == "nt":
            import msvcrt

            self._handle.seek(0)
            if self.path.stat().st_size == 0:
                self._handle.write(b"\0")
                self._handle.flush()
            self._handle.seek(0)
            msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
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


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with open(temporary, "xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        fsync_directory(destination.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_json(path: str | Path, value: Any, *, pretty: bool = False) -> None:
    if pretty:
        text = json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True) + "\n"
    else:
        text = canonical_json(value) + "\n"
    atomic_write_bytes(path, text.encode("utf-8"))


def read_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def remove_if_exists(path: str | Path) -> None:
    try:
        Path(path).unlink()
    except FileNotFoundError:
        return


def python_environment() -> dict[str, Any]:
    return {
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "pid": os.getpid(),
    }

