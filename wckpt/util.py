"""Deterministic helpers: canonical JSON, checksums, and a read/write lock."""
from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Any

MAGIC_HEADER = "WCKPT-WAL-SEG/1"
MAGIC_RECORD = "REC"
MAGIC_FOOTER = "END"


def canonical_json(obj: Any) -> str:
    """Stable JSON text used for checksum computation and persistence."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def crc32_hex(obj: Any) -> str:
    import zlib

    return format(zlib.crc32(canonical_json(obj).encode("utf-8")) & 0xFFFFFFFF, "08x")


def fsync_dir(path: str) -> None:
    """fsync a directory so that file creation/rename is durable."""
    if not os.path.isdir(path):
        return
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class RWLock:
    """Many concurrent readers or one exclusive writer, no writer starvation."""

    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    def acquire_read(self):
        with self._cond:
            while self._writer or self._writers_waiting:
                self._cond.wait()
            self._readers += 1

    def release_read(self):
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self):
        with self._cond:
            self._writers_waiting += 1
            try:
                while self._writer or self._readers:
                    self._cond.wait()
                self._writer = True
            finally:
                self._writers_waiting -= 1

    def release_write(self):
        with self._cond:
            self._writer = False
            self._cond.notify_all()

    class _Ctx:
        def __init__(self, lock, write: bool):
            self._lock = lock
            self._write = write

        def __enter__(self):
            if self._write:
                self._lock.acquire_write()
            else:
                self._lock.acquire_read()
            return self

        def __exit__(self, exc_type, exc, tb):
            if self._write:
                self._lock.release_write()
            else:
                self._lock.release_read()

    def read(self):
        return self._Ctx(self, False)

    def write(self):
        return self._Ctx(self, True)
