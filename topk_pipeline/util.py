"""Deterministic serialization, fingerprints, atomic files and file locks."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

import numpy as np


SCHEMA_VERSION = 1


def canonical_json_bytes(value: Any) -> bytes:
    """Encode a value without platform-dependent whitespace or key ordering."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def digest_value(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def pretty_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def array_fingerprint(array: np.ndarray, *, max_rows_per_chunk: int = 64) -> str:
    """Hash dtype, shape and C-order payload without forcing a full-size copy.

    Chunk boundaries are not part of the hash, only the ordered payload bytes,
    so a memory-mapped array and an ordinary ndarray produce the same value.
    """

    hasher = hashlib.sha256()
    hasher.update(b"numpy-array-v1\0")
    hasher.update(array.dtype.str.encode("ascii"))
    hasher.update(b"\0")
    hasher.update(json.dumps(list(array.shape), separators=(",", ":")).encode("ascii"))
    hasher.update(b"\0")
    if array.ndim == 0:
        hasher.update(memoryview(np.ascontiguousarray(array)).cast("B"))
        return hasher.hexdigest()
    if array.ndim != 2:
        hasher.update(memoryview(np.ascontiguousarray(array)).cast("B"))
        return hasher.hexdigest()
    for row_start in range(0, int(array.shape[0]), max(1, int(max_rows_per_chunk))):
        block = np.ascontiguousarray(array[row_start : row_start + max_rows_per_chunk, :])
        hasher.update(memoryview(block).cast("B"))
    return hasher.hexdigest()


def finite_or_negative_inf(value: float) -> float | str:
    """JSON-safe numeric representation for -inf diagnostics only."""

    if value == -math.inf:
        return "-Infinity"
    if not math.isfinite(value):
        raise ValueError("non-finite value cannot enter a checkpoint")
    return float(value)


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    """Write and fsync a temporary file, then atomically replace the target."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp.{os.getpid()}.{time.time_ns()}"
    try:
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
        if os.name != "nt":
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, pretty_json_bytes(value))


class FileLock(AbstractContextManager["FileLock"]):
    """Cross-platform advisory exclusive lock backed by a dedicated file."""

    def __init__(self, path: str | Path, *, timeout: float = 60.0, poll: float = 0.01):
        self.path = Path(path)
        self.timeout = timeout
        self.poll = poll
        self._fd: int | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.monotonic() + self.timeout
        try:
            if os.name == "nt":
                import msvcrt

                if os.fstat(fd).st_size == 0:
                    os.write(fd, b"\0")
                    os.fsync(fd)
                while True:
                    try:
                        os.lseek(fd, 0, os.SEEK_SET)
                        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError(f"timed out waiting for file lock {self.path}")
                        time.sleep(self.poll)
            else:
                import fcntl

                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError(f"timed out waiting for file lock {self.path}")
                        time.sleep(self.poll)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._fd is None:
            return
        fd = self._fd
        self._fd = None
        try:
            if os.name == "nt":
                import msvcrt

                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def read_json(path: str | Path) -> Any:
    with open(path, "rb") as handle:
        return json.load(handle)


def as_plain_number(value: Any) -> float:
    if isinstance(value, str) and value == "-Infinity":
        return -math.inf
    return float(value)


def ensure_parent(path: str | Path) -> Path:
    result = Path(path)
    result.parent.mkdir(parents=True, exist_ok=True)
    return result
