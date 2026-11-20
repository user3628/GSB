"""Shared deterministic utilities: canonical JSON, hashing, atomic IO, locks."""

from __future__ import annotations

import contextlib
import csv
import fcntl
import hashlib
import json
import os
import tempfile
import time

from .errors import LatencyBudgetExceeded, SampleError


def canonical_json(obj) -> bytes:
    """Canonical JSON bytes: sorted keys, tight separators, no NaN.

    Python's json uses the shortest round-trip repr for floats, so identical
    float64 values always serialize to identical bytes.
    """
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: str, data: bytes) -> None:
    """Write a file atomically: temp file + fsync + os.replace + dir fsync.

    A crash at any point leaves either the old file or nothing, never a
    half-written target file. Leftover temp files use a dot prefix and are
    ignored by journal scanners.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".atomic-", suffix=".part")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(directory)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


@contextlib.contextmanager
def file_lock(path: str):
    """Exclusive advisory file lock (fcntl.flock)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    f = open(path, "a+b")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield f
    finally:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        finally:
            f.close()


class Deadline:
    """Cooperative latency budget checked at stage / chunk boundaries."""

    def __init__(self, budget_ms):
        self.budget_ms = budget_ms
        self.started = time.monotonic()
        self._deadline = None if budget_ms is None else self.started + budget_ms / 1000.0

    def check(self, stage: str = "") -> None:
        if self._deadline is not None:
            now = time.monotonic()
            if now > self._deadline:
                raise LatencyBudgetExceeded(
                    "latency budget exceeded at stage '%s'" % stage,
                    {
                        "budget_ms": self.budget_ms,
                        "elapsed_ms": round((now - self.started) * 1000.0, 3),
                        "stage": stage,
                    },
                )

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.started) * 1000.0


def read_csv_columns(path: str):
    """Read a CSV into (header, {column: [string values]}); '' means missing."""
    with open(path, "r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows or not rows[0]:
        raise SampleError("empty CSV file", {"path": path})
    header = rows[0]
    if len(header) != len(set(header)):
        raise SampleError("duplicate column names", {"path": path})
    columns = {name: [] for name in header}
    for i, row in enumerate(rows[1:], start=2):
        if len(row) != len(header):
            raise SampleError(
                "ragged CSV row",
                {"path": path, "line": i, "expected": len(header), "got": len(row)},
            )
        for name, val in zip(header, row):
            columns[name].append(val)
    return header, columns


def first_diff_offset(a: bytes, b: bytes) -> int:
    """Offset of the first differing byte between two blobs (-1 if equal)."""
    n = min(len(a), len(b))
    for i in range(n):
        if a[i] != b[i]:
            return i
    return n if len(a) != len(b) else -1
