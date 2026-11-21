"""Canonical, deterministic serialization helpers.

All persisted bytes in the engine are produced through these helpers so that
identical inputs always yield byte-identical artifacts:
  * JSON is canonical (sorted keys, compact separators, NaN forbidden).
  * Floats are stored as little-endian IEEE-754 float64.
  * Atomic writes use tmp-file + fsync + os.replace so an interrupted write
    never leaves a half-written record behind.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

import numpy as np


def canonical_json(obj) -> bytes:
    """Deterministic JSON encoding (sorted keys, compact, UTF-8)."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_json(obj) -> str:
    return sha256_hex(canonical_json(obj))


def f8(values) -> np.ndarray:
    """Array as little-endian float64 (canonical float storage)."""
    return np.asarray(values, dtype="<f8")


def i4(values) -> np.ndarray:
    return np.asarray(values, dtype="<i4")


def u4(values) -> np.ndarray:
    return np.asarray(values, dtype="<u4")


def atomic_write_bytes(path, data: bytes) -> None:
    """Write bytes atomically: tmp file in same dir, fsync, os.replace.

    os.replace is atomic on POSIX and Windows, so readers never observe a
    partially written file and a crash leaves at most an orphan *.tmp file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_json(path, obj) -> None:
    atomic_write_bytes(path, canonical_json(obj))


def read_file_bytes(path) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()
