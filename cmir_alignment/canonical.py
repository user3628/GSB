"""Canonical serialization, hashing, atomic writes and real file locks."""

from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator

import numpy as np


def _native(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return [_native(item) for item in value]
    if isinstance(value, list):
        return [_native(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _native(item) for key, item in value.items()}
    return value


def _digestable(value: Any) -> Any:
    value = _native(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float cannot be canonicalized")
        return {"$float_hex": value.hex()}
    if isinstance(value, list):
        return [_digestable(item) for item in value]
    if isinstance(value, dict):
        return {key: _digestable(item) for key, item in sorted(value.items())}
    return value


def canonical_json(value: Any) -> str:
    """Deterministic, human-readable JSON with finite floats only."""
    return json.dumps(
        _native(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_bytes_sha256(data: bytes) -> str:
    return digest_bytes(data)


def digest_obj(value: Any) -> str:
    """Hash exact values, including float bit patterns expressed as hex."""
    payload = json.dumps(
        _digestable(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return digest_bytes(payload.encode("utf-8"))


def digest_file(path: str | Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def b64_array(array: np.ndarray) -> dict[str, Any]:
    array = np.ascontiguousarray(array)
    return {
        "dtype": str(array.dtype),
        "shape": list(array.shape),
        "data_b64": base64.b64encode(array.tobytes(order="C")).decode("ascii"),
    }


def array_from_b64(value: dict[str, Any]) -> np.ndarray:
    dtype = np.dtype(value["dtype"])
    raw = base64.b64decode(value["data_b64"].encode("ascii"))
    return np.frombuffer(raw, dtype=dtype).reshape(tuple(value["shape"])).copy()


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    """Write one complete file via fsync + same-directory atomic replace."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_dir(destination.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_bytes(value) + b"\n")


def read_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def try_read_json(path: str | Path) -> tuple[Any | None, str | None]:
    try:
        return read_json(path), None
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return None, str(exc)


@contextlib.contextmanager
def file_lock(path: str | Path, *, exclusive: bool = True) -> Iterator[None]:
    """Cross-process advisory lock backed by fcntl.flock."""
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
