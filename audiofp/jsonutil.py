"""Canonical JSON and atomic persistence helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any


def _normalise(value: Any, digits: int = 12) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalise(item, digits) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalise(item, digits) for item in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON cannot contain non-finite floats")
        rounded = round(value, digits)
        return 0.0 if rounded == 0 else rounded
    if hasattr(value, "item"):
        return _normalise(value.item(), digits)
    raise TypeError(f"unsupported canonical JSON value: {type(value)!r}")


def canonical_json_bytes(payload: Any) -> bytes:
    """Return stable UTF-8 JSON bytes, including one trailing newline."""

    normalised = _normalise(payload)
    text = json.dumps(
        normalised,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8")


def canonical_json_text(payload: Any) -> str:
    return canonical_json_bytes(payload).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def object_digest(payload: Any) -> str:
    return sha256_bytes(canonical_json_bytes(payload))


def version_fingerprint(*parts: Any) -> str:
    """Hash a labelled sequence of version inputs without ambiguity."""

    hasher = hashlib.sha256()
    for part in parts:
        encoded = canonical_json_bytes(part)
        hasher.update(len(encoded).to_bytes(8, "big"))
        hasher.update(encoded)
    return hasher.hexdigest()


def atomic_write_bytes(path: str | os.PathLike[str], data: bytes) -> None:
    """Atomically replace *path* after flushing file and directory metadata."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, target)
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # Directory fsync is unavailable on some Windows filesystems.
            pass
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass


def atomic_write_json(path: str | os.PathLike[str], payload: Any) -> bytes:
    data = canonical_json_bytes(payload)
    atomic_write_bytes(path, data)
    return data


def read_json(path: str | os.PathLike[str]) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)

