"""Canonical serialization and atomic file helpers.

All externally visible JSON is serialized by :func:`dumps`.  The format is
stable across runs, processes and insertion order: UTF-8, sorted keys, compact
separators and one trailing newline.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Union


PathLike = Union[str, os.PathLike[str]]


def dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n"


def dump_bytes(value: Any) -> bytes:
    return dumps(value).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def digest_json(value: Any) -> str:
    return sha256_bytes(dump_bytes(value))


def digest_file(path: PathLike) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
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


def atomic_write_bytes(path: PathLike, data: bytes) -> None:
    """Write *data* to *path* without ever exposing a partial destination."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, destination)
        _fsync_directory(destination.parent)
    finally:
        if temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError:
                pass


def atomic_write_json(path: PathLike, value: Any) -> bytes:
    data = dump_bytes(value)
    atomic_write_bytes(path, data)
    return data


def read_json(path: PathLike) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def rounded(value: float, digits: int = 6) -> float:
    result = round(float(value), digits)
    if abs(result) < 0.5 * (10 ** -digits):
        return 0.0
    return result

