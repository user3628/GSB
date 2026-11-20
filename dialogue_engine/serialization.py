"""Canonical JSON, deterministic rounding, hashing, and atomic file writes."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any


def _finite_number(value: float, decimals: int) -> float:
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("non-finite number cannot be serialized")
    rounded = round(value, decimals)
    if rounded == 0.0:
        return 0.0
    return rounded


def round_floats(value: Any, decimals: int = 10) -> Any:
    if isinstance(value, float):
        return _finite_number(value, decimals)
    if isinstance(value, dict):
        return {str(key): round_floats(item, decimals) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [round_floats(item, decimals) for item in value]
    return value


def canonical_json_bytes(value: Any, *, decimals: int = 10) -> bytes:
    normalized = round_floats(value, decimals)
    try:
        text = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"value is not canonical-JSON serializable: {exc}") from exc
    return (text + "\n").encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any, *, decimals: int = 10) -> str:
    return sha256_bytes(canonical_json_bytes(value, decimals=decimals))


def digest_file(path: str | Path) -> str:
    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


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


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    """Write a complete file or leave the previous file untouched.

    The temporary file lives in the destination directory so ``os.replace`` is
    an atomic rename on NTFS and POSIX filesystems. Temporary files left by a
    crash are never interpreted as records.
    """

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: str | Path, value: Any, *, decimals: int = 10) -> bytes:
    payload = canonical_json_bytes(value, decimals=decimals)
    atomic_write_bytes(path, payload)
    return payload


def read_json(path: str | Path) -> Any:
    with Path(path).open("rb") as handle:
        return json.loads(handle.read().decode("utf-8"))


def assert_finite_tree(value: Any, path: str = "$") -> None:
    """Raise ValueError with a JSON-ish path if a numeric value is not finite."""

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite number at {path}")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            assert_finite_tree(item, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            assert_finite_tree(item, f"{path}[{index}]")

