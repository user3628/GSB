"""Canonical serialization, hashing, locks, and atomic file helpers."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator

import numpy as np


def _json_default(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value)!r}")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")


def pretty_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
            default=_json_default,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def atomic_write_bytes(path: str | Path, data: bytes, *, mode: int | None = None) -> None:
    """Write a file via same-directory temp, fsync, then os.replace."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            os.chmod(temp, mode)
        os.replace(temp, target)
        fsync_dir(target.parent)
    finally:
        if temp.exists():
            temp.unlink(missing_ok=True)


def atomic_write_json(path: str | Path, value: Any, *, pretty: bool = True) -> None:
    data = pretty_json_bytes(value) if pretty else canonical_json_bytes(value)
    atomic_write_bytes(path, data)


def fsync_dir(path: str | Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextlib.contextmanager
def file_lock(path: str | Path) -> Iterator[None]:
    """Exclusive advisory lock; Linux fcntl is available in the target runtime."""

    import fcntl

    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def encode_pcm_int16(pcm: np.ndarray) -> str:
    contiguous = np.ascontiguousarray(pcm.astype("<i2", copy=False))
    return base64.b64encode(contiguous.tobytes(order="C")).decode("ascii")


def decode_pcm_int16(encoded: str) -> np.ndarray:
    raw = base64.b64decode(encoded.encode("ascii"), validate=True)
    if len(raw) % 2:
        raise ValueError("int16 PCM payload length must be even")
    return np.frombuffer(raw, dtype="<i2").copy()


def safe_component(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in value)
    cleaned = cleaned.strip("._")
    return cleaned or "item"


def relative_display(path: str | Path, root: str | Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(Path(root).resolve()))
    except ValueError:
        return str(path)

