"""Small deterministic and filesystem primitives shared by the engine."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import struct
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import numpy as np


def canonical_json_bytes(value: Any) -> bytes:
    """Canonical JSON used for every content digest and report byte comparison."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def pretty_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def frame_fingerprint(frame: np.ndarray) -> str:
    """Fingerprint pixels plus dtype and shape, independent of .npy headers."""

    array = np.ascontiguousarray(frame)
    digest = hashlib.sha256()
    digest.update(b"MOTION-FRAME-v1\0")
    digest.update(array.dtype.str.encode("ascii"))
    digest.update(struct.pack(">I", array.ndim))
    for dimension in array.shape:
        digest.update(struct.pack(">Q", int(dimension)))
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


def state_digest(state: dict[str, Any]) -> str:
    return digest_json({"schema": "motion-state-v1", "state": state})


def safe_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    if not cleaned or cleaned in {".", ".."}:
        raise ValueError("series id cannot be converted to a safe path component")
    return cleaned


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Durable same-directory replace; abandoned temp files are never records."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp-", dir=str(path.parent)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        fsync_directory(path.parent)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass


@contextmanager
def file_lock(path: Path, *, exclusive: bool = True) -> Iterator[None]:
    """Advisory inter-process lock using a dedicated stable lock file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(handle.fileno(), operation)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_json_file(path: Path) -> Any:
    with path.open("rb") as handle:
        return json.load(handle)
