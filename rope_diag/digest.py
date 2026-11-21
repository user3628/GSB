"""Canonical serialization, fingerprints, and atomic persistence helpers."""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

import numpy as np


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def digest_array(array: np.ndarray) -> str:
    contiguous = np.ascontiguousarray(array)
    header = canonical_json_bytes(
        {
            "dtype": contiguous.dtype.str,
            "shape": list(contiguous.shape),
        }
    )
    h = hashlib.sha256()
    h.update(b"rope-diag-array-v1\0")
    h.update(header)
    h.update(b"\0")
    h.update(contiguous.tobytes(order="C"))
    return h.hexdigest()


def digest_arrays(arrays: Mapping[str, np.ndarray]) -> str:
    h = hashlib.sha256()
    h.update(b"rope-diag-arrays-v1\0")
    for name in sorted(arrays):
        h.update(name.encode("utf-8"))
        h.update(b"\0")
        h.update(digest_array(arrays[name]).encode("ascii"))
        h.update(b"\0")
    return h.hexdigest()


def _fsync_dir(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    """Write *data* and atomically replace *path* without exposing a partial file."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.tmp-", dir=str(target.parent)
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
        _fsync_dir(target.parent)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: str | Path, value: Any, *, pretty: bool = True) -> None:
    if pretty:
        data = (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    else:
        data = canonical_json_bytes(value)
    atomic_write_bytes(path, data)


def atomic_write_npy(path: str | Path, array: np.ndarray) -> None:
    buffer = io.BytesIO()
    np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
    atomic_write_bytes(path, buffer.getvalue())


def read_json(path: str | Path, default: Any = None) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_npy(path: str | Path) -> np.ndarray:
    return np.load(Path(path), allow_pickle=False)
