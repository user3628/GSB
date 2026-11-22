"""Deterministic serialization, hashing, and atomic file helpers."""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

import numpy as np


MASK64 = (1 << 64) - 1


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


def json_safe(value: Any) -> Any:
    """Convert numpy values into canonical JSON-compatible values."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return [json_safe(item) for item in value.tolist()]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def digest_array(array: np.ndarray) -> str:
    """Hash shape, canonical dtype, and little-endian contiguous bytes."""
    arr = np.asarray(array)
    if arr.dtype.kind == "c":
        payload = np.ascontiguousarray(arr.astype("<c16", copy=False)).tobytes()
        dtype_name = "<c16"
    elif arr.dtype.kind == "f":
        payload = np.ascontiguousarray(arr.astype("<f8", copy=False)).tobytes()
        dtype_name = "<f8"
    elif arr.dtype.kind == "b":
        payload = np.ascontiguousarray(arr.astype("u1", copy=False)).tobytes()
        dtype_name = "|u1"
    elif arr.dtype.kind in "iu":
        payload = np.ascontiguousarray(arr.astype("<i8", copy=False)).tobytes()
        dtype_name = "<i8"
    else:
        raise TypeError(f"unsupported dtype for deterministic digest: {arr.dtype}")
    header = canonical_json_bytes({"dtype": dtype_name, "shape": list(arr.shape)})
    return sha256_bytes(header + b"\0" + payload)


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
        _fsync_directory(target.parent)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(json_safe(value)) + b"\n")


def read_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def atomic_write_npz(path: str | Path, arrays: Mapping[str, np.ndarray], meta: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            np.savez(handle, meta_json=np.asarray(canonical_json_bytes(json_safe(meta))), **arrays)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
        _fsync_directory(target.parent)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def load_npz(path: str | Path) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    with np.load(path, allow_pickle=False) as archive:
        arrays = {name: archive[name] for name in archive.files if name != "meta_json"}
        meta_raw = archive["meta_json"] if "meta_json" in archive.files else np.asarray(b"{}")
    meta = json.loads(meta_raw.tobytes().decode("utf-8"))
    return arrays, meta


def splitmix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
    return (value ^ (value >> 31)) & MASK64


def fixed_hash_unit(seed: int, *parts: int) -> float:
    value = int(seed) & MASK64
    for part in parts:
        value = splitmix64(value ^ (int(part) & MASK64))
    return (value >> 11) / float(1 << 53)


def read_npz_bytes(path: str | Path) -> bytes:
    """Return a stable logical digest representation for an npz file."""
    arrays, meta = load_npz(path)
    return canonical_json_bytes(
        {
            "meta": meta,
            "arrays": {name: digest_array(arrays[name]) for name in sorted(arrays)},
        }
    )


def npz_to_bytes(path: str | Path) -> bytes:
    """Small compatibility helper used by tests/tools."""
    with open(path, "rb") as handle:
        return handle.read()


def bytes_io_npz(arrays: Mapping[str, np.ndarray], meta: Any) -> bytes:
    buffer = io.BytesIO()
    np.savez(buffer, meta_json=np.asarray(canonical_json_bytes(json_safe(meta))), **arrays)
    return buffer.getvalue()

