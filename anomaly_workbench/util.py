"""Deterministic serialization, hashing, atomic writes and file locks."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterator

import numpy as np


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | os.PathLike[str], chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def digest_object(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def _fsync_dir(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: str | os.PathLike[str], data: bytes) -> None:
    """Write bytes to a sibling temporary file and atomically rename it."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
        _fsync_dir(destination.parent)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def atomic_write_json(path: str | os.PathLike[str], value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value))


def read_json(path: str | os.PathLike[str]) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def copy_atomic(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
    source_path = Path(source)
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination_path.name}.", suffix=".tmp", dir=destination_path.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as target, open(source_path, "rb") as source_handle:
            shutil.copyfileobj(source_handle, target)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary_name, destination_path)
        _fsync_dir(destination_path.parent)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


class FileLock:
    """Advisory process lock backed by ``flock``."""

    def __init__(self, path: str | os.PathLike[str], *, blocking: bool = True) -> None:
        self.path = Path(path)
        self.blocking = blocking
        self._handle: Any = None

    def acquire(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+b")
        flags = fcntl.LOCK_EX
        if not self.blocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(self._handle.fileno(), flags)
        except BlockingIOError:
            self._handle.close()
            self._handle = None
            raise
        return self

    def release(self) -> None:
        if self._handle is not None:
            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._handle.close()
                self._handle = None

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()


def file_lock(path: str | os.PathLike[str], *, blocking: bool = True) -> FileLock:
    return FileLock(path, blocking=blocking)


def safe_relative_path(root: str | os.PathLike[str], value: str) -> Path:
    root_path = Path(root).resolve()
    candidate = (root_path / value).resolve()
    if candidate != root_path and root_path not in candidate.parents:
        raise ValueError(f"path escapes workspace: {value}")
    return candidate


def finite_float(value: Any, *, field: str) -> float:
    try:
        converted = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not np.isfinite(converted):
        raise ValueError(f"{field} must be finite")
    return 0.0 if converted == 0.0 else converted


def jsonable_array(array: np.ndarray, *, digits: int | None = None) -> list[Any]:
    """Return a JSON-safe, deterministic representation of an ndarray."""

    arr = np.asarray(array)
    if digits is not None:
        arr = np.round(arr, digits=digits)
    if arr.ndim == 0:
        value = arr.item()
        if isinstance(value, float):
            return [0.0 if value == 0.0 else float(value)]
        return [value.item() if hasattr(value, "item") else value]
    if arr.ndim == 1:
        result: list[Any] = []
        for value in arr.tolist():
            if isinstance(value, float):
                result.append(0.0 if value == 0.0 else float(value))
            else:
                result.append(value.item() if hasattr(value, "item") else value)
        return result
    return [jsonable_array(row, digits=digits) for row in arr]


def atomic_directory_write(
    directory: str | os.PathLike[str], files: dict[str, bytes]
) -> None:
    """Write a small set of files through atomic per-file replacements."""

    base = Path(directory)
    base.mkdir(parents=True, exist_ok=True)
    for relative, content in files.items():
        atomic_write_bytes(base / relative, content)


def remove_if_exists(path: str | os.PathLike[str]) -> None:
    try:
        Path(path).unlink()
    except FileNotFoundError:
        pass


@contextlib.contextmanager
def temporary_directory(prefix: str = "awb-") -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix=prefix) as directory:
        yield Path(directory)
