"""Small dependency-free utilities for digests, atomic files, and locks."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Iterator, Mapping

import numpy as np

from .errors import SpMMError, fail


def canonical_json(value: Any) -> bytes:
    """Canonical JSON bytes used by every digest in the project."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_digest(value: Any) -> str:
    return sha256_hex(canonical_json(value))


def canonical_array_bytes(array: np.ndarray, *, dtype: str | np.dtype | None = None) -> bytes:
    """Return platform-independent little-endian C-order bytes."""

    target = np.dtype(dtype) if dtype is not None else array.dtype
    target = target.newbyteorder("<")
    canonical = np.ascontiguousarray(array, dtype=target)
    return canonical.tobytes(order="C")


def array_digest(array: np.ndarray, *, dtype: str | np.dtype | None = None) -> str:
    target = np.dtype(dtype) if dtype is not None else array.dtype
    header = canonical_json(
        {"dtype": target.str, "shape": [int(x) for x in array.shape]}
    )
    return sha256_hex(header + b"\0" + canonical_array_bytes(array, dtype=target))


def output_digest(array: np.ndarray) -> str:
    return array_digest(array, dtype=array.dtype)


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str | os.PathLike[str], data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
        fsync_directory(target.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temp_name)
        raise


def atomic_write_json(path: str | os.PathLike[str], value: Mapping[str, Any]) -> None:
    atomic_write_bytes(path, canonical_json(dict(value)) + b"\n")


def read_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except FileNotFoundError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise SpMMError("E_IO", f"cannot read JSON file: {path}", {"path": str(path), "cause": str(exc)}) from exc
    if not isinstance(value, dict):
        raise SpMMError("E_IO", f"JSON file must contain an object: {path}", {"path": str(path)})
    return value


def json_safe(value: Any) -> Any:
    """Convert NumPy/Decimal/non-finite values into JSON-safe primitives."""

    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return json_safe(value.tolist())
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        if np.isnan(value):
            return "NaN"
        if np.isposinf(value):
            return "Inf"
        if np.isneginf(value):
            return "-Inf"
        return value
    if isinstance(value, Path):
        return str(value)
    return value


def _try_lock(fd: int) -> bool:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    import fcntl

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def _unlock(fd: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        with contextlib.suppress(OSError):
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    with contextlib.suppress(OSError):
        fcntl.flock(fd, fcntl.LOCK_UN)


@contextlib.contextmanager
def file_lock(path: str | os.PathLike[str], *, timeout: float = 30.0) -> Iterator[None]:
    """Exclusive inter-process lock, including Windows support."""

    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    if os.fstat(fd).st_size == 0:
        os.write(fd, b"\0")
        os.fsync(fd)
    deadline = time.monotonic() + timeout
    try:
        while not _try_lock(fd):
            if time.monotonic() >= deadline:
                fail(
                    "E_LOCK_TIMEOUT",
                    "timed out waiting for the version-store lock",
                    details={"lock_path": str(lock_path), "timeout_seconds": timeout},
                )
            time.sleep(0.025)
        yield
    finally:
        _unlock(fd)
        os.close(fd)

