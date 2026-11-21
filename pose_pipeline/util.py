"""Deterministic serialization, hashing, atomic writes, and file locks."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def pretty_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def file_sha256(path: str | os.PathLike[str]) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def encode_array(array: Any) -> dict[str, Any]:
    import numpy as np

    normalized = np.ascontiguousarray(array, dtype="<f4")
    return {
        "dtype": "float32-le",
        "shape": list(normalized.shape),
        "data_b64": base64.b64encode(normalized.tobytes(order="C")).decode("ascii"),
    }


def decode_array(payload: dict[str, Any]) -> Any:
    import numpy as np

    if payload.get("dtype") != "float32-le":
        raise ValueError("unsupported encoded array dtype")
    raw = base64.b64decode(payload["data_b64"], validate=True)
    shape = tuple(int(part) for part in payload["shape"])
    expected = int(np.prod(shape)) * 4 if shape else 4
    if len(raw) != expected:
        raise ValueError("encoded array length does not match shape")
    return np.frombuffer(raw, dtype="<f4").reshape(shape).copy()


def atomic_write_bytes(path: str | os.PathLike[str], data: bytes) -> None:
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
        _fsync_directory(destination.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def atomic_write_json(path: str | os.PathLike[str], value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value))


def atomic_write_pretty_json(path: str | os.PathLike[str], value: Any) -> None:
    atomic_write_bytes(path, pretty_json_bytes(value))


def atomic_copy_file(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
    with open(source, "rb") as handle:
        atomic_write_bytes(destination, handle.read())


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


def read_json_file(path: str | os.PathLike[str]) -> Any:
    with open(path, "rb") as handle:
        return json.loads(handle.read().decode("utf-8"))


def safe_unlink(path: str | os.PathLike[str]) -> None:
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)


def safe_name(value: str) -> str:
    if not value or value in {".", ".."}:
        raise ValueError(f"unsafe identifier: {value!r}")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for character in value):
        raise ValueError(f"unsafe identifier: {value!r}")
    return value


def round_float(value: float, digits: int = 10) -> float:
    rounded = round(float(value), digits)
    return 0.0 if rounded == 0 else rounded


def finalize_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    value = dict(payload)
    value.pop("artifact_digest", None)
    value["artifact_digest"] = digest_json(value)
    return value


def verify_artifact(payload: dict[str, Any]) -> bool:
    claimed = payload.get("artifact_digest")
    if not isinstance(claimed, str):
        return False
    value = dict(payload)
    value.pop("artifact_digest", None)
    return digest_json(value) == claimed


class FileLock:
    """A real cross-process lock using flock on POSIX and msvcrt on Windows."""

    def __init__(self, path: str | os.PathLike[str], *, timeout: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._handle: Any = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+b")
        if os.name == "nt":
            self._lock_windows()
        else:
            self._lock_posix()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def _lock_posix(self) -> None:
        import fcntl

        fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)

    def _lock_windows(self) -> None:
        import msvcrt

        self._handle.seek(0, os.SEEK_END)
        if self._handle.tell() == 0:
            self._handle.write(b"\0")
            self._handle.flush()
        self._handle.seek(0)
        msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)


@contextlib.contextmanager
def locked(path: str | os.PathLike[str]) -> Iterator[None]:
    with FileLock(path):
        yield
