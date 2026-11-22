"""Canonical serialisation, hashing, atomic writes and real file locks."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator

from .errors import CabinVoiceError


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise CabinVoiceError(
            "E_NONFINITE_SCORE",
            "artifact contains a non-finite or non-serialisable value",
            {"error": str(exc)},
        ) from exc


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_finite(value: Any, *, code: str, message: str, details: dict[str, Any]) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CabinVoiceError(code, message, details) from exc
    if not math.isfinite(number):
        raise CabinVoiceError(code, message, details)
    return number


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    """Write *data* through a same-directory temporary and fsync before replace."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_bytes(value))


def read_json(path: str | Path) -> Any:
    with open(path, "rb") as handle:
        return json.loads(handle.read().decode("utf-8"))


class FileLock:
    """Advisory exclusive lock; ``flock`` is released by the kernel on process exit."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._handle = None

    def acquire(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+b")
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def release(self) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()


class TryFileLock(FileLock):
    """Non-blocking exclusive lock used to reject concurrent builds."""

    def acquire(self) -> "TryFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+b")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._handle.close()
            self._handle = None
            raise CabinVoiceError(
                "E_BUILD_IN_PROGRESS",
                "another process is already building this workspace",
                {"lock": str(self.path)},
            ) from exc
        return self


def first_difference(left: bytes, right: bytes) -> dict[str, Any]:
    boundary = min(len(left), len(right))
    for index in range(boundary):
        if left[index] != right[index]:
            return {
                "byte_offset": index,
                "left_hex": left[max(0, index - 12) : index + 12].hex(),
                "right_hex": right[max(0, index - 12) : index + 12].hex(),
            }
    if len(left) != len(right):
        return {
            "byte_offset": boundary,
            "left_length": len(left),
            "right_length": len(right),
            "left_hex": left[max(0, boundary - 12) : boundary].hex(),
            "right_hex": right[max(0, boundary - 12) : boundary].hex(),
        }
    return {}


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def nested_set(document: dict[str, Any], dotted_path: str, value: Any) -> dict[str, Any]:
    if not dotted_path:
        raise CabinVoiceError("E_BAD_REQUEST", "config path must not be empty", {})
    result = json.loads(json.dumps(document))
    cursor = result
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        if part not in cursor or not isinstance(cursor[part], dict):
            raise CabinVoiceError(
                "E_BAD_REQUEST", "config path does not exist", {"path": dotted_path, "segment": part}
            )
        cursor = cursor[part]
    if parts[-1] not in cursor:
        raise CabinVoiceError(
            "E_BAD_REQUEST", "config leaf does not exist", {"path": dotted_path, "leaf": parts[-1]}
        )
    cursor[parts[-1]] = value
    return result


@contextlib.contextmanager
def open_bytes(path: str | Path) -> Iterator[bytes]:
    with open(path, "rb") as handle:
        yield handle.read()
