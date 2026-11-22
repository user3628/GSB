"""Canonical JSON, hashing, atomic replacement, and portable real file locks."""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import secrets
from pathlib import Path
from typing import Any, Iterator

from .errors import WorkbenchError


def quantize(value: float, places: int = 9) -> float:
    """Round a finite float and remove negative zero for stable JSON output."""

    if not math.isfinite(value):
        raise ValueError("non-finite float cannot be serialized")
    result = round(float(value), places)
    if result == 0:
        return 0.0
    return result


def _normalize(value: Any, places: int | None) -> Any:
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float cannot be serialized")
        if places is not None:
            return quantize(value, places)
        return 0.0 if value == 0 else value
    if isinstance(value, dict):
        return {str(key): _normalize(item, places) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item, places) for item in value]
    return value


def canonical_bytes(value: Any, *, places: int | None = None) -> bytes:
    normalized = _normalize(value, places)
    text = json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return text.encode("utf-8")


def canonical_text(value: Any, *, places: int | None = None, pretty: bool = False) -> str:
    normalized = _normalize(value, places)
    if pretty:
        return json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_obj(value: Any, *, places: int | None = None) -> str:
    return digest_bytes(canonical_bytes(value, places=places))


def read_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise
    except json.JSONDecodeError as exc:
        raise WorkbenchError(
            "E_INVALID_JSON",
            f"Invalid JSON in {path}.",
            {"path": str(path), "line": exc.lineno, "column": exc.colno},
            400,
        ) from exc


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes via same-directory temporary file, fsync, and os.replace."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(6)}"
    try:
        with temp_path.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        if os.name != "nt":
            try:
                dir_fd = os.open(path.parent, os.O_RDONLY)
            except OSError:
                dir_fd = -1
            if dir_fd >= 0:
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
    finally:
        if temp_path.exists():
            with contextlib.suppress(OSError):
                temp_path.unlink()


def atomic_write_json(path: Path, value: Any, *, places: int | None = None) -> None:
    atomic_write_bytes(path, canonical_bytes(value, places=places))


class FileLock:
    """Cross-platform exclusive lock backed by flock/msvcrt byte-range locking."""

    def __init__(self, path: Path, *, blocking: bool = True) -> None:
        self.path = Path(path)
        self.blocking = blocking
        self._handle = None

    def acquire(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                mode = msvcrt.LK_LOCK if self.blocking else msvcrt.LK_NBLCK
                msvcrt.locking(handle.fileno(), mode, 1)
            else:
                import fcntl

                flags = fcntl.LOCK_EX
                if not self.blocking:
                    flags |= fcntl.LOCK_NB
                fcntl.flock(handle.fileno(), flags)
        except (OSError, BlockingIOError) as exc:
            handle.close()
            raise WorkbenchError(
                "E_WORKSPACE_BUSY",
                "The workspace resource is locked by another process.",
                {"lock": str(self.path)},
                409,
            ) from exc
        self._handle = handle
        return self

    def release(self) -> None:
        if self._handle is None:
            return
        handle = self._handle
        self._handle = None
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


@contextlib.contextmanager
def locked(path: Path, *, blocking: bool = True) -> Iterator[None]:
    lock = FileLock(path, blocking=blocking)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()

