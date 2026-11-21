"""Canonical encoding, hashing, atomic writes, and cross-platform file locks."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator

from .errors import E_IO, E_INVALID_RECIPE, MatgenError


def canonical_json_bytes(value: Any) -> bytes:
    """Return the one canonical JSON representation used for all artifacts."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fingerprint(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def atomic_write_bytes(path: str | Path, data: bytes, *, mode: int | None = None) -> None:
    """Durably replace a file without exposing a partially written target."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle = None
    temporary_name: str | None = None
    try:
        handle = tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        )
        temporary_name = handle.name
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        handle = None
        if mode is not None:
            os.chmod(temporary_name, mode)
        os.replace(temporary_name, destination)
        temporary_name = None
        if os.name != "nt":
            directory_fd = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except OSError as exc:
        raise MatgenError(
            code=E_IO,
            message=f"unable to atomically write {destination}",
            details={"path": str(destination), "reason": str(exc)},
        ) from exc
    finally:
        if handle is not None:
            handle.close()
        if temporary_name is not None:
            with contextlib.suppress(OSError):
                os.unlink(temporary_name)


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value) + b"\n")


def read_json(path: str | Path) -> Any:
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise MatgenError(
            code=E_INVALID_RECIPE,
            message=f"unable to read JSON from {source}",
            details={"path": str(source), "reason": str(exc)},
        ) from exc


def read_bytes(path: str | Path) -> bytes:
    source = Path(path)
    try:
        return source.read_bytes()
    except OSError as exc:
        raise MatgenError(
            code=E_IO,
            message=f"unable to read {source}",
            details={"path": str(source), "reason": str(exc)},
        ) from exc


class FileLock:
    """Advisory exclusive file lock with POSIX and Windows implementations."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        if os.name == "nt":  # pragma: no cover - exercised on Windows CI
            import msvcrt

            self._handle.seek(0)
            if self._handle.read(1) == b"":
                self._handle.write(b"\0")
                self._handle.flush()
            self._handle.seek(0)
            msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._handle is None:
            return
        if os.name == "nt":  # pragma: no cover - exercised on Windows CI
            import msvcrt

            self._handle.seek(0)
            msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


@contextlib.contextmanager
def temporary_directory(parent: str | Path, prefix: str) -> Iterator[Path]:
    destination = Path(parent)
    destination.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(dir=destination, prefix=prefix))
    try:
        yield directory
    finally:
        for child in sorted(directory.rglob("*"), reverse=True):
            with contextlib.suppress(OSError):
                if child.is_file() or child.is_symlink():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
        with contextlib.suppress(OSError):
            directory.rmdir()

