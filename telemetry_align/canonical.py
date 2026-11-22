"""Canonical serialization, hashing, atomic files and real file locks."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator

from .errors import TelemetryError


def canonical_json_text(value: Any) -> str:
    """Stable UTF-8 JSON used by every persisted artifact and fingerprint."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_json_bytes(value: Any) -> bytes:
    return canonical_json_text(value).encode("utf-8") + b"\n"


def pretty_json_text(value: Any) -> str:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path | str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def digest_obj(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def fingerprint(namespace: str, value: Any) -> str:
    return digest_obj({"namespace": namespace, "value": value})


def fsync_dir(path: Path | str) -> None:
    directory = Path(path)
    try:
        fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path | str, data: bytes) -> None:
    """Write a complete file, fsync it and atomically replace the destination."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        fsync_dir(destination.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def atomic_write_text(path: Path | str, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: Path | str, value: Any, *, pretty: bool = True) -> str:
    text = pretty_json_text(value) if pretty else canonical_json_text(value) + "\n"
    atomic_write_text(path, text)
    return sha256_bytes(text.encode("utf-8"))


def read_json(path: Path | str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def safe_relpath(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "路径必须位于工作区内部 / path must stay inside the workspace",
            {"path": str(path), "workspace": str(root)},
        ) from exc


def resolve_inside(root: Path, relative_or_absolute: str | Path) -> Path:
    candidate = Path(relative_or_absolute)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "路径越界 / path escapes the workspace",
            {"path": str(relative_or_absolute), "workspace": str(root)},
        ) from exc
    return resolved


class FileLock:
    """A real POSIX advisory lock backed by ``flock``."""

    def __init__(
        self,
        path: Path | str,
        *,
        blocking: bool = True,
        timeout_s: float = 10.0,
        purpose: str = "file lock",
    ) -> None:
        self.path = Path(path)
        self.blocking = blocking
        self.timeout_s = timeout_s
        self.purpose = purpose
        self._handle: Any = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self.path, "a+", encoding="utf-8")
        started = time.monotonic()
        while True:
            try:
                flags = fcntl.LOCK_EX | (0 if self.blocking else fcntl.LOCK_NB)
                fcntl.flock(handle.fileno(), flags)
                self._handle = handle
                return
            except BlockingIOError as exc:
                if not self.blocking:
                    handle.close()
                    raise TelemetryError(
                        "E_RUN_ACTIVE",
                        f"{self.purpose} 已被占用 / lock is already held",
                        {"lock": str(self.path)},
                    ) from exc
                if time.monotonic() - started >= self.timeout_s:
                    handle.close()
                    raise TelemetryError(
                        "E_IO",
                        f"{self.purpose} 获取超时 / lock timeout",
                        {"lock": str(self.path), "timeout_s": self.timeout_s},
                    ) from exc
                time.sleep(0.02)

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


@contextlib.contextmanager
def file_lock(
    path: Path | str,
    *,
    blocking: bool = True,
    timeout_s: float = 10.0,
    purpose: str = "file lock",
) -> Iterator[None]:
    lock = FileLock(
        path, blocking=blocking, timeout_s=timeout_s, purpose=purpose
    )
    lock.acquire()
    try:
        yield
    finally:
        lock.release()

