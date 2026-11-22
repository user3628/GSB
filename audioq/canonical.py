"""Canonical serialization, digests, atomic publication and portable file locks."""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import AudioQError

try:  # Linux/macOS implementation; the project targets a local Linux workspace.
    import fcntl
except ImportError:  # pragma: no cover - exercised only on non-POSIX hosts
    fcntl = None


_THREAD_LOCKS: dict[str, threading.Lock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


def _thread_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _THREAD_LOCKS_GUARD:
        return _THREAD_LOCKS.setdefault(key, threading.Lock())


def canonical_json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    if pretty:
        text = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
    else:
        text = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    return (text + "\n").encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def fingerprint(value: Any) -> str:
    return "sha256:" + sha256_json(value)


def normalize_zero(value: float) -> float:
    return 0.0 if value == 0.0 else value


def finite_float(value: Any, *, name: str = "value") -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AudioQError("E_NUMERIC", f"{name} is not numeric", observed={name: repr(value)}) from exc
    if not math.isfinite(number):
        raise AudioQError("E_NUMERIC", f"{name} is NaN or infinite", observed={name: repr(value)})
    return normalize_zero(number)


def rounded_float(value: Any, decimals: int = 12, *, name: str = "value") -> float:
    number = finite_float(value, name=name)
    return normalize_zero(round(number, decimals))


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        directory_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def atomic_write_json(path: Path, value: Any, *, pretty: bool = True) -> None:
    atomic_write_bytes(Path(path), canonical_json_bytes(value, pretty=pretty))


def read_json(path: Path) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AudioQError("E_IO", "file does not exist", observed={"path": str(path)}) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AudioQError(
            "E_CONFIG" if Path(path).suffix == ".json" else "E_IO",
            "file is not valid JSON",
            observed={"path": str(path), "reason": str(exc)},
        ) from exc


@contextmanager
def file_lock(path: Path, *, timeout: float = 30.0) -> Iterator[None]:
    """Exclusive cross-process plus in-process lock.

    ``flock`` is released by the kernel if a process dies, so persistent lock
    files do not become stale locks. The thread lock is needed because POSIX
    record locks have process-oriented edge cases.
    """

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    local = _thread_lock(path)
    if not local.acquire(timeout=timeout):
        raise AudioQError("E_IO", "timed out waiting for local file lock", observed={"path": str(path)})
    handle = None
    try:
        handle = path.open("a+b")
        if fcntl is None:  # pragma: no cover
            raise AudioQError("E_IO", "file locking is unsupported on this platform")
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise AudioQError(
                        "E_IO",
                        "timed out waiting for cross-process file lock",
                        observed={"path": str(path)},
                    )
                time.sleep(0.01)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        if handle is not None:
            handle.close()
        local.release()
