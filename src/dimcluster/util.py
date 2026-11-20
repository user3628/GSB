from __future__ import annotations

import hashlib
import io
import json
import os
import time
import uuid
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .errors import WorkbenchError


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_text(value: Any, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
    return canonical_json_bytes(value).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact_digest(artifacts: dict[str, str]) -> str:
    return sha256_bytes(canonical_json_bytes(artifacts))


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def atomic_write_text(path: Path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: Path, value: Any, pretty: bool = False) -> None:
    atomic_write_text(path, canonical_json_text(value, pretty=pretty))


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def stable_round(array: np.ndarray, decimals: int = 12) -> np.ndarray:
    rounded = np.round(np.asarray(array), decimals=decimals)
    rounded = np.where(np.abs(rounded) < 10 ** (-decimals), 0.0, rounded)
    return rounded


def canonical_float_array(array: np.ndarray) -> np.ndarray:
    return np.asarray(stable_round(array), dtype="<f8", order="C")


def canonical_int_array(array: np.ndarray) -> np.ndarray:
    return np.asarray(array, dtype="<i8", order="C")


def array_bytes(array: np.ndarray, kind: str = "float") -> bytes:
    if kind == "int":
        canonical = canonical_int_array(array)
    else:
        canonical = canonical_float_array(array)
    buffer = io.BytesIO()
    np.save(buffer, canonical, allow_pickle=False)
    return buffer.getvalue()


def atomic_write_array(path: Path, array: np.ndarray, kind: str = "float") -> str:
    data = array_bytes(array, kind=kind)
    atomic_write_bytes(path, data)
    return sha256_bytes(data)


def load_array(path: Path) -> np.ndarray:
    with path.open("rb") as handle:
        return np.load(handle, allow_pickle=False)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = json.loads(json.dumps(base))
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def parse_override(text: str) -> tuple[str, Any]:
    if "=" not in text:
        raise WorkbenchError("E_INVALID_CONFIG", f"Override must be key=value: {text}")
    key, raw = text.split("=", 1)
    key = key.strip()
    raw = raw.strip()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        value = raw
    return key, value


def set_dotted(target: dict[str, Any], dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    cursor = target
    for part in parts[:-1]:
        current = cursor.get(part)
        if not isinstance(current, dict):
            current = {}
            cursor[part] = current
        cursor = current
    cursor[parts[-1]] = value


def apply_overrides(config: dict[str, Any], overrides: Iterable[str]) -> dict[str, Any]:
    result = json.loads(json.dumps(config))
    for override in overrides:
        key, value = parse_override(override)
        set_dotted(result, key, value)
    return result


class FileLock(AbstractContextManager["FileLock"]):
    """Small cross-platform exclusive file lock.

    POSIX uses flock(2). Windows uses msvcrt.locking on a one-byte region.
    The lock file is intentionally never deleted so lock identity is stable.
    """

    def __init__(self, path: Path, timeout: float = 20.0) -> None:
        self.path = path
        self.timeout = timeout
        self._handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        if os.name == "nt":
            self._handle.seek(0, os.SEEK_END)
            if self._handle.tell() == 0:
                self._handle.write(b"\0")
                self._handle.flush()
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    self._handle.seek(0)
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    self._handle.close()
                    self._handle = None
                    raise WorkbenchError(
                        "E_STALE_CHECKPOINT",
                        f"Timed out waiting for state lock: {self.path}",
                    )
                time.sleep(0.025)

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


def ensure_workspace(root: Path) -> dict[str, Path]:
    paths = {
        "root": root,
        "samples": root / "samples",
        "state": root / "state",
        "outputs": root / "outputs",
        "demo": root / "demo",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    (paths["state"] / "checkpoints").mkdir(parents=True, exist_ok=True)
    (paths["state"] / "artifacts").mkdir(parents=True, exist_ok=True)
    (paths["state"] / "ledger").mkdir(parents=True, exist_ok=True)
    (paths["state"] / "tmp").mkdir(parents=True, exist_ok=True)
    return paths


def relative_or_absolute(path: Path, root: Path | None = None) -> str:
    if root is not None:
        try:
            return str(path.resolve().relative_to(root.resolve()))
        except ValueError:
            pass
    return str(path.resolve())
