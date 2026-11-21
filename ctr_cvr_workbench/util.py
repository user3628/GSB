"""Canonical serialization, atomic writes and cross-platform file locks."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator

from .errors import ErrorCode, PipelineError


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def jsonable(value: Any) -> Any:
    """Convert numpy-like values and nested containers into JSON-safe values."""

    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if hasattr(value, "tolist"):
        return jsonable(value.tolist())
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        return jsonable(value.item())
    return value


def canonical_json_line(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, data: bytes, mode: int | None = None) -> None:
    """Replace *path* atomically; a crash never leaves the final file half written."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            try:
                os.chmod(temporary, mode)
            except OSError:
                pass
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise


def atomic_write_json(path: Path, value: Any, mode: int | None = None) -> bytes:
    data = canonical_json_line(value)
    atomic_write_bytes(path, data, mode=mode)
    return data


def atomic_write_jsonl(path: Path, records: list[Any]) -> bytes:
    data = b"".join(canonical_json_line(record) for record in records)
    atomic_write_bytes(path, data)
    return data


def read_json(path: Path) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(
            ErrorCode.E_IO,
            f"cannot read JSON artifact: {path}",
            details={"path": str(path), "reason": str(exc)},
        ) from exc


def ensure_relative_path(value: str, field_name: str) -> str:
    path = Path(value)
    if path.is_absolute() or value.startswith(("/", "\\")):
        raise PipelineError(
            ErrorCode.E_INVALID_PARAM,
            f"{field_name} must be a local relative path",
            details={"field": field_name, "value": value},
        )
    normalized = path.as_posix()
    if normalized == ".." or normalized.startswith("../"):
        raise PipelineError(
            ErrorCode.E_INVALID_PARAM,
            f"{field_name} cannot escape the workspace",
            details={"field": field_name, "value": value},
        )
    return normalized


def resolve_under(base_dir: Path, relative: str, field_name: str = "path") -> Path:
    relative = ensure_relative_path(relative, field_name)
    base = Path(base_dir).resolve()
    candidate = (base / relative).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise PipelineError(
            ErrorCode.E_INVALID_PARAM,
            f"{field_name} escapes the workspace",
            details={"field": field_name, "value": relative},
        ) from exc
    return candidate


def portable_relative(path: Path, base_dir: Path) -> str:
    resolved = Path(path).resolve()
    base = Path(base_dir).resolve()
    try:
        relative = resolved.relative_to(base)
    except ValueError:
        return resolved.as_posix()
    return relative.as_posix()


class FileLock:
    """Small cross-platform lock based on atomic directory creation."""

    def __init__(self, path: Path, timeout_seconds: float = 30.0):
        self.path = Path(path)
        self.timeout_seconds = timeout_seconds

    def __enter__(self) -> "FileLock":
        deadline = time.monotonic() + self.timeout_seconds
        self.path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                os.mkdir(self.path)
                break
            except FileExistsError:
                try:
                    age = time.time() - self.path.stat().st_mtime
                    if age > 120:
                        shutil.rmtree(self.path, ignore_errors=True)
                        continue
                except OSError:
                    pass
                if time.monotonic() >= deadline:
                    raise PipelineError(
                        ErrorCode.E_IO,
                        "timed out acquiring local checkpoint lock",
                        details={"lock_path": str(self.path)},
                    )
                time.sleep(0.01)
        owner = str(os.getpid()).encode("ascii")
        atomic_write_bytes(self.path / "owner", owner)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


@contextlib.contextmanager
def read_only_file(path: Path) -> Iterator[Path]:
    """Read-only registration helper: callers only receive an open-for-read handle."""

    with Path(path).open("r", encoding="utf-8") as handle:
        yield handle  # type: ignore[misc]
