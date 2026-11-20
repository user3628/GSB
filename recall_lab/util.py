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

from .errors import BusinessError, ErrorCode


ZERO_DIGEST = "0" * 64


def canonical_json_bytes(value: Any) -> bytes:
    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise BusinessError(ErrorCode.NUMERIC, "value is not canonical JSON", {"reason": str(exc)}) from exc
    return text.encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_obj(value: Any) -> str:
    return digest_bytes(canonical_json_bytes(value))


def digest_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def ensure_finite(value: float | int, *, field: str, **details: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise BusinessError(ErrorCode.NUMERIC, f"non-numeric value for {field}", details) from exc
    if not math.isfinite(number):
        payload = {"field": field, "value": str(value), **details}
        raise BusinessError(ErrorCode.NUMERIC, f"non-finite value for {field}", payload)
    return number


def quantize(value: float | int | None, places: int = 10) -> float | None:
    if value is None:
        return None
    number = float(value)
    if not math.isfinite(number):
        raise BusinessError(ErrorCode.NUMERIC, "non-finite summary metric", {"value": str(value)})
    rounded = round(number, places)
    return 0.0 if rounded == 0 else rounded


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(path.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def atomic_write_json(path: Path, value: Any) -> bytes:
    data = canonical_json_bytes(value)
    atomic_write_bytes(path, data)
    return data


def read_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise BusinessError(
            ErrorCode.INVALID_STAGE,
            "required JSON file is missing",
            {"path": str(path), "reason": "MISSING_FILE"},
        ) from None
    except json.JSONDecodeError as exc:
        raise BusinessError(
            ErrorCode.STALE_CHECKPOINT,
            "checkpoint JSON cannot be decoded",
            {"path": str(path), "reason": "JSON_DECODE_ERROR", "line": exc.lineno},
        ) from exc


@contextlib.contextmanager
def file_lock(path: Path, *, exclusive: bool = True, blocking: bool = True) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(handle.fileno(), flags)
        except BlockingIOError as exc:
            raise BusinessError(
                ErrorCode.BUSY,
                "workspace is locked by another worker",
                {"lock": str(path)},
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def safe_relative_path(relative: str) -> Path:
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "manifest path must be a non-empty relative path",
            {"path": relative},
        )
    return candidate
