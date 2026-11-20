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


class WorkbenchError(Exception):
    """Base error carrying a stable business code."""

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class StaleCheckpointError(WorkbenchError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__("E_STALE_CHECKPOINT", message, details)


def _nonfinite_token(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if value == math.inf:
        return "Inf"
    return "-Inf"


def json_safe(value: Any) -> Any:
    """Convert values into a canonical-JSON-safe structure.

    Non-finite floats are represented as stable strings, never as JSON NaN/Infinity
    extensions. This keeps every digest and report byte-reproducible.
    """

    if isinstance(value, float):
        if not math.isfinite(value):
            return _nonfinite_token(value)
        if value == 0.0:
            return 0.0
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, Path):
        return value.as_posix()
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        json_safe(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def pretty_json(value: Any) -> str:
    return json.dumps(
        json_safe(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        indent=2,
    ) + "\n"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def digest(value: Any) -> str:
    return sha256_text(canonical_json(value))


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_bytes(path: Path, data: bytes, mode: int = 0o644) -> None:
    """Atomically replace a file, fsyncing the file and containing directory."""

    ensure_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def atomic_write_text(path: Path, text: str, mode: int = 0o644) -> None:
    atomic_write_bytes(path, text.encode("utf-8"), mode=mode)


def atomic_write_json(path: Path, value: Any, pretty: bool = False) -> None:
    text = pretty_json(value) if pretty else canonical_json(value) + "\n"
    atomic_write_text(path, text)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_workspace_path(root: Path, value: str | Path, *, must_exist: bool = False) -> Path:
    root = root.resolve()
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise WorkbenchError(
            "E_OUTSIDE_WORKSPACE",
            "path must stay inside the workspace",
            {"root": str(root), "path": str(candidate)},
        ) from exc
    if must_exist and not candidate.exists():
        raise WorkbenchError(
            "E_NOT_FOUND",
            "required local file does not exist",
            {"path": relpath(root, candidate)},
        )
    return candidate


def relpath(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


@contextlib.contextmanager
def exclusive_lock(path: Path, *, blocking: bool = True) -> Iterator[None]:
    """Advisory process lock based on the standard-library fcntl facility."""

    ensure_dir(path.parent)
    handle = path.open("a+b")
    flags = fcntl.LOCK_EX
    if not blocking:
        flags |= fcntl.LOCK_NB
    try:
        try:
            fcntl.flock(handle.fileno(), flags)
        except BlockingIOError as exc:
            raise WorkbenchError(
                "E_RUN_IN_PROGRESS",
                "another process already holds the workbench run lock",
                {"lock": path.as_posix()},
            ) from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def strict_float(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not a score")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "nan":
            return math.nan
        if lowered in {"inf", "+inf", "infinity", "+infinity"}:
            return math.inf
        if lowered in {"-inf", "-infinity"}:
            return -math.inf
        return float(value)
    raise ValueError(f"not numeric: {type(value).__name__}")


def stable_round(value: float | None, digits: int = 12) -> float | None:
    if value is None:
        return None
    if not math.isfinite(value):
        return None
    rounded = round(float(value), digits)
    return 0.0 if rounded == 0.0 else rounded


__all__ = [
    "WorkbenchError",
    "StaleCheckpointError",
    "atomic_write_bytes",
    "atomic_write_json",
    "atomic_write_text",
    "canonical_json",
    "digest",
    "ensure_dir",
    "exclusive_lock",
    "file_sha256",
    "json_safe",
    "pretty_json",
    "read_json",
    "relpath",
    "resolve_workspace_path",
    "sha256_bytes",
    "sha256_text",
    "stable_round",
    "strict_float",
]
