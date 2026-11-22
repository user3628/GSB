from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import tempfile
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterator

import fcntl


SCHEMA_VERSION = "1.0"
FIXED_SCALE = 1_000_000


class PipelineError(RuntimeError):
    """A deterministic, machine-readable pipeline error."""

    def __init__(self, code: str, message: str, details: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details if details is not None else {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class GracefulStop(PipelineError):
    def __init__(self, stage: str, completed_units: int, message: str | None = None) -> None:
        super().__init__(
            "E_STOPPED",
            message or "停止请求已生效；当前不可拆分单元已完成，可安全恢复。",
            {"stage": stage, "completed_units": completed_units},
        )


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_bytes(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_obj(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def artifact_digest(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def round_float(value: float, digits: int = 12) -> float:
    """Stable display scalar; classification uses the same rounded scalar."""
    if value == 0.0:
        return 0.0
    return float(f"{value:.{digits}f}")


def fixed_fp(value: float, scale: int = FIXED_SCALE) -> int:
    """Round a non-negative scalar to a fixed-point integer, half up."""
    if value is None:
        return 0
    value = float(value)
    if not math.isfinite(value):
        return 0
    if value <= 0.0:
        return 0
    dec = Decimal(str(value)) * Decimal(scale)
    return int(dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def fixed_str(fp: int, scale: int = FIXED_SCALE) -> str:
    whole, frac = divmod(int(fp), scale)
    return f"{whole}.{frac:06d}"


def now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path | str, data: bytes) -> None:
    """Write bytes using same-directory temp file, fsync, and replace."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
        _fsync_dir(target.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)
        raise


def atomic_write_json(path: Path | str, value: Any) -> str:
    data = canonical_bytes(value)
    atomic_write_bytes(path, data)
    return sha256_bytes(data)


def read_json(path: Path | str, default: Any = None) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_bytes(path: Path | str) -> bytes:
    return Path(path).read_bytes()


@contextlib.contextmanager
def exclusive_file_lock(path: Path | str, blocking: bool = True) -> Iterator[None]:
    """Linux/Unix advisory flock. The lock file is a real filesystem object."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(fd, flags)
        except BlockingIOError as exc:
            raise PipelineError("E_RUN_BUSY", "另一个进程正在执行此操作。", {"lock": str(target)}) from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = json.loads(json.dumps(base))
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
