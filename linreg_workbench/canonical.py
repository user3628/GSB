"""Canonical JSON, digests and crash-safe filesystem primitives."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .errors import IOErrorWorkbench, NumericError


def json_safe(value: Any) -> Any:
    """Convert numpy/scalar containers into deterministic JSON-safe values."""

    if isinstance(value, np.ndarray):
        return json_safe(value.tolist())
    if isinstance(value, np.generic):
        return json_safe(value.item())
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NumericError(
                "Non-finite value cannot be serialized",
                details={"reason": "NaN or Inf encountered during serialization"},
            )
        return value
    if isinstance(value, (int, str, bool)) or value is None:
        return value
    raise TypeError(f"Unsupported JSON value type: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        json_safe(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_bytes(value: Any, *, trailing_newline: bool = False) -> bytes:
    payload = canonical_json(value).encode("utf-8")
    return payload + (b"\n" if trailing_newline else b"")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str | os.PathLike[str], payload: bytes) -> None:
    """Write bytes through a same-directory temp file and fsync the rename."""

    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, target)
            fsync_directory(target.parent)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
    except OSError as exc:
        raise IOErrorWorkbench(
            f"Could not atomically write {target.name}",
            details={"path": str(target), "os_error": str(exc)},
        ) from exc


def atomic_write_json(path: str | os.PathLike[str], value: Any) -> bytes:
    payload = canonical_bytes(value, trailing_newline=True)
    atomic_write_bytes(path, payload)
    return payload


def read_json(path: str | os.PathLike[str]) -> Any:
    with open(path, "rb") as handle:
        return json.loads(handle.read().decode("utf-8"))


def coefficient_summary(coefficients: Iterable[float], intercept: float) -> dict[str, Any]:
    values = np.asarray(list(coefficients), dtype=np.float64)
    abs_values = np.abs(values)
    return {
        "digest": digest({"coefficients": values.tolist(), "intercept": float(intercept)}),
        "intercept": float(intercept),
        "l1_norm": float(abs_values.sum()),
        "l2_norm": float(math.sqrt(float(np.dot(values, values)))),
        "max_abs": float(abs_values.max()) if values.size else 0.0,
        "non_zero": int(np.count_nonzero(values)),
        "size": int(values.size),
    }
