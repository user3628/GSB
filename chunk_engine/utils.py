from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value))


@contextmanager
def atomic_text_writer(path: Path):
    """Stream text into a same-directory temporary file and publish atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    handle = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
    try:
        yield handle
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temp_name, path)
    except BaseException:
        try:
            handle.close()
        except OSError:
            pass
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def strict_json_loads(data: str | bytes) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(data, object_pairs_hook=reject_duplicates)


def format_float_fixed(value: float, decimal_places: int = 6) -> str:
    if not math.isfinite(value):
        raise ValueError("non-finite floats are not accepted in result JSON")
    if value == 0.0:
        value = 0.0
    return f"{value:.{decimal_places}f}"


def result_value_for_json(dtype: str, value: Any, decimal_places: int = 6) -> Any:
    if value is None:
        return None
    if dtype == "float64":
        return format_float_fixed(float(value), decimal_places)
    if dtype == "int64":
        return int(value)
    if dtype == "string":
        return str(value)
    raise ValueError(f"unsupported dtype: {dtype}")


def exact_value_key(dtype: str, value: Any) -> Any:
    if value is None:
        return None
    if dtype == "float64":
        return float(value).hex()
    if dtype == "int64":
        return int(value)
    if dtype == "string":
        return str(value)
    raise ValueError(f"unsupported dtype: {dtype}")


def ceil_div(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator
