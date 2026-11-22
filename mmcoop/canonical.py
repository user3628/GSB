from __future__ import annotations

import hashlib
import json
import math
import os
import secrets
from pathlib import Path
from typing import Any


def canonical_bytes(value: Any) -> bytes:
    """Deterministic UTF-8 JSON used for every content fingerprint and artifact."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_text(value: Any, *, indent: int | None = None) -> str:
    kwargs: dict[str, Any] = {
        "ensure_ascii": False,
        "sort_keys": True,
        "allow_nan": False,
    }
    if indent is None:
        kwargs["separators"] = (",", ":")
    else:
        kwargs["indent"] = indent
    return json.dumps(value, **kwargs)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def round_float(value: float, digits: int = 12) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite float cannot be serialized")
    rounded = round(number, digits)
    if rounded == 0:
        return 0.0
    return rounded


def round_vector(values: Any, digits: int = 12) -> list[float]:
    return [round_float(float(item), digits) for item in values]


def fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp.{os.getpid()}.{secrets.token_hex(6)}"
    try:
        with temporary.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if temporary.exists():
            try:
                temporary.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, value: Any, *, indent: int | None = None) -> bytes:
    text = canonical_text(value, indent=indent)
    data = (text + ("\n" if indent is not None else "")).encode("utf-8")
    atomic_write_bytes(path, data)
    return data


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def first_difference(left: Any, right: Any, path: str = "$") -> dict[str, Any] | None:
    """Return a compact, deterministic location for the first JSON difference."""
    if type(left) is not type(right):
        return {"path": path, "left_type": type(left).__name__, "right_type": type(right).__name__}
    if isinstance(left, dict):
        left_keys = sorted(left)
        right_keys = sorted(right)
        if left_keys != right_keys:
            missing_left = sorted(set(right_keys) - set(left_keys))
            missing_right = sorted(set(left_keys) - set(right_keys))
            return {"path": path, "left_missing_keys": missing_left, "right_missing_keys": missing_right}
        for key in left_keys:
            difference = first_difference(left[key], right[key], f"{path}.{key}")
            if difference is not None:
                return difference
        return None
    if isinstance(left, list):
        if len(left) != len(right):
            return {"path": path, "left_length": len(left), "right_length": len(right)}
        for index, (left_item, right_item) in enumerate(zip(left, right, strict=True)):
            difference = first_difference(left_item, right_item, f"{path}[{index}]")
            if difference is not None:
                return difference
        return None
    if left != right:
        return {
            "path": path,
            "left": repr(left)[:160],
            "right": repr(right)[:160],
        }
    return None

