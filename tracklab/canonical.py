"""Canonical serialization and hashing helpers used by every persisted stage."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable


FLOAT_PLACES = 12


def stable_float(value: float, places: int = FLOAT_PLACES) -> float:
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"non-finite float cannot be serialized: {value!r}")
    rounded = round(value, places)
    if abs(rounded) < 0.5 * (10.0 ** -places):
        return 0.0
    return rounded


def stable_value(value: Any, *, places: int = FLOAT_PLACES) -> Any:
    """Recursively remove platform-sensitive float tails and negative zero."""
    if isinstance(value, float):
        return stable_float(value, places)
    if isinstance(value, dict):
        return {str(k): stable_value(v, places=places) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [stable_value(v, places=places) for v in value]
    return value


def canonical_json_text(value: Any, *, sort_keys: bool = True) -> str:
    stable = stable_value(value)
    return json.dumps(
        stable,
        sort_keys=sort_keys,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def canonical_json_bytes(value: Any, *, sort_keys: bool = True) -> bytes:
    return canonical_json_text(value, sort_keys=sort_keys).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes atomically in the target directory, with fsync boundaries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        _fsync_dir(path.parent)
    finally:
        try:
            if temp.exists():
                temp.unlink()
        except OSError:
            pass


def atomic_write_json(path: Path, value: Any, *, pretty: bool = False) -> bytes:
    if pretty:
        payload = json.dumps(
            stable_value(value),
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8") + b"\n"
    else:
        payload = canonical_json_bytes(value)
    atomic_write_bytes(path, payload)
    return payload


def _fsync_dir(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def hash_tree(root: Path) -> dict[str, str]:
    """Stable tree digest mapping relative POSIX paths to SHA-256 values."""
    if not root.exists():
        return {}
    result: dict[str, str] = {}
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel = path.relative_to(root).as_posix()
        result[rel] = sha256_file(path)
    return result


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)
