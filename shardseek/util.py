"""Canonical serialization, hashing, and crash-safe file primitives."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable


def canonical_bytes(value: Any, *, pretty: bool = False) -> bytes:
    if pretty:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
    else:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    return (text + "\n").encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def digest_obj(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def jsonl_bytes(records: Iterable[dict[str, Any]]) -> bytes:
    return b"".join(canonical_bytes(record) for record in records)


def fsync_dir(path: str | Path) -> None:
    fd = os.open(Path(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_atomic(path: str | Path, data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
        fsync_dir(target.parent)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def write_json_atomic(path: str | Path, value: Any, *, pretty: bool = False) -> bytes:
    payload = canonical_bytes(value, pretty=pretty)
    write_atomic(path, payload)
    return payload


def read_json(path: str | Path) -> Any:
    with open(path, "rb") as handle:
        return json.loads(handle.read().decode("utf-8"))


def decimal_string(value: float, digits: int = 21) -> str:
    """Serialize a float without accepting non-finite values."""
    if value != value or value in (float("inf"), float("-inf")):
        raise ValueError("non-finite value")
    return format(value, f".{digits}g")


def rounded_float(value: float, digits: int = 15) -> float:
    if value == 0:
        return 0.0
    return float(format(value, f".{digits}g"))


def first_difference(items_a: list[str], items_b: list[str]) -> str | None:
    for a, b in zip(items_a, items_b):
        if a != b:
            return a
    if len(items_a) != len(items_b):
        return items_a[len(items_b)] if len(items_a) > len(items_b) else items_b[len(items_a)]
    return None

