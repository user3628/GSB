"""Canonical serialization and digest helpers.

All public content hashes are computed from canonical JSON or canonical
little-endian float64 bytes. Dict insertion order and NumPy hash randomization
therefore never influence a fingerprint.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable

import numpy as np


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def digest_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _normalize_float64(array: np.ndarray) -> np.ndarray:
    result = np.asarray(array, dtype=np.float64)
    # A negative zero is numerically equal to positive zero. Normalizing makes
    # byte digests agree even if an otherwise equivalent kernel emits -0.0.
    return result + np.float64(0.0)


def array_bytes(array: np.ndarray) -> bytes:
    normalized = _normalize_float64(array)
    header = canonical_bytes(
        {"dtype": "float64-le", "shape": list(normalized.shape), "order": "C"}
    )
    return header + b"\n" + normalized.astype("<f8", copy=False).tobytes(order="C")


def digest_array(array: np.ndarray) -> str:
    return sha256_bytes(array_bytes(array))


def digest_int_array(array: np.ndarray) -> str:
    normalized = np.asarray(array, dtype="<i8")
    header = canonical_bytes(
        {"dtype": "int64-le", "shape": list(normalized.shape), "order": "C"}
    )
    return sha256_bytes(header + b"\n" + normalized.tobytes(order="C"))


def digest_arrays(arrays: Iterable[tuple[str, np.ndarray]]) -> str:
    """Digest named arrays without depending on zip timestamps."""

    hasher = hashlib.sha256()
    for name, array in sorted(arrays, key=lambda item: item[0]):
        hasher.update(canonical_bytes({"name": name}))
        hasher.update(b"\n")
        hasher.update(array_bytes(array))
        hasher.update(b"\n")
    return hasher.hexdigest()


def require_plain_float(value: Any, field_name: str, error_factory: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise error_factory(
            f"{field_name} must be a JSON number", field=field_name, value=value
        )
    number = float(value)
    if not math.isfinite(number):
        raise error_factory(
            f"{field_name} must be finite", field=field_name, value=value
        )
    return number


def require_plain_int(value: Any, field_name: str, error_factory: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise error_factory(
            f"{field_name} must be a JSON integer", field=field_name, value=value
        )
    return int(value)
