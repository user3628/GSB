"""Content fingerprints (SHA-256) used for versioning and deterministic output."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

import numpy as np


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fingerprint_bytes(data: bytes) -> str:
    return sha256_hex(data)


def canonical_json_bytes(obj: Any) -> bytes:
    """Deterministic JSON encoding.

    Keys are sorted, separators fixed and numpy scalars are converted to
    plain Python values so the same logical object always yields identical
    bytes on every platform and interpreter.
    """

    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
        allow_nan=True,
    ).encode("utf-8")


def canonical_json_text(obj: Any) -> str:
    """Pretty, still fully deterministic, JSON text used for report files."""

    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=False,
        indent=2,
        default=_json_default,
        allow_nan=True,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (set, tuple)):
        return list(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def fingerprint_json(obj: Any) -> str:
    return sha256_hex(canonical_json_bytes(obj))


def fingerprint_array(array: np.ndarray) -> str:
    """Stable fingerprint for a numpy array including dtype and shape."""

    array = np.ascontiguousarray(array)
    header = canonical_json_bytes(
        {"shape": list(array.shape), "dtype": str(array.dtype)}
    )
    digest = hashlib.sha256()
    digest.update(header)
    digest.update(b"\x00")
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


def combine_fingerprints(parts: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part.encode("ascii"))
        digest.update(b"\x1f")
    return digest.hexdigest()
