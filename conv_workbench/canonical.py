"""Canonical serialization, hashing, and deterministic NumPy artifacts."""

from __future__ import annotations

import hashlib
import json
from io import BytesIO
from typing import Any

import numpy as np


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def object_fingerprint(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def chained_fingerprint(*parts: Any) -> str:
    payload = {"parts": list(parts)}
    return object_fingerprint(payload)


def npy_bytes(array: np.ndarray) -> bytes:
    """Serialize an array without pickle.

    The workbench uses these exact bytes both for published artifacts and for
    final exports, so a resumed run and a clean run can be compared byte-wise.
    """

    buffer = BytesIO()
    np.save(buffer, np.asarray(array), allow_pickle=False)
    return buffer.getvalue()


def array_from_bytes(data: bytes) -> np.ndarray:
    array = np.load(BytesIO(data), allow_pickle=False)
    if not isinstance(array, np.ndarray):
        raise ValueError("artifact is not an ndarray")
    return array

