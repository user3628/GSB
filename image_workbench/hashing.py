from __future__ import annotations

import hashlib
import json
from typing import Any

import numpy as np


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_bytes(value: bytes) -> str:
    return sha256_bytes(value)


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def digest_array(array: np.ndarray, *, role: str = "array") -> str:
    contiguous = np.ascontiguousarray(array)
    header = f"{role}|{contiguous.dtype.str}|{contiguous.shape}|".encode("ascii")
    return sha256_bytes(header + contiguous.tobytes(order="C"))


def short_digest(value: str, length: int = 16) -> str:
    return value[:length]

