"""Deterministic hashing helpers.

All fingerprints in the system are SHA-256 hex digests computed over a
canonical byte representation so that the same logical content always maps
to the same fingerprint regardless of dict ordering or platform.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json_bytes(obj: Any) -> bytes:
    """Serialise ``obj`` to canonical JSON bytes (sorted keys, tight separators)."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_obj(obj: Any) -> str:
    return hash_bytes(canonical_json_bytes(obj))


def hash_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_rows(rows: list[list[float]]) -> str:
    """Hash a float matrix in a platform-stable fixed-point representation."""
    h = hashlib.sha256()
    for row in rows:
        for v in row:
            h.update(f"{v:.10f},".encode("ascii"))
        h.update(b";")
    return h.hexdigest()
