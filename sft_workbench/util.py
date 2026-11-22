"""Deterministic serialization and digest helpers.

All persisted artifacts in the workbench are produced through these helpers so
that identical inputs always yield byte-identical outputs.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    """Canonical JSON: sorted keys, no ASCII escaping, tight separators."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_bytes(obj: Any) -> bytes:
    return canonical_json(obj).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_obj(obj: Any) -> str:
    """Content digest of any JSON-serializable object."""
    return sha256_hex(canonical_bytes(obj))


def quantize(value: float) -> float:
    """Quantize a float to 6 decimal places for stable round-trips."""
    return round(float(value), 6)
