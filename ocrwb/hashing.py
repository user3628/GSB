"""Deterministic hashing helpers and canonical JSON serialization.

All persistence and fingerprinting goes through :func:`canonical_json` so that
the same logical content always serializes byte-for-byte identically, which is
what makes checkpoints, diagnoses and incremental rebuilds reproducible.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    """Serialize ``obj`` with sorted keys and stable separators (no whitespace)."""
    return json.dumps(
        obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_text(text: str) -> str:
    return digest_bytes(text.encode("utf-8"))


def digest_json(obj: Any) -> str:
    return digest_text(canonical_json(obj))


def digest_file(path: str, chunk_size: int = 1 << 16) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def digest_many(parts: list[str]) -> str:
    return digest_text("|".join(parts))
