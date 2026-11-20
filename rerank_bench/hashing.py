"""Deterministic hashing and fixed-point arithmetic helpers.

All scoring is done in fixed-point integer arithmetic (micro units, scale
1e6) so results are byte-identical across runs, processes and platforms.
"""

from __future__ import annotations

import hashlib
import json

SCALE = 1_000_000  # fixed-point scale: 6 decimal places


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def content_digest(obj) -> str:
    """Canonical digest of any JSON-serialisable object."""
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_hex(blob)


def file_digest(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def to_micro(value) -> int:
    """Convert a JSON number to fixed-point micro units deterministically."""
    return int(round(float(value) * SCALE))


def micro_to_str(micro: int) -> str:
    """Render micro units as a fixed 6-decimal string (byte-stable)."""
    sign = "-" if micro < 0 else ""
    mag = abs(micro)
    return f"{sign}{mag // SCALE}.{mag % SCALE:06d}"


def embedding_vector(query_id: str, doc_id: str, dim: int) -> list[int]:
    """Deterministic embedding in [0, SCALE) derived from sha256 chunks."""
    vec = []
    for i in range(dim):
        digest = hashlib.sha256(f"emb|{query_id}|{doc_id}|{i}".encode("utf-8")).digest()
        vec.append(int.from_bytes(digest[:8], "big") % SCALE)
    return vec
