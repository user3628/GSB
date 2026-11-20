"""Deterministic hashing helpers.

Everything here is pure-Python and independent of ``hash()`` randomization or
the host word size, so outputs are byte-identical across runs and machines.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Iterable

MASK64 = (1 << 64) - 1
FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
_WS_RE = re.compile(r"\s+")


def fnv1a64(data: bytes) -> int:
    h = FNV_OFFSET
    for byte in data:
        h ^= byte
        h = (h * FNV_PRIME) & MASK64
    return h


def stable_hash64(text: str | bytes) -> str:
    """Return a 16-char zero-padded hex digest of a stable 64-bit hash."""
    if isinstance(text, str):
        text = text.encode("utf-8")
    return f"{fnv1a64(text):016x}"


def seeded_hash64(text: str, seed: int) -> int:
    """64-bit hash mixed with an integer seed (FNV-1a over seed-prefixed bytes)."""
    data = seed.to_bytes(8, "big", signed=False) + text.encode("utf-8")
    return fnv1a64(data)


def canonical_json(obj: Any) -> str:
    """Canonical JSON text: sorted keys, no whitespace, stable separators."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False, default=_json_default)


def _json_default(obj):
    if isinstance(obj, set):
        return sorted(obj)
    if hasattr(obj, "tolist"):  # numpy scalar/array
        return obj.tolist()
    raise TypeError(f"not JSON serializable: {type(obj)!r}")


def sha256_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_text(value: Any) -> str:
    """NFKC normalization, whitespace folding and lower-casing."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = _WS_RE.sub(" ", text).strip().lower()
    return text


def tokenize(text: str) -> list[str]:
    """Whitespace tokenizer over normalized text."""
    return [token for token in text.split(" ") if token]


def simhash64(tokens: Iterable[str], seed: int = 0) -> int:
    """64-bit SimHash over token 64-bit hashes."""
    bits = [0] * 64
    count = 0
    for token in tokens:
        token_hash = seeded_hash64(token, seed)
        for index in range(64):
            bits[index] += 1 if (token_hash >> index) & 1 else -1
        count += 1
    if count == 0:
        return 0
    fingerprint = 0
    for index, score in enumerate(bits):
        if score > 0:
            fingerprint |= 1 << index
        elif score == 0:
            # Deterministic tie-break: 0 bit (do nothing) rather than host-dependent.
            continue
    return fingerprint & MASK64
