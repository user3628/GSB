"""Deterministic hashing primitives independent of PYTHONHASHSEED."""
from __future__ import annotations

import hashlib
import struct

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
MASK64 = 0xFFFFFFFFFFFFFFFF


def fnv1a64(data: bytes) -> int:
    """64-bit FNV-1a hash, fully deterministic across runs/machines."""
    h = FNV_OFFSET
    for byte in data:
        h ^= byte
        h = (h * FNV_PRIME) & MASK64
    return h


def hash_str(text: str) -> int:
    return fnv1a64(text.encode("utf-8"))


def hash_parts(*parts: object) -> int:
    joined = "\x1f".join(str(p) for p in parts)
    return hash_str(joined)


def stable_float(text: str) -> float:
    """Map text deterministically to a float in [0.0, 1.0)."""
    digest = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
    (uint64,) = struct.unpack(">Q", digest)
    return uint64 / float(1 << 64)


def canonical_fingerprint(payload: bytes) -> str:
    """SHA-256 fingerprint used for input/config/version identity."""
    return hashlib.sha256(payload).hexdigest()
