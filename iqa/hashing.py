"""Deterministic hashing helpers.

All persistent fingerprints and hash-based token mapping go through these
functions so that results are reproducible byte-for-byte across runs and
Python invocations.
"""

import hashlib
import math

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
FNV_MASK = (1 << 64) - 1


def fnv1a64(text):
    """64-bit FNV-1a hash of a Unicode string (encoded as UTF-8)."""
    value = FNV_OFFSET
    for byte in text.encode("utf-8"):
        value ^= byte
        value = (value * FNV_PRIME) & FNV_MASK
    return value


def _round_float(value):
    if math.isnan(value) or math.isinf(value):
        raise ValueError("non-finite float cannot be canonicalized")
    return round(float(value), 10)


def canonical(obj):
    """Return a deterministic JSON string for nested JSON-like data.

    Floats are rounded to 10 decimal places and keys are sorted, so equal
    logical structures always serialise to identical bytes.
    """
    import json

    def convert(item):
        if isinstance(item, float):
            return _round_float(item)
        if isinstance(item, dict):
            return {str(key): convert(value) for key, value in sorted(item.items())}
        if isinstance(item, (list, tuple)):
            return [convert(value) for value in item]
        if isinstance(item, int) and not isinstance(item, bool):
            return item
        if isinstance(item, bool) or item is None or isinstance(item, str):
            return item
        if hasattr(item, "item"):
            return convert(item.item())
        return str(item)

    return json.dumps(convert(obj), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(obj):
    """SHA-256 hex digest of the canonical form of ``obj``."""
    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()


def file_digest(path):
    """SHA-256 hex digest of a file's raw bytes."""
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def round_floats(obj, digits=10):
    """Recursively round floats so in-memory and on-disk values match."""
    if isinstance(obj, float):
        return round(obj, digits)
    if isinstance(obj, dict):
        return {key: round_floats(value, digits) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [round_floats(value, digits) for value in obj]
    return obj
