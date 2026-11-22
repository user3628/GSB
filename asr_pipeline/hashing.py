"""Deterministic hashing helpers independent of PYTHONHASHSEED."""

import hashlib
import json

import numpy as np


def stable_hash(*parts):
    """FNV-1a 64-bit hash over the UTF-8 rendering of the joined parts."""
    rendered = "|".join(str(part) for part in parts)
    h = 0xCBF29CE484222325
    for byte in rendered.encode("utf-8"):
        h ^= byte
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def sha256_file(path, chunk_size=1 << 20):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def canonical_json(obj):
    """Deterministic JSON rendering used for fingerprints and checkpoints."""
    return json.dumps(
        obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def digest_json(obj):
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def rng_from_seed(*parts):
    """numpy Generator seeded deterministically from arbitrary parts."""
    seed = stable_hash(*parts)
    return np.random.default_rng(seed)
