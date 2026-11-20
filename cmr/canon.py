"""Canonical (byte-deterministic) serialization and hashing helpers."""

import hashlib
import json


def canon_json(obj):
    """Serialize an object to canonical JSON bytes-stable text.

    Keys sorted, no whitespace, UTF-8 (non-ASCII kept), NaN/Inf rejected so
    persisted records never carry non-deterministic numerics.
    """
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def digest_obj(obj):
    return sha256_bytes(canon_json(obj).encode("utf-8"))


def digest_bytes(data):
    return sha256_bytes(data)


def digest_file(path):
    with open(path, "rb") as fh:
        return sha256_bytes(fh.read())


def vec_digest(vec):
    """Digest of a float64 numpy vector's raw bytes (byte-identical per platform)."""
    import numpy as np

    arr = np.ascontiguousarray(np.asarray(vec, dtype=np.float64))
    return sha256_bytes(arr.tobytes())
