"""Deterministic content fingerprints used for invalidation tracking."""
from __future__ import annotations

import hashlib
import json


def canonical_json(obj) -> str:
    """Canonical JSON serialization: sorted keys, no whitespace ambiguity."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint_of_obj(obj) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def fingerprint_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fingerprint_of_file(path) -> str:
    with open(path, "rb") as fh:
        return fingerprint_of_bytes(fh.read())
