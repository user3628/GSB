"""Canonical serialization and content digests.

Every byte-level comparison in the workbench goes through these helpers so
that identical logical content always produces identical bytes.
"""

import hashlib
import json


def canonical(obj):
    """Serialize obj to deterministic UTF-8 bytes."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_text(obj):
    return canonical(obj).decode("utf-8")


def digest_bytes(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def digest(obj):
    return digest_bytes(canonical(obj))
