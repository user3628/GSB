"""Deterministic serialisation and hashing helpers.

Byte-for-byte reproducibility is a core acceptance criterion, therefore every
canonical dump uses ``sort_keys=True``, a fixed separator set and a trailing
newline.  All fingerprints are SHA-256 hex digests of those canonical bytes.
"""

import hashlib
import json

CANONICAL_KWARGS = {
    "ensure_ascii": False,
    "sort_keys": True,
    "separators": (",", ":"),
}


def canonical_bytes(obj):
    """Return canonical UTF-8 bytes (sorted keys, no whitespace)."""
    return json.dumps(obj, **CANONICAL_KWARGS).encode("utf-8")


def canonical_text(obj, indent=None):
    """Deterministic JSON text.  ``indent`` is used only for human artifacts."""
    if indent is None:
        return json.dumps(obj, **CANONICAL_KWARGS)
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=indent)


def fingerprint(obj):
    """SHA-256 fingerprint of the canonical representation of ``obj``."""
    return hashlib.sha256(canonical_bytes(obj)).hexdigest()


def write_jsonl_atomic(path, records):
    """Write records as deterministic UTF-8 JSONL via a temp-file + rename."""
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        for rec in records:
            fh.write(canonical_text(rec))
            fh.write("\n")
    tmp.replace(path)


def read_jsonl(path):
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def write_json_atomic(path, obj, indent=2):
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(canonical_text(obj, indent=indent))
        fh.write("\n")
    tmp.replace(path)


def read_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)
