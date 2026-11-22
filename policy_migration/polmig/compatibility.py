"""Input format detection and the explicit version compatibility matrix."""

import json

from .codecs import MAGIC
from .errors import FormatUnsupported

KNOWN_NEW_VERSION = 1

COMPATIBILITY_MATRIX = [
    {"format": "binary", "version": 1, "verdict": "ACCEPT",
     "reason": "current compact binary format, all features understood"},
    {"format": "jsonl", "version": 1, "verdict": "ACCEPT",
     "reason": "current sharded JSON-lines format, all features understood"},
    {"format": "legacy-json", "version": 1, "verdict": "DEGRADED",
     "reason": "flat model lacks parent_ref and weighted preempt_tier; "
               "defaults are synthesized and recorded in the decision report"},
    {"format": "binary", "version": 2, "verdict": "REJECT",
     "reason": "future version may carry unknown required fields"},
    {"format": "jsonl", "version": 2, "verdict": "REJECT",
     "reason": "future version may carry unknown required fields"},
]


def matrix_lookup(fmt, version):
    for entry in COMPATIBILITY_MATRIX:
        if entry["format"] == fmt and entry["version"] == version:
            return dict(entry)
    return None


def detect_format(data):
    """Return (format_name, version_or_None) for a raw payload."""
    if isinstance(data, (bytes, bytearray)):
        if bytes(data[:8]) == MAGIC:
            version = data[8] if len(data) >= 9 else None
            return "binary", version
        try:
            text = bytes(data).decode("utf-8")
        except UnicodeDecodeError:
            return "unknown", None
    else:
        text = data
    stripped = text.strip()
    if not stripped:
        return "unknown", None
    try:
        obj = json.loads(stripped)
    except json.JSONDecodeError:
        try:
            obj = json.loads(stripped.splitlines()[0])
        except json.JSONDecodeError:
            return "unknown", None
    if not isinstance(obj, dict):
        return "unknown", None
    if obj.get("schema") == "legacy-flat" or "policies" in obj:
        return "legacy-json", 1
    if "preempt_enabled" in obj or "concurrency_limit" in obj and "preempt_tier" not in obj:
        return "legacy-json", 1
    if "preempt_tier" in obj or "lifetime" in obj:
        return "jsonl", KNOWN_NEW_VERSION
    return "unknown", None


def negotiate(data):
    """Apply the compatibility matrix before any decoding/migration.

    Returns a report dict; raises FormatUnsupported for REJECT/unknown input.
    """
    fmt, version = detect_format(data)
    entry = matrix_lookup(fmt, version)
    if fmt == "unknown" or entry is None:
        raise FormatUnsupported(
            "unrecognized input format or unsupported version",
            {"detected_format": fmt, "detected_version": version},
        )
    report = {
        "detected_format": fmt,
        "detected_version": version,
        "verdict": entry["verdict"],
        "reason": entry["reason"],
        "degradations": [],
    }
    if entry["verdict"] == "REJECT":
        raise FormatUnsupported(entry["reason"], report)
    if fmt == "legacy-json":
        report["degradations"] = [
            {
                "field": "parent_ref",
                "rule": "default to null (root tenant, inherit platform quota)",
            },
            {
                "field": "preempt_tier",
                "rule": "boolean preempt_enabled mapped to weighted tier "
                        "(true -> 60, false -> 10)",
            },
            {
                "field": "lifetime",
                "rule": "synthesize explicit lifetime from epoch with default 365d TTL",
            },
        ]
    return report
