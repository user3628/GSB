"""Canonical JSON checksums used by snapshots, intents and artifacts."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def sha256_value(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def payload_without_checksum(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "checksum"}


def compute_payload_checksum(payload: dict[str, Any]) -> str:
    return sha256_value(payload_without_checksum(payload))


def checksum_matches(payload: dict[str, Any]) -> bool:
    supplied = payload.get("checksum")
    if not isinstance(supplied, str):
        return False
    return hmac.compare_digest(supplied, compute_payload_checksum(payload))

