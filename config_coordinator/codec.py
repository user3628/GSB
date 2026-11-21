"""Deterministic JSON and digest helpers."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def digest_json(value: Any) -> str:
    return sha256_text(canonical_json(value))


def compute_snapshot_checksum(version: int, generated_at: str, configs: dict[str, Any]) -> str:
    """Checksum covers the complete snapshot envelope.

    ``generated_at`` is intentionally checksummed in its supplied ISO-8601
    representation.  This makes fixed sample files portable and prevents an
    accidental timezone-normalisation change from hiding an integrity error.
    """

    return digest_json(
        {
            "version": version,
            "generated_at": generated_at,
            "configs": configs,
        }
    )


def expected_config_digest(
    config_keys: list[str],
    expected_config_version: int,
    configs: dict[str, Any],
) -> str:
    selected = {key: configs.get(key) for key in sorted(set(config_keys))}
    return digest_json(
        {
            "expected_config_version": expected_config_version,
            "config_keys": sorted(set(config_keys)),
            "values": selected,
        }
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

