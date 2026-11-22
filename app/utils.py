from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso_z(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_datetime(value: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("timestamp must be a non-empty ISO-8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def snapshot_checksum_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": snapshot["version"],
        "generated_at": snapshot["generated_at"],
        "layers": [
            {"id": layer["id"], "capacity_bytes": layer["capacity_bytes"]}
            for layer in snapshot["layers"]
        ],
        "heat_threshold": snapshot["heat_threshold"],
        "batch_limit": snapshot["batch_limit"],
        "cooldown_seconds": snapshot["cooldown_seconds"],
    }


def snapshot_checksum(snapshot: dict[str, Any]) -> str:
    return sha256_json(snapshot_checksum_payload(snapshot))


def heat_bucket(heat: int) -> int:
    return min(15, max(0, heat // 16))


def heat_histogram(heats: list[int]) -> dict[str, int]:
    buckets = [0] * 16
    for heat in heats:
        buckets[heat_bucket(heat)] += 1
    return {f"{index * 16:03d}-{index * 16 + 15:03d}": count for index, count in enumerate(buckets)}
