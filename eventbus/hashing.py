from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import BusinessError


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def partition_for(topic_id: str, partition_key: str, partitions: int) -> int:
    digest = hashlib.sha256(f"{topic_id}\x1f{partition_key}".encode("utf-8")).digest()
    return int.from_bytes(digest, "big", signed=False) % partitions


def normalize_event_time(value: Any) -> str:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not a timestamp")
        if isinstance(value, (int, float)):
            if not math.isfinite(float(value)):
                raise ValueError("timestamp is not finite")
            parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
        elif isinstance(value, str) and value.strip():
            text = value.strip()
            if text.endswith(("Z", "z")):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
        else:
            raise ValueError("event_time must be ISO-8601 text or an epoch number")
    except (ValueError, TypeError, OverflowError, OSError) as exc:
        raise BusinessError(
            "E_EVENT_INVALID",
            "事件时刻无法解析。",
            {"field": "event_time", "value": str(value), "reason": str(exc)},
        ) from exc
    return parsed.isoformat(timespec="microseconds")


def parse_measure(value: Any) -> tuple[float | None, str | None]:
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BusinessError(
            "E_EVENT_INVALID",
            "度量值必须是有限数字或 null。",
            {"field": "measure", "value": repr(value), "reason": "not_numeric"},
        )
    if not math.isfinite(float(value)):
        raise BusinessError(
            "E_EVENT_INVALID",
            "度量值必须是有限数字或 null。",
            {"field": "measure", "value": str(value), "reason": "not_finite"},
        )
    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation as exc:
        raise BusinessError(
            "E_EVENT_INVALID",
            "度量值无法规范化。",
            {"field": "measure", "value": repr(value)},
        ) from exc
    canonical = format(decimal_value, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if canonical in {"", "-0"}:
        canonical = "0"
    return float(decimal_value), canonical


def body_digest(key: str, measure_canonical: str | None, payload: dict[str, Any]) -> str:
    body = {
        "key": key,
        "measure": measure_canonical,
        "payload": payload,
    }
    return sha256_text(canonical_json(body))


def event_fingerprint(
    *,
    topic_id: str,
    partition_key: str,
    event_time: str,
    key: str,
    measure_canonical: str | None,
    payload: dict[str, Any],
    digest: str,
) -> str:
    # batch_id and the bus-assigned production_seq are envelopes, not event body
    # content. Keeping them out allows a retried event in another batch to be
    # recognised as an idempotent republish.
    content = {
        "topic_id": topic_id,
        "partition_key": partition_key,
        "event_time": event_time,
        "key": key,
        "measure": measure_canonical,
        "payload": payload,
        "body_digest": digest,
    }
    return sha256_text(canonical_json(content))
