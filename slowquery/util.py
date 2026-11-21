from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_timestamp(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} is not a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_canonical(value: Any) -> str:
    return sha256_text(canonical_json(value))


def iso_at_or_after(new_value: str, old_value: str) -> bool:
    def parse(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    return parse(new_value) >= parse(old_value)


class Clock:
    def __init__(self, fn: Callable[[], str] | None = None) -> None:
        self._fn = fn or utc_now

    def now(self) -> str:
        return self._fn()


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
