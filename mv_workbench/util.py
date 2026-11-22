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


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_json_object(raw: bytes | str, *, source: str) -> dict[str, Any]:
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        from .errors import fail

        fail(
            "E_STALE_BASE",
            "Input JSON could not be parsed; no committed state was changed.",
            context={"source": source, "detail": str(exc)},
            status_code=409,
        )
    if not isinstance(value, dict):
        from .errors import fail

        fail(
            "E_STALE_BASE",
            "Input JSON must be an object; no committed state was changed.",
            context={"source": source, "actual_type": type(value).__name__},
            status_code=409,
        )
    return value


def require_dict(value: Any, *, field: str) -> dict[str, Any]:
    from .errors import fail

    if not isinstance(value, dict):
        fail(
            "E_INVALID_REQUEST",
            f"{field} must be an object.",
            context={"field": field, "actual_type": type(value).__name__},
        )
    return value


def require_list(value: Any, *, field: str) -> list[Any]:
    from .errors import fail

    if not isinstance(value, list):
        fail(
            "E_INVALID_REQUEST",
            f"{field} must be an array.",
            context={"field": field, "actual_type": type(value).__name__},
        )
    return value


def require_str(value: Any, *, field: str) -> str:
    from .errors import fail

    if not isinstance(value, str) or not value:
        fail(
            "E_INVALID_REQUEST",
            f"{field} must be a non-empty string.",
            context={"field": field, "actual_type": type(value).__name__},
        )
    return value
