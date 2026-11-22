"""Small deterministic helpers."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


def monotonic_ms() -> float:
    return time.monotonic() * 1000.0


def new_id(prefix: str = "") -> str:
    value = uuid.uuid4().hex
    return f"{prefix}{value}" if prefix else value


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def body_digest(body: Any) -> str:
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def require_int(value: Any, name: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        from .errors import E_INVALID_ARGUMENT, MWError

        raise MWError(E_INVALID_ARGUMENT, f"{name} must be an integer")
    if minimum is not None and value < minimum:
        from .errors import E_INVALID_ARGUMENT, MWError

        raise MWError(E_INVALID_ARGUMENT, f"{name} must be >= {minimum}")
    return value


def maybe_fault(point: str) -> None:
    """Abruptly end the process at a named fault-injection point.

    This intentionally does not use sys.exit/raise: no finally blocks or
    atexit handlers run, which is what the recovery tests need.
    """

    if os.environ.get("MQMW_FAULT_POINT") == point:
        os._exit(int(os.environ.get("MQMW_FAULT_EXIT_CODE", "97")))


def validate_loopback_url(value: str, name: str = "url") -> str:
    """Reject non-loopback consumer endpoints in this offline test harness."""

    from urllib.parse import urlparse

    from .errors import E_INVALID_ARGUMENT, MWError

    if not isinstance(value, str) or not value:
        raise MWError(E_INVALID_ARGUMENT, f"{name} must be a non-empty URL")
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise MWError(E_INVALID_ARGUMENT, f"{name} must be an http(s) URL")
    if parsed.hostname not in ("127.0.0.1", "::1", "localhost"):
        raise MWError(
            E_INVALID_ARGUMENT,
            f"{name} must target a loopback host; got {parsed.hostname!r}",
        )
    return value
