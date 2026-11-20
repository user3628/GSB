from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from .errors import ConfigHubError, ErrorCode


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="microseconds")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def json_loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def normalize_config(config: dict[str, Any]) -> dict[str, float]:
    if not isinstance(config, dict) or not config:
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "config must be a non-empty object",
            status_code=422,
        )
    result: dict[str, float] = {}
    for key, value in config.items():
        if not isinstance(key, str) or not key:
            raise ConfigHubError(
                ErrorCode.E_VALIDATION,
                "configuration keys must be non-empty strings",
                status_code=422,
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigHubError(
                ErrorCode.E_VALIDATION,
                f"configuration value for {key!r} must be numeric",
                status_code=422,
            )
        number = float(value)
        if not math.isfinite(number):
            raise ConfigHubError(
                ErrorCode.E_VALIDATION,
                f"configuration value for {key!r} must be finite",
                status_code=422,
            )
        result[key] = number
    return result


def validate_loopback_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(endpoint_url)
    if parsed.scheme not in {"http", "https"}:
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "endpoint_url must use http or https",
            status_code=422,
        )
    if not parsed.hostname or parsed.port is None:
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "endpoint_url must contain a host and an explicit port",
            status_code=422,
        )
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ConfigHubError(
            ErrorCode.E_EXTERNAL_TARGET,
            "only loopback client endpoints are permitted",
            status_code=422,
            details={"hostname": parsed.hostname},
        )
    return endpoint_url


def flatten_numeric(value: Any, prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if isinstance(value, dict):
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            flat.update(flatten_numeric(value[key], child))
    elif isinstance(value, list):
        flat[prefix] = json.dumps(value, separators=(",", ":"), sort_keys=True)
    else:
        flat[prefix] = value
    return flat


def differing_fields(reference: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    left = flatten_numeric(reference)
    right = flatten_numeric(candidate)
    fields = sorted(set(left) | set(right))
    return [field for field in fields if left.get(field) != right.get(field)]

