from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from .errors import (
    E_ENDPOINT_INVALID,
    E_PROBE_METHOD_INVALID,
    E_PROBE_PARAMS_INVALID,
    BusinessError,
)
from .models import PROBE_METHODS


LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def default_probe_params(method: str) -> dict[str, Any]:
    if method == "http":
        return {
            "timeout_ms": 1000,
            "expected_status": 200,
            "expected_body_contains": "OK",
        }
    if method == "tcp":
        return {"timeout_ms": 1000}
    if method == "lease":
        return {"timeout_ms": 250}
    return {}


def validate_probe_method(method: str, *, record_index: int | None = None) -> str:
    normalized = (method or "").strip().lower()
    if normalized not in PROBE_METHODS:
        raise BusinessError(
            E_PROBE_METHOD_INVALID,
            "probe method is not allowed",
            details={
                "record_index": record_index,
                "probe_method": method,
                "allowed": list(PROBE_METHODS),
            },
        )
    return normalized


def validate_endpoint(endpoint: str, probe_method: str, *, record_index: int | None = None) -> None:
    if not isinstance(endpoint, str) or not endpoint:
        raise BusinessError(
            E_ENDPOINT_INVALID,
            "endpoint must be a non-empty string",
            details={"record_index": record_index, "endpoint": endpoint},
        )
    parsed = urlsplit(endpoint)
    scheme = parsed.scheme.lower()
    allowed_schemes = {"tcp"} if probe_method == "tcp" else {"http", "https"}
    if scheme not in allowed_schemes:
        raise BusinessError(
            E_ENDPOINT_INVALID,
            "endpoint scheme does not match probe method",
            details={
                "record_index": record_index,
                "endpoint": endpoint,
                "probe_method": probe_method,
                "allowed_schemes": sorted(allowed_schemes),
            },
        )
    if parsed.hostname not in LOOPBACK_HOSTS:
        raise BusinessError(
            E_ENDPOINT_INVALID,
            "only loopback endpoints are accepted",
            details={"record_index": record_index, "endpoint": endpoint, "host": parsed.hostname},
        )
    try:
        port = parsed.port
    except ValueError as exc:
        raise BusinessError(
            E_ENDPOINT_INVALID,
            "endpoint contains an invalid port",
            details={"record_index": record_index, "endpoint": endpoint},
        ) from exc
    if port is None or not 1 <= port <= 65535:
        raise BusinessError(
            E_ENDPOINT_INVALID,
            "endpoint must contain a port within 1..65535",
            details={"record_index": record_index, "endpoint": endpoint, "port": port},
        )


def _positive_int(value: Any, name: str, *, record_index: int | None, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise BusinessError(
            E_PROBE_PARAMS_INVALID,
            f"{name} must be an integer within 1..{maximum}",
            details={"record_index": record_index, "parameter": name, "value": value},
        )
    return value


def validate_probe_params(
    method: str,
    params: dict[str, Any],
    *,
    record_index: int | None = None,
) -> dict[str, Any]:
    if not isinstance(params, dict):
        raise BusinessError(
            E_PROBE_PARAMS_INVALID,
            "probe params must be an object",
            details={"record_index": record_index, "probe_method": method},
        )
    result = dict(params)
    _positive_int(result.get("timeout_ms", 1000), "timeout_ms", record_index=record_index, maximum=600_000)
    if method == "http":
        _positive_int(
            result.get("expected_status", 200),
            "expected_status",
            record_index=record_index,
            maximum=599,
        )
        marker = result.get("expected_body_contains", "OK")
        if not isinstance(marker, str):
            raise BusinessError(
                E_PROBE_PARAMS_INVALID,
                "expected_body_contains must be a string",
                details={"record_index": record_index, "value": marker},
            )
        path = result.get("path")
        if path is not None and (not isinstance(path, str) or not path.startswith("/")):
            raise BusinessError(
                E_PROBE_PARAMS_INVALID,
                "HTTP probe path must start with /",
                details={"record_index": record_index, "path": path},
            )
    return result
