"""Stable business errors shared by the proxy, admin API, and tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ProxyError(Exception):
    code: str
    message: str
    http_status: int = 502
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


E_CONFIG_INVALID = "E_CONFIG_INVALID"
E_STALE_BASE = "E_STALE_BASE"
E_VERSION_ROLLBACK = "E_VERSION_ROLLBACK"
E_CONFIG_CHANGED = "E_CONFIG_CHANGED"
E_ROUTE_NOT_FOUND = "E_ROUTE_NOT_FOUND"
E_UPSTREAM_UNAVAILABLE = "E_UPSTREAM_UNAVAILABLE"
E_CONNECT_TIMEOUT = "E_CONNECT_TIMEOUT"
E_READ_TIMEOUT = "E_READ_TIMEOUT"
E_WRITE_TIMEOUT = "E_WRITE_TIMEOUT"
E_CONNECTION_REFUSED = "E_CONNECTION_REFUSED"
E_CONNECT_FAILED = "E_CONNECT_FAILED"
E_UPSTREAM_5XX = "E_UPSTREAM_5XX"
E_UPSTREAM_PROTOCOL = "E_UPSTREAM_PROTOCOL"
E_REQUEST_INVALID = "E_REQUEST_INVALID"
E_REQUEST_TOO_LARGE = "E_REQUEST_TOO_LARGE"
E_UNSUPPORTED_TARGET = "E_UNSUPPORTED_TARGET"
E_SHUTTING_DOWN = "E_SHUTTING_DOWN"
E_PERSISTENCE = "E_PERSISTENCE"
E_INTERNAL = "E_INTERNAL"


def config_error(message: str, **details: Any) -> ProxyError:
    return ProxyError(E_CONFIG_INVALID, message, 400, details)

