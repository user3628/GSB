from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_HTTP_STATUS = {
    "E_SNAPSHOT_INVALID": 400,
    "E_FRAME_INVALID": 400,
    "E_STALE_BASE": 409,
    "E_CONFLICTING_FRAME": 409,
    "E_CHANNEL_QUEUE_FULL": 429,
    "E_TUNNEL_TIMEOUT": 504,
    "E_REBIND_IN_PROGRESS": 409,
    "E_RECOVERY_REQUIRED": 503,
    "E_NOT_FOUND": 404,
    "E_REQUEST_INVALID": 400,
    "E_INTERNAL": 500,
}


@dataclass(slots=True)
class GatewayError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)
        if self.http_status is None:
            self.http_status = ERROR_HTTP_STATUS.get(self.code, 500)

    def as_payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            },
        }


def as_gateway_error(exc: Exception, code: str = "E_INTERNAL") -> GatewayError:
    if isinstance(exc, GatewayError):
        return exc
    return GatewayError(code, str(exc), {"exception": type(exc).__name__})

