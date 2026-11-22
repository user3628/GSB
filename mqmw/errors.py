"""Error types returned by the public API."""

from __future__ import annotations

from typing import Any


class MWError(Exception):
    """A domain error with a stable machine-readable code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int = 400,
        details: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"ok": False, "error": {"code": self.code, "message": self.message}}
        if self.details is not None:
            payload["error"]["details"] = self.details
        return payload


E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_DUPLICATE_ACK = "E_DUPLICATE_ACK"
E_ALREADY_ACKED = "E_ALREADY_ACKED"
E_LEASE_HELD = "E_LEASE_HELD"
E_LEASE_LOST = "E_LEASE_LOST"
E_LEASE_EXPIRED = "E_LEASE_EXPIRED"
E_QUEUE_FULL = "E_QUEUE_FULL"
E_DISPATCH_TIMEOUT = "E_DISPATCH_TIMEOUT"
E_DISPATCH_REFUSED = "E_DISPATCH_REFUSED"
E_DISPATCH_ERROR = "E_DISPATCH_ERROR"
E_NOT_FOUND = "E_NOT_FOUND"
E_INVALID_ARGUMENT = "E_INVALID_ARGUMENT"
E_REQUEST_CONFLICT = "E_REQUEST_CONFLICT"
E_REQUEST_TIMEOUT = "E_REQUEST_TIMEOUT"
E_ROUND_IN_PROGRESS = "E_ROUND_IN_PROGRESS"
E_RECOVERY = "E_RECOVERY"
E_INTERNAL = "E_INTERNAL"
