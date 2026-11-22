"""Business errors and stable error codes."""

from __future__ import annotations

from typing import Any


OK = "OK"
E_LOCK_HELD = "E_LOCK_HELD"
E_STALE_LEASE = "E_STALE_LEASE"
E_LEASE_EXPIRED = "E_LEASE_EXPIRED"
E_NOT_HOLDER = "E_NOT_HOLDER"
E_LOCK_NOT_HELD = "E_LOCK_NOT_HELD"
E_CONFLICTING_REQUEST = "E_CONFLICTING_REQUEST"
E_INVALID_REQUEST = "E_INVALID_REQUEST"
E_INTERNAL = "E_INTERNAL"


class LeaseError(Exception):
    """An expected business failure which is safe to persist as a response."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            **self.details,
        }

