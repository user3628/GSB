from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """A stable, machine-readable domain error.

    The HTTP layer serializes this object as ``{"code", "message", "context"}``.
    Domain methods raise the same error for direct callers.
    """

    _HTTP_STATUS = {
        "E_SPEC_INVALID": 422,
        "E_INVALID_TRANSITION": 409,
        "E_DEPENDENCY_CYCLE": 409,
        "E_STALE_BASE": 409,
        "E_CONFLICTING_TRIGGER": 409,
        "E_NOT_FOUND": 404,
        "E_PLAN_NOT_READY": 409,
        "E_INTERNAL": 500,
    }

    def __init__(
        self,
        code: str,
        message: str,
        context: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.http_status = http_status or self._HTTP_STATUS.get(code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "context": self.context}


def spec_invalid(message: str, **context: Any) -> BusinessError:
    return BusinessError("E_SPEC_INVALID", message, context)
