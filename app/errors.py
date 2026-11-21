from __future__ import annotations

from typing import Any


class DomainError(Exception):
    """A deterministic business error returned by the HTTP API."""

    def __init__(
        self,
        code: str,
        message: str,
        context: dict[str, Any] | None = None,
        http_status: int = 409,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.http_status = http_status

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "context": self.context}


def spec_invalid(message: str, **context: Any) -> DomainError:
    return DomainError("E_SPEC_INVALID", message, context, 422)


def unknown_tenant(tenant_id: str, **context: Any) -> DomainError:
    return DomainError(
        "E_UNKNOWN_TENANT",
        f"tenant {tenant_id!r} is not present in the current policy",
        {"tenant_id": tenant_id, **context},
        404,
    )
