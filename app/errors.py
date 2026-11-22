"""Business errors and the single JSON error envelope used by the API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ErrorCode:
    E_SPEC_INVALID = "E_SPEC_INVALID"
    E_QUOTA_EXCEEDED = "E_QUOTA_EXCEEDED"
    E_PARENT_QUOTA_EXCEEDED = "E_PARENT_QUOTA_EXCEEDED"
    E_RATE_LIMITED = "E_RATE_LIMITED"
    E_STALE_BASE = "E_STALE_BASE"
    E_VERSION_CONFLICT = "E_VERSION_CONFLICT"
    E_MIGRATION_REVIEW = "E_MIGRATION_REVIEW"
    E_MIGRATION_REJECTED = "E_MIGRATION_REJECTED"
    E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
    E_CONFLICTING_REQUEST = "E_CONFLICTING_REQUEST"
    E_TIMESTAMP_REGRESSION = "E_TIMESTAMP_REGRESSION"
    E_TENANT_NOT_FOUND = "E_TENANT_NOT_FOUND"
    E_TENANT_INACTIVE = "E_TENANT_INACTIVE"
    E_TENANT_SUSPENDED = "E_TENANT_SUSPENDED"
    E_TENANT_MIGRATING = "E_TENANT_MIGRATING"
    E_NO_SNAPSHOT = "E_NO_SNAPSHOT"
    E_REVISION_NOT_FOUND = "E_REVISION_NOT_FOUND"
    E_REQUEST_INVALID = "E_REQUEST_INVALID"
    E_NOT_FOUND = "E_NOT_FOUND"
    E_INTERNAL = "E_INTERNAL"


_HTTP_STATUS_DEFAULTS = {
    ErrorCode.E_SPEC_INVALID: 400,
    ErrorCode.E_REQUEST_INVALID: 400,
    ErrorCode.E_QUOTA_EXCEEDED: 409,
    ErrorCode.E_PARENT_QUOTA_EXCEEDED: 409,
    ErrorCode.E_RATE_LIMITED: 429,
    ErrorCode.E_STALE_BASE: 409,
    ErrorCode.E_VERSION_CONFLICT: 409,
    ErrorCode.E_MIGRATION_REVIEW: 409,
    ErrorCode.E_MIGRATION_REJECTED: 422,
    ErrorCode.E_INVALID_TRANSITION: 409,
    ErrorCode.E_CONFLICTING_REQUEST: 409,
    ErrorCode.E_TIMESTAMP_REGRESSION: 409,
    ErrorCode.E_TENANT_NOT_FOUND: 404,
    ErrorCode.E_TENANT_INACTIVE: 409,
    ErrorCode.E_TENANT_SUSPENDED: 423,
    ErrorCode.E_TENANT_MIGRATING: 423,
    ErrorCode.E_NO_SNAPSHOT: 409,
    ErrorCode.E_REVISION_NOT_FOUND: 404,
    ErrorCode.E_NOT_FOUND: 404,
    ErrorCode.E_INTERNAL: 500,
}


@dataclass
class AppError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)
        if self.http_status is None:
            self.http_status = _HTTP_STATUS_DEFAULTS.get(self.code, 400)

    def as_error(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }

    def as_response(self) -> dict[str, Any]:
        return {"error": self.as_error()}


def error_response(
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    http_status: int | None = None,
) -> dict[str, Any]:
    return AppError(code, message, details or {}, http_status).as_response()
