"""Stable error codes and the exception type used by the public API."""

from __future__ import annotations

from typing import Any, Mapping


E_SPEC_INVALID = "E_SPEC_INVALID"
E_QUOTA_GROUP_UNKNOWN = "E_QUOTA_GROUP_UNKNOWN"
E_QUOTA_EXCEEDED = "E_QUOTA_EXCEEDED"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_BATCH_TOO_LARGE = "E_BATCH_TOO_LARGE"
E_BATCH_UNPLACEABLE = "E_BATCH_UNPLACEABLE"
E_EXEC_QUEUE_FULL = "E_EXEC_QUEUE_FULL"
E_QUOTA_INCONSISTENT = "E_QUOTA_INCONSISTENT"
E_REQUEST_NOT_FOUND = "E_REQUEST_NOT_FOUND"
E_BATCH_NOT_FOUND = "E_BATCH_NOT_FOUND"
E_ROUND_NOT_FOUND = "E_ROUND_NOT_FOUND"
E_MODEL_UNKNOWN = "E_MODEL_UNKNOWN"
E_NODE_UNKNOWN = "E_NODE_UNKNOWN"
E_NODE_UNHEALTHY = "E_NODE_UNHEALTHY"
E_STATE_CONFLICT = "E_STATE_CONFLICT"
E_VALIDATION = "E_VALIDATION"
E_DATABASE = "E_DATABASE"


class SchedulerError(Exception):
    """An expected, machine-readable adjudication error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        field: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.field is not None:
            payload["field"] = self.field
        if self.details:
            payload["details"] = self.details
        return payload


def spec_error(field: str, message: str, **details: Any) -> SchedulerError:
    return SchedulerError(
        E_SPEC_INVALID,
        message,
        field=field,
        details=details or None,
    )

