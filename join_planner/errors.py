from __future__ import annotations

from typing import Any


class PlannerError(Exception):
    """A failure that is safe to expose through the local HTTP API."""

    status_by_code = {
        "E_SNAPSHOT_INVALID": 409,
        "E_SEARCH_SPACE_TOO_LARGE": 422,
        "E_VERSION_RETIRED": 409,
        "E_INVALID_QUERY": 400,
        "E_PLAN_NOT_FOUND": 404,
        "E_RECOVERY_REPORT_NOT_FOUND": 404,
        "E_INTERNAL": 500,
    }

    def __init__(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.status_code = self.status_by_code.get(code, 400)

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def invalid_query(message: str, **details: Any) -> PlannerError:
    return PlannerError("E_INVALID_QUERY", message, details)


def snapshot_invalid(message: str, **details: Any) -> PlannerError:
    return PlannerError("E_SNAPSHOT_INVALID", message, details)


def version_retired(snapshot_version: int, current_version: int | None) -> PlannerError:
    return PlannerError(
        "E_VERSION_RETIRED",
        "The requested snapshot version is not active.",
        {
            "requested_version": snapshot_version,
            "current_version": current_version,
            "rule": "A plan may use an immutable snapshot captured at submission, but new requests must bind to the active version.",
        },
    )
