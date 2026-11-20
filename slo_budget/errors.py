"""Business errors shared by the engine and HTTP layer."""

from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An expected error with a stable machine-readable code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


def invalid_request(message: str, **details: Any) -> BusinessError:
    return BusinessError(
        "E_INVALID_REQUEST", message, http_status=422, details=details
    )


def insufficient_data(sample_count: int, minimum: int, **details: Any) -> BusinessError:
    return BusinessError(
        "E_INSUFFICIENT_DATA",
        "The window contains fewer events than the configured minimum sample size.",
        http_status=422,
        details={
            "sample_count": sample_count,
            "minimum_sample_count": minimum,
            **details,
        },
    )


def late_event(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_LATE_EVENT", message, http_status=409, details=details)
