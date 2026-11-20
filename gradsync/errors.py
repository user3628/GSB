from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    OK = "OK"
    E_ZERO_WEIGHT = "E_ZERO_WEIGHT"
    E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
    E_ROUND_NOT_FOUND = "E_ROUND_NOT_FOUND"
    E_CONFLICTING_REPORT = "E_CONFLICTING_REPORT"
    E_DEGRADED_ROUND = "E_DEGRADED_ROUND"
    E_BUSY = "E_BUSY"
    E_INVALID_SUBMISSION = "E_INVALID_SUBMISSION"
    E_RESULT_NOT_FOUND = "E_RESULT_NOT_FOUND"
    E_VALIDATION_ERROR = "E_VALIDATION_ERROR"
    E_INTERNAL = "E_INTERNAL"


HTTP_STATUS_BY_CODE: dict[ErrorCode, int] = {
    ErrorCode.E_ZERO_WEIGHT: 422,
    ErrorCode.E_INVALID_TRANSITION: 409,
    ErrorCode.E_ROUND_NOT_FOUND: 404,
    ErrorCode.E_CONFLICTING_REPORT: 409,
    ErrorCode.E_DEGRADED_ROUND: 200,
    ErrorCode.E_BUSY: 409,
    ErrorCode.E_INVALID_SUBMISSION: 422,
    ErrorCode.E_RESULT_NOT_FOUND: 404,
    ErrorCode.E_VALIDATION_ERROR: 422,
    ErrorCode.E_INTERNAL: 500,
}


class GradSyncError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        round_id: str | None = None,
        details: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.round_id = round_id
        self.details = details or {}
        self.http_status = http_status or HTTP_STATUS_BY_CODE.get(code, 400)

    def payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code.value,
            "message": self.message,
            "round_id": self.round_id,
            "details": self.details,
        }

