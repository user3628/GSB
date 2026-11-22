from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    OK = "E_OK"
    INJECTION_SPEC_INVALID = "E_INJECTION_SPEC_INVALID"
    INVALID_TRANSITION = "E_INVALID_TRANSITION"
    FAULT_CONNECTION_REFUSED = "E_FAULT_CONNECTION_REFUSED"
    FAULT_RESPONSE_TIMEOUT = "E_FAULT_RESPONSE_TIMEOUT"
    FAULT_CONTENT_TRUNCATED = "E_FAULT_CONTENT_TRUNCATED"
    FAULT_DUPLICATE_SUBMISSION = "E_FAULT_DUPLICATE_SUBMISSION"
    FAULT_ORDER_MISMATCH = "E_FAULT_ORDER_MISMATCH"
    BLOCK_VALIDATION_FAILED = "E_BLOCK_VALIDATION_FAILED"
    RETRY_EXHAUSTED = "E_RETRY_EXHAUSTED"
    DEGRADED_AGGREGATION = "E_DEGRADED_AGGREGATION"
    AGGREGATION_ABORTED = "E_AGGREGATION_ABORTED"
    RECOVERED_AFTER_RESTART = "E_RECOVERED_AFTER_RESTART"
    BATCH_NOT_READY = "E_BATCH_NOT_READY"
    BATCH_PAYLOAD_CONFLICT = "E_BATCH_PAYLOAD_CONFLICT"
    INJECTOR_UNAVAILABLE = "E_INJECTOR_UNAVAILABLE"
    NOT_FOUND = "E_NOT_FOUND"
    REQUEST_VALIDATION = "E_REQUEST_VALIDATION"
    INTERNAL = "E_INTERNAL"


HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.OK: 200,
    ErrorCode.INJECTION_SPEC_INVALID: 422,
    ErrorCode.INVALID_TRANSITION: 409,
    ErrorCode.FAULT_CONNECTION_REFUSED: 502,
    ErrorCode.FAULT_RESPONSE_TIMEOUT: 504,
    ErrorCode.FAULT_CONTENT_TRUNCATED: 502,
    ErrorCode.FAULT_DUPLICATE_SUBMISSION: 409,
    ErrorCode.FAULT_ORDER_MISMATCH: 409,
    ErrorCode.BLOCK_VALIDATION_FAILED: 422,
    ErrorCode.RETRY_EXHAUSTED: 503,
    ErrorCode.DEGRADED_AGGREGATION: 200,
    ErrorCode.AGGREGATION_ABORTED: 409,
    ErrorCode.RECOVERED_AFTER_RESTART: 409,
    ErrorCode.BATCH_NOT_READY: 409,
    ErrorCode.BATCH_PAYLOAD_CONFLICT: 409,
    ErrorCode.INJECTOR_UNAVAILABLE: 502,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.REQUEST_VALIDATION: 422,
    ErrorCode.INTERNAL: 500,
}


@dataclass(slots=True)
class AppError(Exception):
    code: ErrorCode
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    retryable: bool = False
    http_status: int | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)
        if self.http_status is None:
            self.http_status = HTTP_STATUS.get(self.code, 500)

    def to_envelope(self, *, batch_id: str | None = None) -> dict[str, Any]:
        details = dict(self.details)
        if batch_id is not None:
            details.setdefault("batch_id", batch_id)
        return {
            "ok": False,
            "error": {
                "code": self.code.value,
                "message": self.message,
                "retryable": self.retryable,
                "details": details,
            },
        }


def ok_envelope(data: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "data": data}

