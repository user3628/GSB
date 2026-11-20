from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    EMPTY_INPUT = "E_EMPTY_INPUT"
    STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
    INVALID_STAGE = "E_INVALID_STAGE"
    NUMERIC = "E_NUMERIC"
    INVALID_INPUT = "E_INVALID_INPUT"
    NOT_FOUND = "E_NOT_FOUND"
    BUSY = "E_BUSY"
    INTERNAL = "E_INTERNAL"


HTTP_STATUS = {
    ErrorCode.EMPTY_INPUT: 400,
    ErrorCode.STALE_CHECKPOINT: 409,
    ErrorCode.INVALID_STAGE: 409,
    ErrorCode.NUMERIC: 422,
    ErrorCode.INVALID_INPUT: 400,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.BUSY: 409,
    ErrorCode.INTERNAL: 500,
}


@dataclass(eq=False)
class BusinessError(Exception):
    code: ErrorCode
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 500)

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code.value,
            "message": self.message,
            "details": self.details,
        }

    def __str__(self) -> str:
        return f"{self.code.value}: {self.message}"
