from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


HTTP_STATUS_BY_CODE: dict[str, int] = {
    "E_VALIDATION": 422,
    "E_POLICY_INVALID": 422,
    "E_STALE_POLICY": 409,
    "E_POLICY_NOT_FOUND": 404,
    "E_POLICY_WINDOW_INCOMPATIBLE": 409,
    "E_BATCH_INVALID": 422,
    "E_BATCH_EXISTS": 409,
    "E_BATCH_NOT_FOUND": 404,
    "E_OBSERVATION_INVALID": 422,
    "E_CONFLICTING_REPORT": 409,
    "E_LATE_SAMPLE": 409,
    "E_INSUFFICIENT_SAMPLES": 422,
    "E_WINDOW_NOT_FOUND": 404,
    "E_INVALID_TRANSITION": 409,
    "E_ROLLBACK_COOLDOWN": 409,
    "E_DECISION_INVALID": 422,
    "E_FAULT_INJECTION_DISABLED": 404,
    "E_NOT_FOUND": 404,
    "E_METHOD_NOT_ALLOWED": 405,
    "E_HTTP": 400,
    "E_INTERNAL": 500,
}


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    @property
    def http_status(self) -> int:
        return HTTP_STATUS_BY_CODE.get(self.code, 400)


def require(condition: bool, code: str, message: str, **context: Any) -> None:
    if not condition:
        raise BusinessError(code=code, message=message, context=context)
