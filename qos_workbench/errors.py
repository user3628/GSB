from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_FORMAT_UNSUPPORTED = "E_FORMAT_UNSUPPORTED"
E_FORMAT_INVALID = "E_FORMAT_INVALID"
E_CRC_MISMATCH = "E_CRC_MISMATCH"
E_UNIT_PRECISION = "E_UNIT_PRECISION"
E_UNIT_OVERFLOW = "E_UNIT_OVERFLOW"
E_POLICY_INVALID = "E_POLICY_INVALID"
E_PARENT_NOT_FOUND = "E_PARENT_NOT_FOUND"
E_DEPENDENCY_CYCLE = "E_DEPENDENCY_CYCLE"
E_RATE_EXCEEDED = "E_RATE_EXCEEDED"
E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
E_SNAPSHOT_STALE = "E_SNAPSHOT_STALE"
E_SNAPSHOT_MISSING = "E_SNAPSHOT_MISSING"
E_SAMPLE_INVALID = "E_SAMPLE_INVALID"
E_REQUEST_INVALID = "E_REQUEST_INVALID"
E_INTERNAL = "E_INTERNAL"


@dataclass(slots=True)
class BusinessError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    http_status: int = 400

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def fail(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
    http_status: int = 400,
) -> None:
    raise BusinessError(code, message, details or {}, http_status)

