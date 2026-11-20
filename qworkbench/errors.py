from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class StopRequested(Exception):
    """Internal control-flow signal; not a business failure."""


def numeric_error(sample_id: str, factor: str, value: Any, reason: str) -> BusinessError:
    return BusinessError(
        "E_NUMERIC",
        "non-finite or invalid quality factor",
        {"sample_id": sample_id, "factor": factor, "value": str(value), "reason": reason},
    )
