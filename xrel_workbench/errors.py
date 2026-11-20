from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_EMPTY_TEXT = "E_EMPTY_TEXT"
E_NUMERIC = "E_NUMERIC"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_CONFIG = "E_CONFIG"
E_INPUT = "E_INPUT"


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def numeric_error(
    pair_id: str,
    term: str | None,
    stage: str,
    reason: str,
    value: Any = None,
) -> BusinessError:
    details: dict[str, Any] = {
        "pair_id": pair_id,
        "term": term,
        "stage": stage,
        "reason": reason,
    }
    if value is not None:
        details["value"] = str(value)
    return BusinessError(E_NUMERIC, "non-finite, negative, or zero-scale numeric value", details)
