from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


BUSINESS_CODES = {
    "E_CLAUSE_EMPTY",
    "E_INVALID_CLAUSE_NO",
    "E_TEXT_TOO_LONG",
    "E_UNKNOWN_ELEMENT",
    "E_INVALID_DATE",
    "E_NUMERIC",
    "E_CHECKPOINT_MISMATCH",
    "E_INTEROP",
}


@dataclass
class BusinessError(Exception):
    code: str
    contract_id: str
    stage: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.code not in BUSINESS_CODES:
            raise ValueError(f"unknown business code: {self.code}")
        Exception.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "code": self.code,
            "contract_id": self.contract_id,
            "stage": self.stage,
            "message": self.message,
        }
        if self.details:
            value["details"] = self.details
        return value


def error_dict(
    code: str,
    contract_id: str,
    stage: str,
    message: str,
    **details: Any,
) -> dict[str, Any]:
    return BusinessError(code, contract_id, stage, message, details).to_dict()

