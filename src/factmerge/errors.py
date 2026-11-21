"""Structured errors used across the component boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


EXIT_CODES = {
    "E_PARSE_AMBIGUOUS": 2,
    "E_EVIDENCE_BROKEN": 3,
    "E_INVALID_FORMAT": 4,
    "E_NOT_FOUND": 5,
    "E_STOP_REQUESTED": 0,
}


@dataclass
class FactMergeError(Exception):
    code: str
    message: str
    claim_id: str | None = None
    entity_id: str | None = None
    offsets: dict[str, Any] | None = None
    breakpoint: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def as_dict(self) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.claim_id is not None:
            error["claim_id"] = self.claim_id
        if self.entity_id is not None:
            error["entity_id"] = self.entity_id
        if self.offsets is not None:
            error["offsets"] = self.offsets
        if self.breakpoint is not None:
            error["breakpoint"] = self.breakpoint
        if self.details:
            error["details"] = self.details
        return error

    @property
    def exit_code(self) -> int:
        return EXIT_CODES.get(self.code, 1)
