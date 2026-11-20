"""Stable business errors exposed by the workbench API and CLI."""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    E_INVALID_CONFIG = "E_INVALID_CONFIG"
    E_MISSING_REFERENCE = "E_MISSING_REFERENCE"
    E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
    E_INVALID_STAGE = "E_INVALID_STAGE"
    E_INCONSISTENT_STATE = "E_INCONSISTENT_STATE"


@dataclass
class BusinessError(Exception):
    code: ErrorCode
    message: str
    shard: str | None = None
    field: str | None = None
    details: dict[str, Any] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": False,
            "code": self.code.value,
            "message": self.message,
        }
        if self.shard is not None:
            result["shard"] = self.shard
        if self.field is not None:
            result["field"] = self.field
        if self.details:
            result["details"] = self.details
        return result

