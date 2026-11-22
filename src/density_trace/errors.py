from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

E_EMPTY_BATCH = "E_EMPTY_BATCH"
E_REFERENCE_INVALID = "E_REFERENCE_INVALID"
E_NUMERIC = "E_NUMERIC"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_VERSION = "E_STALE_VERSION"
E_MANIFEST_INVALID = "E_MANIFEST_INVALID"
E_CONFIG_INVALID = "E_CONFIG_INVALID"
E_CHECKPOINT_CORRUPT = "E_CHECKPOINT_CORRUPT"
E_INPUT_CONTRACT = "E_INPUT_CONTRACT"


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, **self.details}


def business_error(code: str, message: str, **details: Any) -> BusinessError:
    return BusinessError(code=code, message=message, details=details)
