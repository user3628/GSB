from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_INDEX_OOB = "E_INDEX_OOB"
E_INDEX_NONINTEGER = "E_INDEX_NONINTEGER"
E_EMPTY_BATCH = "E_EMPTY_BATCH"
E_DIM_MISMATCH = "E_DIM_MISMATCH"
E_PAD_OVERLAP = "E_PAD_OVERLAP"
E_NUMERIC = "E_NUMERIC"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_MANIFEST_INVALID = "E_MANIFEST_INVALID"
E_BATCH_INVALID = "E_BATCH_INVALID"
E_WEIGHT_SHAPE = "E_WEIGHT_SHAPE"
E_CONTENT_DIGEST = "E_CONTENT_DIGEST"
E_LINK_MISMATCH = "E_LINK_MISMATCH"
E_IO = "E_IO"


@dataclass
class BusinessError(RuntimeError):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


def fail(code: str, message: str, **context: Any) -> BusinessError:
    return BusinessError(code=code, message=message, context=context)
