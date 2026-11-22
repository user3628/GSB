from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


HTTP_STATUS = {
    "E_INVALID_LAYOUT": 400,
    "E_DIM_MISMATCH": 400,
    "E_NUMERIC": 400,
    "E_EMPTY_INPUT": 422,
    "E_UNKNOWN_REF": 422,
    "E_SHARD_FULL": 409,
    "E_DUPLICATE_ROUTE": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_STOPPED": 409,
    "E_INVALID_INPUT": 400,
}


@dataclass
class EngineError(Exception):
    code: str
    message: str
    stage: str
    batch_seq: int | None = None
    batch_id: str | None = None
    entries: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "stage": self.stage,
            "batch_seq": self.batch_seq,
            "batch_id": self.batch_id,
            "entries": self.entries,
            "conflicts": self.conflicts,
            "details": self.details,
        }


def entry_ref(text_id: str | None = None, image_id: str | None = None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if text_id is not None:
        out["text_id"] = text_id
    if image_id is not None:
        out["image_id"] = image_id
    out.update(extra)
    return out
