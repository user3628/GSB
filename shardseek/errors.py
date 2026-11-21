"""Stable business errors used by every ShardSeek interface."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    stage: str = "unknown"
    item_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    http_status: int = 400

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "stage": self.stage,
                "item_id": self.item_id,
                "details": self.details,
            },
        }


def error(
    code: str,
    message: str,
    *,
    stage: str,
    item_id: str | None = None,
    details: dict[str, Any] | None = None,
    http_status: int = 400,
) -> BusinessError:
    return BusinessError(
        code=code,
        message=message,
        stage=stage,
        item_id=item_id,
        details=details or {},
        http_status=http_status,
    )


E_EMPTY_IMAGE = "E_EMPTY_IMAGE"
E_GRID_OUT_OF_BOUNDS = "E_GRID_OUT_OF_BOUNDS"
E_EMPTY_CAPTION = "E_EMPTY_CAPTION"
E_DIM_MISMATCH = "E_DIM_MISMATCH"
E_NUMERIC = "E_NUMERIC"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_INVALID_MANIFEST = "E_INVALID_MANIFEST"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_SNAPSHOT_REGRESSION = "E_SNAPSHOT_REGRESSION"
E_SNAPSHOT_MISMATCH = "E_SNAPSHOT_MISMATCH"
E_INDEX_NOT_READY = "E_INDEX_NOT_READY"
E_QUERY_EMPTY = "E_QUERY_EMPTY"
E_INVALID_K = "E_INVALID_K"
E_INVALID_DIRECTION = "E_INVALID_DIRECTION"
E_BUILD_STOPPED = "E_BUILD_STOPPED"

