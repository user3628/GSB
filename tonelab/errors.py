"""Domain errors with stable codes and diagnostic context."""

from __future__ import annotations

from typing import Any


ERROR_CODES = {
    "E_FORMAT_MISMATCH",
    "E_MANIFEST_MISMATCH",
    "E_TIMELINE_INVALID",
    "E_NUMERIC",
    "E_INCREMENTAL_MISMATCH",
    "E_STALE_CHECKPOINT",
    "E_INVALID_STAGE",
    "E_VALIDATION",
    "E_NOT_FOUND",
    "E_BAD_REQUEST",
    "E_STOPPED",
}


class ToneLabError(Exception):
    """A deliberately serializable application error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        shard_id: str | None = None,
        unit_index: int | None = None,
        position: Any = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if code not in ERROR_CODES:
            raise ValueError(f"unknown error code: {code}")
        super().__init__(message)
        self.code = code
        self.message = message
        self.shard_id = shard_id
        self.unit_index = unit_index
        self.position = position
        self.context = context or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "shard_id": self.shard_id,
                "unit_index": self.unit_index,
                "position": self.position,
                "context": self.context,
            },
        }
        return payload


def result_error(
    code: str,
    message: str,
    *,
    shard_id: str | None = None,
    unit_index: int | None = None,
    position: Any = None,
    context: dict[str, Any] | None = None,
) -> ToneLabError:
    return ToneLabError(
        code,
        message,
        shard_id=shard_id,
        unit_index=unit_index,
        position=position,
        context=context,
    )

