"""Stable business errors exposed by vadkit."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_EXIT_CODES = {
    "E_INVALID_CONFIG": 2,
    "E_INPUT_LENGTH": 3,
    "E_SILENT_INPUT": 4,
    "E_FEATURE_MISMATCH": 5,
    "E_STALE_CHECKPOINT": 6,
    "E_INVALID_STAGE": 7,
}


@dataclass
class VadError(Exception):
    code: str
    message: str
    chunk_id: str | None = None
    sample_position: int | None = None
    frame_index: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.code not in ERROR_EXIT_CODES:
            raise ValueError(f"unknown business error code: {self.code}")
        super().__init__(self.message)

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.chunk_id is not None:
            result["chunk_id"] = self.chunk_id
        if self.sample_position is not None:
            result["sample_position"] = self.sample_position
        if self.frame_index is not None:
            result["frame_index"] = self.frame_index
        if self.details:
            result["details"] = self.details
        return result


def invalid_config(message: str, **details: Any) -> VadError:
    return VadError("E_INVALID_CONFIG", message, details=details)


def input_length(
    message: str,
    *,
    chunk_id: str | None = None,
    sample_position: int | None = None,
    **details: Any,
) -> VadError:
    return VadError(
        "E_INPUT_LENGTH",
        message,
        chunk_id=chunk_id,
        sample_position=sample_position,
        details=details,
    )

