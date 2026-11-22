"""Typed application errors shared by the pipeline, CLI, and HTTP API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AppError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                **self.details,
            },
        }


def invalid_config(
    message: str,
    *,
    slice_id: str | None = None,
    param: str | None = None,
    **extra: Any,
) -> AppError:
    details: dict[str, Any] = dict(extra)
    if slice_id is not None:
        details["slice_id"] = slice_id
    if param is not None:
        details["param"] = param
    return AppError("E_INVALID_CONFIG", message, details)


def numeric_error(
    message: str,
    *,
    slice_id: str | None = None,
    band_index: int | None = None,
    frame_index: int | None = None,
    **extra: Any,
) -> AppError:
    details: dict[str, Any] = dict(extra)
    if slice_id is not None:
        details["slice_id"] = slice_id
    if band_index is not None:
        details["band_index"] = int(band_index)
    if frame_index is not None:
        details["frame_index"] = int(frame_index)
    return AppError("E_NUMERIC", message, details)


def manifest_error(message: str, *, slice_id: str | None = None, **extra: Any) -> AppError:
    details: dict[str, Any] = dict(extra)
    if slice_id is not None:
        details["slice_id"] = slice_id
    return AppError("E_MANIFEST", message, details)


def not_found(message: str, **details: Any) -> AppError:
    return AppError("E_NOT_FOUND", message, details)


def conflict(message: str, **details: Any) -> AppError:
    return AppError("E_CONFLICT", message, details)


def incremental_mismatch(
    message: str,
    *,
    slice_id: str,
    band_index: int,
    block_index: int,
    stage: str,
) -> AppError:
    return AppError(
        "E_INCREMENTAL_MISMATCH",
        message,
        {
            "slice_id": slice_id,
            "band_index": int(band_index),
            "block_index": int(block_index),
            "stage": stage,
        },
    )


def stale_checkpoint(message: str, **details: Any) -> AppError:
    return AppError("E_STALE_CHECKPOINT", message, details)

