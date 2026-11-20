"""Structured engine errors used by the CLI and HTTP API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TTSError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            },
        }


def invalid_text(
    *,
    utt_id: str,
    chunk_index: int,
    char_position: int,
    message: str,
    **extra: Any,
) -> TTSError:
    return TTSError(
        "E_INVALID_TEXT",
        message,
        {
            "utt_id": utt_id,
            "chunk_index": chunk_index,
            "char_position": char_position,
            **extra,
        },
    )


def numeric_error(
    *,
    utt_id: str,
    frame_index: int,
    parameter: str,
    value: float,
    **extra: Any,
) -> TTSError:
    return TTSError(
        "E_NUMERIC",
        f"non-finite acoustic parameter {parameter!r} at frame {frame_index}",
        {
            "utt_id": utt_id,
            "frame_index": frame_index,
            "parameter_name": parameter,
            "value": str(value),
            **extra,
        },
    )


def stream_mismatch(
    *,
    utt_id: str,
    first_deviating_block: int,
    frame_index: int,
    sample_index: int,
    chunked_summary: dict[str, Any],
    full_summary: dict[str, Any],
    detail: str,
    **extra: Any,
) -> TTSError:
    return TTSError(
        "E_STREAM_MISMATCH",
        detail,
        {
            "utt_id": utt_id,
            "first_deviating_block": first_deviating_block,
            "frame_index": frame_index,
            "sample_index": sample_index,
            "chunked_summary": chunked_summary,
            "full_summary": full_summary,
            **extra,
        },
    )


def stale_checkpoint(message: str, **context: Any) -> TTSError:
    return TTSError("E_STALE_CHECKPOINT", message, context)


def invalid_stage(message: str, **context: Any) -> TTSError:
    return TTSError("E_INVALID_STAGE", message, context)
