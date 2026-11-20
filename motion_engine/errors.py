"""Stable business error codes exposed by both CLI and HTTP."""

from __future__ import annotations

from typing import Any


class EngineError(Exception):
    """An expected, user-facing engine failure."""

    def __init__(self, code: str, message: str, **context: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        payload.update(self.context)
        return payload


def invalid_state(
    message: str,
    *,
    series_id: str | None = None,
    position: str | int | None = None,
    **context: Any,
) -> EngineError:
    return EngineError(
        "E_INVALID_STATE",
        message,
        series_id=series_id,
        position=position,
        **context,
    )


def timeline_overflow(
    series_id: str,
    *,
    overflow_frames: int,
    last_required_frame: int,
    frame_count: int,
) -> EngineError:
    return EngineError(
        "E_TIMELINE_OVERFLOW",
        f"timeline for series {series_id!r} exceeds available truth frames",
        series_id=series_id,
        overflow_frames=overflow_frames,
        last_required_frame=last_required_frame,
        frame_count=frame_count,
    )


def stale_checkpoint(
    message: str,
    *,
    series_id: str | None = None,
    step_index: int | None = None,
    expected_step: int | None = None,
) -> EngineError:
    return EngineError(
        "E_STALE_CHECKPOINT",
        message,
        series_id=series_id,
        step_index=step_index,
        expected_step=expected_step,
    )
