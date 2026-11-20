from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from .errors import PipelineError


CAMERA_ALIASES = {
    "static": "static",
    "still": "static",
    "静止": "static",
    "pan": "pan",
    "平移": "pan",
    "zoom": "zoom",
    "缩放": "zoom",
}

TRANSITION_ALIASES = {
    "hard_cut": "hard_cut",
    "cut": "hard_cut",
    "硬切": "hard_cut",
    "crossfade": "crossfade",
    "dissolve": "crossfade",
    "交叉淡化": "crossfade",
}


@dataclass(frozen=True)
class Shot:
    shot_id: str
    image: str
    prompt: str
    camera_move: str
    duration_frames: int
    fps: Fraction
    transition: str
    source_index: int


def _as_fraction(value: Any, *, shot_id: str) -> Fraction:
    try:
        if isinstance(value, bool):
            raise ValueError
        if isinstance(value, int):
            parsed = Fraction(value, 1)
        elif isinstance(value, float):
            parsed = Fraction(str(value))
        elif isinstance(value, str):
            parsed = Fraction(value)
        else:
            raise ValueError
    except (ValueError, ZeroDivisionError) as exc:
        raise PipelineError(
            "E_INVALID_TIMELINE",
            f"invalid fps {value!r}",
            stage="schedule",
            shot_id=shot_id,
        ) from exc
    return parsed


def parse_shot(raw: dict[str, Any], source_index: int) -> Shot:
    shot_id = raw.get("shot_id", raw.get("id"))
    if not isinstance(shot_id, str) or not shot_id.strip():
        raise PipelineError(
            "E_INVALID_SHOT",
            "shot_id must be a non-empty string",
            stage="schedule",
            details={"source_index": source_index},
        )
    shot_id = shot_id.strip()
    image = raw.get("image", raw.get("image_file", raw.get("image_filename")))
    if not isinstance(image, str) or not image:
        raise PipelineError(
            "E_INVALID_SHOT",
            "image must be a non-empty filename",
            stage="schedule",
            shot_id=shot_id,
        )
    prompt = raw.get("prompt", raw.get("text", raw.get("提示词", "")))
    if not isinstance(prompt, str):
        raise PipelineError(
            "E_INVALID_SHOT",
            "prompt must be a string",
            stage="schedule",
            shot_id=shot_id,
        )
    move_raw = raw.get(
        "camera_move",
        raw.get(
            "camera_type",
            raw.get("camera", raw.get("motion", raw.get("运镜类型"))),
        ),
    )
    move_key = str(move_raw).strip().lower() if move_raw is not None else ""
    camera_move = CAMERA_ALIASES.get(move_key) or CAMERA_ALIASES.get(str(move_raw).strip())
    if camera_move is None:
        raise PipelineError(
            "E_INVALID_SHOT",
            f"unsupported camera_move {move_raw!r}",
            stage="compose",
            shot_id=shot_id,
        )
    transition_raw = raw.get(
        "transition", raw.get("transition_type", raw.get("转场类型", "hard_cut"))
    )
    transition_key = str(transition_raw).strip().lower()
    transition = TRANSITION_ALIASES.get(transition_key) or TRANSITION_ALIASES.get(
        str(transition_raw).strip()
    )
    if transition is None:
        raise PipelineError(
            "E_INVALID_SHOT",
            f"unsupported transition {transition_raw!r}",
            stage="schedule",
            shot_id=shot_id,
        )
    duration = raw.get("duration_frames", raw.get("duration", raw.get("frames")))
    if isinstance(duration, bool) or not isinstance(duration, int):
        raise PipelineError(
            "E_INVALID_TIMELINE",
            "duration_frames must be an integer",
            stage="schedule",
            shot_id=shot_id,
        )
    if duration <= 0:
        raise PipelineError(
            "E_INVALID_TIMELINE",
            "duration_frames must be greater than zero",
            stage="schedule",
            shot_id=shot_id,
        )
    fps = _as_fraction(raw.get("fps", raw.get("frame_rate", raw.get("帧率"))), shot_id=shot_id)
    if fps <= 0:
        raise PipelineError(
            "E_INVALID_TIMELINE",
            "fps must be greater than zero",
            stage="schedule",
            shot_id=shot_id,
        )
    return Shot(
        shot_id=shot_id,
        image=image,
        prompt=prompt,
        camera_move=camera_move,
        duration_frames=duration,
        fps=fps,
        transition=transition,
        source_index=source_index,
    )


def fraction_text(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def fraction_number(value: Fraction) -> int | float:
    if value.denominator == 1:
        return value.numerator
    return float(value)
