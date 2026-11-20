from __future__ import annotations

from fractions import Fraction
from typing import Any

from .canonical import digest_value
from .models import Shot, fraction_number, fraction_text


US_PER_SECOND = 1_000_000
MS_PER_SECOND = 1_000


def round_fraction_half_up(value: Fraction, scale: int) -> int:
    if value < 0:
        raise ValueError("timeline values must be non-negative")
    numerator = value.numerator * scale
    quotient, remainder = divmod(numerator, value.denominator)
    if remainder * 2 >= value.denominator:
        quotient += 1
    return quotient


def build_schedule(shots: list[Shot], config: dict[str, Any]) -> dict[str, Any]:
    shot_records: list[dict[str, Any]] = []
    frame_records: list[dict[str, Any]] = []
    elapsed = Fraction(0, 1)
    global_frame = 0
    for shot in shots:
        start_exact = elapsed
        end_exact = start_exact + Fraction(shot.duration_frames, 1) / shot.fps
        start_us = round_fraction_half_up(start_exact, US_PER_SECOND)
        end_us = round_fraction_half_up(end_exact, US_PER_SECOND)
        start_ms = round_fraction_half_up(start_exact, MS_PER_SECOND)
        end_ms = round_fraction_half_up(end_exact, MS_PER_SECOND)
        shot_frame_start = global_frame
        for local_frame in range(shot.duration_frames):
            frame_start = start_exact + Fraction(local_frame, 1) / shot.fps
            frame_end = frame_start + Fraction(1, 1) / shot.fps
            frame_start_ms = round_fraction_half_up(frame_start, MS_PER_SECOND)
            frame_end_ms = round_fraction_half_up(frame_end, MS_PER_SECOND)
            frame_records.append(
                {
                    "global_frame": global_frame,
                    "shot_id": shot.shot_id,
                    "source_index": shot.source_index,
                    "local_frame": local_frame,
                    "start_exact": fraction_text(frame_start),
                    "end_exact": fraction_text(frame_end),
                    "start_us": round_fraction_half_up(frame_start, US_PER_SECOND),
                    "end_us": round_fraction_half_up(frame_end, US_PER_SECOND),
                    "start_ms": frame_start_ms,
                    "end_ms": frame_end_ms,
                }
            )
            global_frame += 1
        shot_records.append(
            {
                "shot_id": shot.shot_id,
                "source_index": shot.source_index,
                "image": shot.image,
                "camera_move": shot.camera_move,
                "duration_frames": shot.duration_frames,
                "fps": fraction_number(shot.fps),
                "fps_exact": fraction_text(shot.fps),
                "global_frame_start": shot_frame_start,
                "global_frame_end": global_frame,
                "start_exact": fraction_text(start_exact),
                "end_exact": fraction_text(end_exact),
                "start_us": start_us,
                "end_us": end_us,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "transition": shot.transition,
            }
        )
        elapsed = end_exact

    body = {
        "stage": "schedule",
        "config_version": config["config_version"],
        "timebase": "microsecond",
        "ms_rounding": "half_up",
        "total_frames": len(frame_records),
        "total_duration_exact": fraction_text(elapsed),
        "total_duration_us": round_fraction_half_up(elapsed, US_PER_SECOND),
        "total_duration_ms": round_fraction_half_up(elapsed, MS_PER_SECOND),
        "shots": shot_records,
        "frames": frame_records,
    }
    body["timeline_digest"] = digest_value(body)
    return body


def shot_totals(schedule: dict[str, Any]) -> dict[str, int]:
    return {
        record["shot_id"]: record["duration_frames"]
        for record in schedule["shots"]
    }
