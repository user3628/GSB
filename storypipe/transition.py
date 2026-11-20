from __future__ import annotations

from typing import Any

from .canonical import digest_value
from .errors import PipelineError
from .models import Shot


def build_transition_plan(
    shots: list[Shot], schedule: dict[str, Any], config: dict[str, Any]
) -> dict[str, Any]:
    shot_schedule = {record["shot_id"]: record for record in schedule["shots"]}
    frames: list[dict[str, Any]] = []
    for frame in schedule["frames"]:
        frames.append(
            {
                "global_frame": frame["global_frame"],
                "source_shot": frame["shot_id"],
                "source_index": frame["source_index"],
                "local_frame": frame["local_frame"],
                "start_ms": frame["start_ms"],
                "end_ms": frame["end_ms"],
                "blend_sources": [
                    {
                        "shot_id": frame["shot_id"],
                        "local_frame": frame["local_frame"],
                        "weight": 1.0,
                    }
                ],
                "transition": None,
            }
        )
    boundaries: list[dict[str, Any]] = []
    crossfade_window = int(config["transition"]["crossfade"]["window_frames"])
    for index, shot in enumerate(shots[:-1]):
        following = shots[index + 1]
        boundary_frame = int(shot_schedule[following.shot_id]["global_frame_start"])
        transition_type = shot.transition
        boundary = {
            "transition_id": f"{shot.shot_id}->{following.shot_id}",
            "from_shot": shot.shot_id,
            "to_shot": following.shot_id,
            "boundary_global_frame": boundary_frame,
            "type": transition_type,
            "window_frames": 0,
        }
        if transition_type == "crossfade":
            window = crossfade_window
            if window > shot.duration_frames or window > following.duration_frames:
                raise PipelineError(
                    "E_TRANSITION_OVERFLOW",
                    "crossfade window exceeds an adjacent shot frame count",
                    stage="transition",
                    shot_id=shot.shot_id,
                    details={
                        "next_shot_id": following.shot_id,
                        "window_frames": window,
                        "from_shot_frames": shot.duration_frames,
                        "to_shot_frames": following.duration_frames,
                    },
                )
            boundary["window_frames"] = window
            outgoing_start = shot_schedule[shot.shot_id]["global_frame_start"]
            incoming_start = shot_schedule[following.shot_id]["global_frame_start"]
            for offset in range(window):
                outgoing_local = shot.duration_frames - window + offset
                incoming_local = offset
                outgoing_alpha = float(offset + 1) / float(2 * window)
                incoming_alpha = float(window + offset + 1) / float(2 * window)
                outgoing_index = outgoing_start + outgoing_local
                incoming_index = incoming_start + incoming_local
                pair = [
                    {
                        "shot_id": shot.shot_id,
                        "local_frame": outgoing_local,
                        "weight": 1.0 - outgoing_alpha,
                    },
                    {
                        "shot_id": following.shot_id,
                        "local_frame": incoming_local,
                        "weight": outgoing_alpha,
                    },
                ]
                frames[outgoing_index]["blend_sources"] = [dict(item) for item in pair]
                frames[outgoing_index]["transition"] = {
                    "type": "crossfade",
                    "transition_id": boundary["transition_id"],
                    "from_shot": shot.shot_id,
                    "to_shot": following.shot_id,
                    "boundary_global_frame": boundary_frame,
                    "window_frames": window,
                    "role": "outgoing",
                    "alpha": outgoing_alpha,
                }
                incoming_pair = [
                    {
                        "shot_id": shot.shot_id,
                        "local_frame": outgoing_local,
                        "weight": 1.0 - incoming_alpha,
                    },
                    {
                        "shot_id": following.shot_id,
                        "local_frame": incoming_local,
                        "weight": incoming_alpha,
                    },
                ]
                frames[incoming_index]["blend_sources"] = incoming_pair
                frames[incoming_index]["transition"] = {
                    "type": "crossfade",
                    "transition_id": boundary["transition_id"],
                    "from_shot": shot.shot_id,
                    "to_shot": following.shot_id,
                    "boundary_global_frame": boundary_frame,
                    "window_frames": window,
                    "role": "incoming",
                    "alpha": incoming_alpha,
                }
        elif transition_type == "hard_cut":
            frames[boundary_frame]["transition"] = {
                "type": "hard_cut",
                "transition_id": boundary["transition_id"],
                "from_shot": shot.shot_id,
                "to_shot": following.shot_id,
                "boundary_global_frame": boundary_frame,
                "window_frames": 0,
                "role": "cut",
                "alpha": 1.0,
            }
        boundaries.append(boundary)

    plan = {
        "format": "transition-plan-v1",
        "total_frames": len(frames),
        "boundaries": boundaries,
        "frames": frames,
        "crossfade_window_frames": crossfade_window,
    }
    plan["transition_plan_digest"] = digest_value(plan)
    return plan


def frame_blocks(plan: dict[str, Any], schedule: dict[str, Any]) -> list[dict[str, Any]]:
    blocks = []
    for shot in schedule["shots"]:
        start = shot["global_frame_start"]
        end = shot["global_frame_end"]
        records = plan["frames"][start:end]
        blocks.append(
            {
                "block_index": len(blocks),
                "shot_id": shot["shot_id"],
                "global_frame_start": start,
                "global_frame_end": end,
                "frames": records,
            }
        )
    return blocks
