from __future__ import annotations

from typing import Any

from .canonical import digest_file, digest_value
from .config import config_digest
from .models import Shot, fraction_text
from .workspace import InputBundle


STAGE_VERSIONS = {
    "schedule": "schedule-v1",
    "compose": "compose-v1",
    "transition": "transition-v1",
    "encode": "encode-v1",
}


def stage_fingerprint(stage: str, components: dict[str, Any]) -> str:
    return digest_value(
        {
            "stage": stage,
            "stage_version": STAGE_VERSIONS[stage],
            "components": components,
        }
    )


def build_fingerprints(
    bundle: InputBundle,
    schedule: dict[str, Any],
    cache_plan: dict[str, Any],
) -> dict[str, Any]:
    schedule_components = {
        "config.version": bundle.config["config_version"],
        "schedule.config": digest_value(bundle.config["schedule"]),
        "schedule.shots": digest_value(
            [
                {
                    "shot_id": shot.shot_id,
                    "source_index": shot.source_index,
                    "fps": fraction_text(shot.fps),
                    "duration_frames": shot.duration_frames,
                }
                for shot in bundle.shots
            ]
        ),
    }
    schedule_fp = stage_fingerprint("schedule", schedule_components)

    image_digests = {
        shot.shot_id: digest_file(bundle.image_dir / shot.image)
        for shot in bundle.shots
        if (bundle.image_dir / shot.image).exists()
    }
    missing_images = [
        shot.shot_id for shot in bundle.shots if shot.shot_id not in image_digests
    ]
    compose_components = {
        "compose.upstream.schedule": schedule_fp,
        "compose.images": digest_value(image_digests),
        "compose.missing_images": digest_value(missing_images),
        "compose.prompts": digest_value(
            {shot.shot_id: shot.prompt for shot in bundle.shots}
        ),
        "compose.camera_moves": digest_value(
            {shot.shot_id: shot.camera_move for shot in bundle.shots}
        ),
        "compose.motion_config": digest_value(bundle.config["motion"]),
        "compose.cache_config": digest_value(bundle.config["cache"]),
        "compose.cache_selection": cache_plan.get("selection_digest", ""),
    }
    compose_fp = stage_fingerprint("compose", compose_components)

    transition_components = {
        "transition.upstream.compose": compose_fp,
        "transition.script": digest_value(
            {shot.shot_id: shot.transition for shot in bundle.shots}
        ),
        "transition.config": digest_value(bundle.config["transition"]),
    }
    transition_fp = stage_fingerprint("transition", transition_components)

    encode_components = {
        "encode.upstream.transition": transition_fp,
        "encode.config": digest_value(bundle.config["encode"]),
    }
    encode_fp = stage_fingerprint("encode", encode_components)

    fingerprints = {
        "schedule": schedule_fp,
        "compose": compose_fp,
        "transition": transition_fp,
        "encode": encode_fp,
    }
    components = {
        "schedule": schedule_components,
        "compose": compose_components,
        "transition": transition_components,
        "encode": encode_components,
    }
    return {
        "overall_fingerprint": digest_value(fingerprints),
        "stages": fingerprints,
        "components": components,
        "config_digest": config_digest(bundle.config),
        "timeline_digest": schedule["timeline_digest"],
        "cache_selection_digest": cache_plan.get("selection_digest"),
    }


def changed_components(
    previous: dict[str, Any] | None, current: dict[str, Any]
) -> list[str]:
    if not previous:
        return sorted(current)
    changed = []
    for key in sorted(set(previous) | set(current)):
        if previous.get(key) != current.get(key):
            changed.append(key)
    return changed
