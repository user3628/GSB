"""Explicit previous-state dialogue state evolution."""

from __future__ import annotations

import math
from typing import Any

from .behavior import validate_behavior_artifact
from .config import EMOTIONS, StateConfig
from .emotion import validate_emotion_artifact
from .errors import E_INVALID_STAGE, E_NUMERIC, as_error
from .manifest import ShardData, TurnAnnotation
from .serialization import digest_json, round_floats


def initial_state() -> dict[str, Any]:
    return {
        "step": -1,
        "turn_count": 0,
        "dominant_emotion": "平静",
        "cumulative_emotions": {label: 0.0 for label in EMOTIONS},
        "emotion_trend": {label: 0.0 for label in EMOTIONS},
        "trend_label": "平稳",
        "interruption_count": 0,
        "reference_interruption_count": 0,
        "speaker_change_count": 0,
        "last_speaker": None,
    }


def _normalized(values: dict[str, float]) -> dict[str, float]:
    total = sum(max(0.0, values[label]) for label in EMOTIONS)
    if total <= 0.0:
        return {label: 1.0 / len(EMOTIONS) for label in EMOTIONS}
    return {label: max(0.0, values[label]) / total for label in EMOTIONS}


def evolve_state(
    shard: ShardData,
    turn: TurnAnnotation,
    emotion_artifact: dict[str, Any],
    behavior_artifact: dict[str, Any],
    *,
    previous_state_artifact: dict[str, Any] | None,
    config: StateConfig,
) -> dict[str, Any]:
    """Advance state using only explicit prior state and current stage results."""

    config.validate()
    emotion_scores = validate_emotion_artifact(
        emotion_artifact, shard_id=shard.shard_id, turn_index=turn.index
    )
    behavior_label = validate_behavior_artifact(
        behavior_artifact, shard_id=shard.shard_id, turn_index=turn.index
    )
    if behavior_artifact.get("emotion_digest") != digest_json(emotion_artifact):
        raise as_error(
            E_INVALID_STAGE,
            "behavior artifact does not reference the supplied emotion result",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="state",
        )

    if turn.index == 0:
        if previous_state_artifact is not None:
            previous_state = previous_state_artifact.get("state")
            if not isinstance(previous_state, dict) or previous_state.get("step") != -1:
                raise as_error(
                    E_INVALID_STAGE,
                    "turn 0 only accepts the explicit initial state",
                    shard_id=shard.shard_id,
                    turn_index=turn.index,
                    stage="state",
                )
        state = initial_state()
        previous_state_digest = "initial"
    else:
        if previous_state_artifact is None:
            raise as_error(
                E_INVALID_STAGE,
                "previous state artifact is required",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="state",
            )
        state = previous_state_artifact.get("state")
        if not isinstance(state, dict) or state.get("step") != turn.index - 1:
            raise as_error(
                E_INVALID_STAGE,
                "previous state step does not immediately precede this turn",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="state",
            )
        previous_state_digest = digest_json(previous_state_artifact)

    previous_cumulative = state.get("cumulative_emotions")
    if not isinstance(previous_cumulative, dict) or set(previous_cumulative) != set(
        EMOTIONS
    ):
        raise as_error(
            E_INVALID_STAGE,
            "previous state has malformed cumulative emotions",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="state",
        )
    for label in EMOTIONS:
        value = previous_cumulative[label]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise as_error(
                E_NUMERIC,
                f"previous cumulative emotion {label} is invalid",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="state",
            )

    normalized_previous = _normalized(
        {label: float(previous_cumulative[label]) for label in EMOTIONS}
    )
    cumulative = {
        label: config.decay * float(previous_cumulative[label]) + emotion_scores[label]
        for label in EMOTIONS
    }
    if not all(math.isfinite(value) for value in cumulative.values()):
        raise as_error(
            E_NUMERIC,
            "cumulative emotion values contain NaN or Inf",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="state",
        )
    normalized_current = _normalized(cumulative)
    trend = {
        label: (normalized_current[label] - normalized_previous[label]) * config.trend_scale
        for label in EMOTIONS
    }
    if not all(math.isfinite(value) for value in trend.values()):
        raise as_error(
            E_NUMERIC,
            "state emotion trend contains NaN or Inf",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="state",
        )
    dominant = max(EMOTIONS, key=lambda label: (cumulative[label], -EMOTIONS.index(label)))
    trend_label = max(EMOTIONS, key=lambda label: (trend[label], -EMOTIONS.index(label)))
    if abs(max(trend.values())) < 0.05:
        trend_label = "平稳"

    last_speaker = state.get("last_speaker")
    speaker_change_count = int(state.get("speaker_change_count", 0))
    if last_speaker is not None and last_speaker != turn.speaker_id:
        speaker_change_count += 1
    interruption_count = int(state.get("interruption_count", 0))
    if behavior_label == "打断":
        interruption_count += 1
    reference_interruption_count = int(state.get("reference_interruption_count", 0))
    if turn.ref_dialogue_act == "打断":
        reference_interruption_count += 1

    next_state = {
        "step": turn.index,
        "turn_count": turn.index + 1,
        "dominant_emotion": dominant,
        "cumulative_emotions": cumulative,
        "normalized_emotions": normalized_current,
        "emotion_trend": trend,
        "trend_label": trend_label,
        "interruption_count": interruption_count,
        "reference_interruption_count": reference_interruption_count,
        "speaker_change_count": speaker_change_count,
        "last_speaker": turn.speaker_id,
        "current_emotion": emotion_artifact.get("label"),
        "current_behavior": behavior_label,
        "reference_emotion": turn.ref_emotion,
        "reference_behavior": turn.ref_dialogue_act,
        "turn_start_ms": turn.start_ms,
        "turn_end_ms": turn.end_ms,
    }
    artifact = {
        "schema_version": 1,
        "shard_id": shard.shard_id,
        "turn_index": turn.index,
        "emotion_digest": digest_json(emotion_artifact),
        "behavior_digest": digest_json(behavior_artifact),
        "previous_state_digest": previous_state_digest,
        "state": next_state,
        "config_version": config.version,
    }
    return round_floats(artifact, config.decimals)


def summarize_shard(shard: ShardData, state_artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    if len(state_artifacts) != shard.turn_count:
        raise as_error(
            E_INVALID_STAGE,
            f"expected {shard.turn_count} state artifacts, got {len(state_artifacts)}",
            shard_id=shard.shard_id,
            stage="state",
        )
    trajectory = []
    for index, artifact in enumerate(state_artifacts):
        state = artifact.get("state")
        if not isinstance(state, dict) or state.get("step") != index:
            raise as_error(
                E_INVALID_STAGE,
                "state artifacts are not in contiguous turn order",
                shard_id=shard.shard_id,
                turn_index=index,
                stage="state",
            )
        trajectory.append(
            {
                "turn_index": index,
                "dominant_emotion": state["dominant_emotion"],
                "trend_label": state["trend_label"],
                "current_emotion": state["current_emotion"],
                "current_behavior": state["current_behavior"],
                "interruption_count": state["interruption_count"],
                "cumulative_emotions": state["cumulative_emotions"],
            }
        )
    final_state = state_artifacts[-1]["state"]
    summary = {
        "schema_version": 1,
        "shard_id": shard.shard_id,
        "turn_count": shard.turn_count,
        "speaker_ids": list(dict.fromkeys(turn.speaker_id for turn in shard.turns)),
        "dominant_emotion": final_state["dominant_emotion"],
        "cumulative_emotions": final_state["cumulative_emotions"],
        "interruption_count": final_state["interruption_count"],
        "reference_interruption_count": final_state["reference_interruption_count"],
        "speaker_change_count": final_state["speaker_change_count"],
        "trajectory": trajectory,
    }
    summary["summary_digest"] = digest_json(summary)
    return summary

