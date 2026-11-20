"""Deterministic dialogue-act recognition using rules and transition weights."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .config import DIALOGUE_ACTS, BehaviorConfig
from .emotion import validate_emotion_artifact
from .errors import E_FEATURE_DIM, E_INVALID_STAGE, E_NUMERIC, as_error
from .manifest import ShardData, TurnAnnotation
from .serialization import digest_json, round_floats


def _softmax(scores: dict[str, float], temperature: float) -> dict[str, float]:
    logits = np.asarray(
        [scores[act] * temperature for act in DIALOGUE_ACTS], dtype=np.float64
    )
    logits -= float(np.max(logits))
    values = np.exp(logits)
    values /= values.sum()
    return {act: float(values[index]) for index, act in enumerate(DIALOGUE_ACTS)}


def recognize_behavior(
    shard: ShardData,
    turn: TurnAnnotation,
    feature_artifact: dict[str, Any],
    emotion_artifact: dict[str, Any],
    *,
    previous_turn: TurnAnnotation | None,
    previous_emotion_artifact: dict[str, Any] | None,
    previous_behavior_artifact: dict[str, Any] | None,
    config: BehaviorConfig,
    expected_feature_dim: int | None = None,
) -> dict[str, Any]:
    """Classify one turn and retain explicit evidence for auditability."""

    config.validate()
    vector = feature_artifact.get("vector")
    if not isinstance(vector, list):
        raise as_error(
            E_FEATURE_DIM,
            "feature vector is missing or malformed",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )
    try:
        vector_array = np.asarray(vector, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise as_error(
            E_FEATURE_DIM,
            f"feature vector cannot be interpreted as numbers: {exc}",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        ) from exc
    if vector_array.ndim != 1 or vector_array.size < 16:
        raise as_error(
            E_FEATURE_DIM,
            f"feature dimension {vector_array.size} is too small",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )
    if feature_artifact.get("feature_dim") != int(vector_array.size):
        raise as_error(
            E_FEATURE_DIM,
            "feature artifact feature_dim does not match its vector length",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )
    if expected_feature_dim is not None and vector_array.size != expected_feature_dim:
        raise as_error(
            E_FEATURE_DIM,
            f"feature dimension {vector_array.size} != expected {expected_feature_dim}",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
            actual_dimension=int(vector_array.size),
            expected_dimension=expected_feature_dim,
        )
    if not np.all(np.isfinite(vector_array)):
        raise as_error(
            E_NUMERIC,
            "feature vector contains NaN or Inf",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )
    current_emotion_scores = validate_emotion_artifact(
        emotion_artifact, shard_id=shard.shard_id, turn_index=turn.index
    )
    if emotion_artifact.get("turn_index") != turn.index:
        raise as_error(
            E_INVALID_STAGE,
            "emotion artifact belongs to a different turn",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )

    if turn.index > 0:
        if previous_turn is None or previous_turn.index != turn.index - 1:
            raise as_error(
                E_INVALID_STAGE,
                "previous turn annotation is required for behavior transition",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="behavior",
            )
        if previous_emotion_artifact is None:
            raise as_error(
                E_INVALID_STAGE,
                "previous emotion result is required for behavior transition",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="behavior",
            )
        previous_scores = validate_emotion_artifact(
            previous_emotion_artifact,
            shard_id=shard.shard_id,
            turn_index=turn.index - 1,
        )
        if previous_behavior_artifact is None:
            raise as_error(
                E_INVALID_STAGE,
                "previous behavior result is required for behavior transition",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="behavior",
            )
        previous_label = previous_behavior_artifact.get("label")
        if previous_label not in DIALOGUE_ACTS:
            raise as_error(
                E_INVALID_STAGE,
                f"previous behavior label {previous_label!r} is invalid",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="behavior",
            )
        if previous_behavior_artifact.get("turn_index") != turn.index - 1:
            raise as_error(
                E_INVALID_STAGE,
                "previous behavior result belongs to a different turn",
                shard_id=shard.shard_id,
                turn_index=turn.index,
                stage="behavior",
            )
        gap_before_ms = float(turn.start_ms - previous_turn.end_ms)
        speaker_changed = previous_turn.speaker_id != turn.speaker_id
    else:
        previous_scores = {label: 0.0 for label in ("平静", "高兴", "愤怒", "悲伤")}
        previous_label = None
        gap_before_ms = 0.0
        speaker_changed = False

    if gap_before_ms < 0.0:
        raise as_error(
            "E_INVALID_TURN",
            f"negative inter-turn gap: {gap_before_ms} ms",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )
    duration_ms = float(feature_artifact.get("duration_ms", turn.duration_ms))
    pause_ratio = float(feature_artifact.get("pause_ratio", vector_array[10]))
    voiced_slope = float(vector_array[5])
    voiced_delta = float(vector_array[13])
    voiced_mean = float(vector_array[14])
    intensity = 1.0 / (1.0 + math.exp(-(voiced_mean + 5.0)))
    normalized_slope = max(0.0, min(1.0, voiced_slope / 2.0))
    normalized_delta = max(0.0, min(1.0, voiced_delta / 8.0))

    rule_scores = {
        "陈述": (
            config.statement_prior
            + 0.80 * min(duration_ms / 1200.0, 1.0)
            + 0.25 * (1.0 if pause_ratio >= 0.12 else 0.0)
        ),
        "提问": (
            0.10
            + config.question_energy_slope_gain * normalized_slope
            + config.question_end_energy_gain * normalized_delta
            + 0.12 * pause_ratio
        ),
        "确认": (
            0.05
            + 1.00
            * (1.0 if duration_ms <= config.confirmation_max_duration_ms else 0.0)
            + config.confirmation_low_pause_gain
            * (1.0 if pause_ratio < 0.03 else 0.0)
            + 0.50 * (1.0 if gap_before_ms >= 100.0 else 0.0)
        ),
        "打断": (
            0.05
            + 0.80
            * (1.0 if duration_ms <= config.interruption_max_duration_ms else 0.0)
            + 1.00
            * (1.0 if gap_before_ms <= config.interruption_max_gap_ms else 0.0)
            + 0.35 * (1.0 if speaker_changed else 0.0)
            + config.interruption_energy_gain * intensity
        ),
    }

    transition_boost = {act: 0.0 for act in DIALOGUE_ACTS}
    if previous_label is not None:
        weights = config.transition_weights[previous_label]
        for act in DIALOGUE_ACTS:
            transition_boost[act] = config.prior_act_weight * float(weights[act])
            rule_scores[act] += transition_boost[act]
    emotion_context = {
        "previous_anger": previous_scores["愤怒"],
        "previous_sadness": previous_scores["悲伤"],
        "previous_happiness": previous_scores["高兴"],
    }
    rule_scores["打断"] += (
        config.prior_emotion_weight * emotion_context["previous_anger"] * 0.80
    )
    rule_scores["陈述"] += (
        config.prior_emotion_weight * emotion_context["previous_sadness"] * 0.30
    )
    rule_scores["提问"] += (
        config.prior_emotion_weight * emotion_context["previous_happiness"] * 0.10
    )
    if not all(math.isfinite(value) for value in rule_scores.values()):
        raise as_error(
            E_NUMERIC,
            "dialogue-act rule scores contain NaN or Inf",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )
    probabilities = _softmax(rule_scores, config.score_temperature)
    if not all(math.isfinite(value) for value in probabilities.values()):
        raise as_error(
            E_NUMERIC,
            "dialogue-act probability scores contain NaN or Inf",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="behavior",
        )
    ranking = sorted(
        DIALOGUE_ACTS,
        key=lambda act: (-probabilities[act], DIALOGUE_ACTS.index(act)),
    )
    artifact = {
        "schema_version": 1,
        "shard_id": shard.shard_id,
        "turn_index": turn.index,
        "emotion_digest": digest_json(emotion_artifact),
        "feature_digest": digest_json(feature_artifact),
        "label": ranking[0],
        "scores": probabilities,
        "confidence": probabilities[ranking[0]],
        "ranking": list(ranking),
        "rule_scores": rule_scores,
        "transition_boost": transition_boost,
        "evidence": {
            "duration_ms": duration_ms,
            "gap_before_ms": gap_before_ms,
            "pause_ratio": pause_ratio,
            "voiced_energy_slope": voiced_slope,
            "voiced_energy_delta": voiced_delta,
            "voiced_energy_mean": voiced_mean,
            "intensity": intensity,
            "speaker_changed": speaker_changed,
            "speaker_id": turn.speaker_id,
            "previous_label": previous_label,
            "emotion_context": emotion_context,
        },
        "config_version": config.version,
    }
    return round_floats(artifact, config.decimals)


def validate_behavior_artifact(
    artifact: Any,
    *,
    shard_id: str,
    turn_index: int,
) -> str:
    if not isinstance(artifact, dict):
        raise as_error(
            E_INVALID_STAGE,
            "behavior artifact is not an object",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="behavior",
        )
    if artifact.get("turn_index") != turn_index:
        raise as_error(
            E_INVALID_STAGE,
            "behavior artifact belongs to a different turn",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="behavior",
        )
    label = artifact.get("label")
    if label not in DIALOGUE_ACTS:
        raise as_error(
            E_INVALID_STAGE,
            f"behavior label {label!r} is invalid",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="behavior",
        )
    scores = artifact.get("scores")
    if not isinstance(scores, dict) or set(scores) != set(DIALOGUE_ACTS):
        raise as_error(
            E_INVALID_STAGE,
            "behavior scores must contain exactly four dialogue acts",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="behavior",
        )
    for act in DIALOGUE_ACTS:
        value = scores[act]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise as_error(
                E_NUMERIC,
                f"behavior score for {act} is not numeric",
                shard_id=shard_id,
                turn_index=turn_index,
                stage="behavior",
            )
        if not math.isfinite(float(value)):
            raise as_error(
                E_NUMERIC,
                f"behavior score for {act} is not finite",
                shard_id=shard_id,
                turn_index=turn_index,
                stage="behavior",
            )
    return str(label)

