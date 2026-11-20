"""Deterministic hashed-projection emotion classifier (no learned weights)."""

from __future__ import annotations

import hashlib
import math
from functools import lru_cache
from typing import Any

import numpy as np

from .config import EMOTIONS, ClassifierConfig, FeatureConfig
from .errors import E_FEATURE_DIM, E_NUMERIC, as_error
from .features import validate_feature_artifact
from .serialization import digest_json, round_floats


ANCHOR_VERSION = "acoustic-anchors-v2"

# Each descriptor is centered around typical conversational speech. The anchors
# are fixed acoustic profiles, not weights learned from the demonstration data.
_CLASS_ANCHORS = {
    "平静": (-0.30, -0.20, -0.20, -0.50, 0.00, 0.00, -0.30, 0.0, 0.0, 0.0, 3.0),
    "高兴": (0.60, 0.60, 0.70, 0.30, -0.30, 0.00, 0.50, 0.0, 1.5, 0.0, 0.0),
    "愤怒": (0.80, 1.50, 1.40, 1.20, -0.60, -1.00, 1.00, 1.8, 0.0, 0.0, 0.0),
    "悲伤": (-0.50, -0.80, -0.80, -0.80, 0.40, 0.60, -0.70, 0.0, 0.0, 1.8, 0.0),
}


def _descriptor(vector: np.ndarray, mel_bands: int) -> np.ndarray:
    mel = vector[16 : 16 + mel_bands]
    if mel.size != mel_bands:
        raise ValueError("mel band slice is shorter than configured dimension")
    low_mel = float(np.mean(mel[: max(1, mel_bands // 4)]))
    high_mel = float(np.mean(mel[-max(1, mel_bands // 4) :]))
    tilt = high_mel - low_mel
    zcr = float(vector[6])
    centroid = float(vector[8])
    voiced_mean = float(vector[14])
    voiced_std = float(vector[15])
    pause_ratio = float(vector[10])
    angry_evidence = max(0.0, (zcr - 0.042) / 0.01) + max(
        0.0, (voiced_mean + 3.5) / 0.5
    )
    happy_evidence = (
        max(0.0, (zcr - 0.020) / 0.01)
        * max(0.0, voiced_mean + 6.0)
        * max(0.0, (0.045 - zcr) / 0.01)
    )
    sad_evidence = max(0.0, (0.017 - zcr) / 0.006) * max(
        0.0, (-6.5 - voiced_mean) / 0.5
    )
    calm_evidence = max(0.0, (0.022 - zcr) / 0.006) * max(
        0.0, (voiced_mean + 6.7) / 0.4
    )
    descriptor = np.asarray(
        [
            (voiced_mean + 8.0) / 4.0,
            (zcr - 0.018) / 0.02,
            (centroid - 0.025) / 0.02,
            (tilt + 4.5) / 2.0,
            (pause_ratio - 0.15) / 0.20,
            voiced_std - 1.0,
            (float(vector[3]) + 5.0) / 2.0,
            angry_evidence,
            happy_evidence,
            sad_evidence,
            calm_evidence,
        ],
        dtype=np.float64,
    )
    return descriptor


@lru_cache(maxsize=32)
def _projection_matrix(seed: int, input_dim: int, output_dim: int) -> np.ndarray:
    matrix = np.empty((output_dim, input_dim), dtype=np.float64)
    for row in range(output_dim):
        for column in range(input_dim):
            digest = hashlib.sha256(
                f"emotion-hash|{seed}|{row}|{column}".encode("utf-8")
            ).digest()
            matrix[row, column] = 1.0 if digest[0] & 1 else -1.0
    matrix /= math.sqrt(output_dim)
    return matrix


def _cosine(left: np.ndarray, right: np.ndarray) -> float:
    left_norm = float(np.linalg.norm(left))
    right_norm = float(np.linalg.norm(right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return float(np.dot(left, right) / (left_norm * right_norm))


def projection_fingerprint(config: ClassifierConfig) -> str:
    return digest_json(
        {
            "seed": config.projection_seed,
            "dim": config.projection_dim,
            "anchors": ANCHOR_VERSION,
            "labels": list(EMOTIONS),
        }
    )


def score_features(
    feature_artifact: Any,
    *,
    feature_config: FeatureConfig,
    classifier_config: ClassifierConfig,
    shard_id: str,
    turn_index: int,
) -> dict[str, Any]:
    classifier_config.validate()
    feature_vector = validate_feature_artifact(
        feature_artifact,
        feature_config,
        shard_id=shard_id,
        turn_index=turn_index,
    )
    descriptor = _descriptor(feature_vector, feature_config.mel_bands)
    if not np.all(np.isfinite(descriptor)):
        raise as_error(
            E_NUMERIC,
            "emotion descriptor contains NaN or Inf",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="emotion",
        )
    matrix = _projection_matrix(
        classifier_config.projection_seed,
        descriptor.shape[0],
        classifier_config.projection_dim,
    )
    projected = matrix @ descriptor
    similarities: dict[str, float] = {}
    for label in EMOTIONS:
        anchor = matrix @ np.asarray(_CLASS_ANCHORS[label], dtype=np.float64)
        similarities[label] = _cosine(projected, anchor)

    logits = np.asarray(
        [similarities[label] * classifier_config.score_temperature for label in EMOTIONS],
        dtype=np.float64,
    )
    logits -= float(np.max(logits))
    exponentials = np.exp(logits)
    probabilities = exponentials / exponentials.sum()
    if not np.all(np.isfinite(probabilities)):
        raise as_error(
            E_NUMERIC,
            "emotion scores contain NaN or Inf",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="emotion",
        )
    score_map = {
        label: float(probabilities[index]) for index, label in enumerate(EMOTIONS)
    }
    ranking = sorted(
        EMOTIONS,
        key=lambda label: (-score_map[label], EMOTIONS.index(label)),
    )
    artifact = {
        "schema_version": 1,
        "shard_id": shard_id,
        "turn_index": turn_index,
        "feature_digest": digest_json(feature_artifact),
        "label": ranking[0],
        "scores": score_map,
        "similarities": similarities,
        "confidence": score_map[ranking[0]],
        "ranking": list(ranking),
        "projection_fingerprint": projection_fingerprint(classifier_config),
        "projection_dim": classifier_config.projection_dim,
        "config_version": classifier_config.version,
    }
    return round_floats(artifact, classifier_config.decimals)


def validate_emotion_artifact(
    artifact: Any,
    *,
    shard_id: str,
    turn_index: int,
) -> dict[str, float]:
    if not isinstance(artifact, dict):
        raise as_error(
            E_FEATURE_DIM,
            "emotion artifact is not an object",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="emotion",
        )
    raw_scores = artifact.get("scores")
    if not isinstance(raw_scores, dict) or set(raw_scores) != set(EMOTIONS):
        raise as_error(
            E_FEATURE_DIM,
            "emotion scores must contain exactly four emotion classes",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="emotion",
        )
    scores: dict[str, float] = {}
    for label in EMOTIONS:
        value = raw_scores[label]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise as_error(
                E_NUMERIC,
                f"emotion score for {label} is not numeric",
                shard_id=shard_id,
                turn_index=turn_index,
                stage="emotion",
            )
        number = float(value)
        if not math.isfinite(number) or number < 0.0:
            raise as_error(
                E_NUMERIC,
                f"emotion score for {label} is invalid: {number}",
                shard_id=shard_id,
                turn_index=turn_index,
                stage="emotion",
            )
        scores[label] = number
    if not math.isclose(sum(scores.values()), 1.0, abs_tol=1e-6):
        raise as_error(
            E_NUMERIC,
            "emotion scores do not sum to one",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="emotion",
            score_sum=sum(scores.values()),
        )
    return scores

