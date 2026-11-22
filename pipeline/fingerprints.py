from __future__ import annotations

from typing import Any

from .config import Config
from .features import feature_input_fingerprint
from .util import canonical_digest


def artifact_digest(artifacts: list[dict[str, Any]]) -> str:
    references = [
        {"path": artifact["path"], "digest": artifact["digest"]} for artifact in artifacts
    ]
    return canonical_digest(references)


def feature_fingerprint(batch_id: str, data_digest: str, row_count: int, config: Config) -> str:
    return feature_input_fingerprint(batch_id, data_digest, row_count, config.hash_dim)


def train_score_fingerprint(
    feature_fp: str,
    feature_artifact_digest: str,
    labels: tuple[str, ...],
    config: Config,
) -> str:
    return canonical_digest(
        {
            "stage_schema": "train-score-v1",
            "feature_fingerprint": feature_fp,
            "feature_artifact_digest": feature_artifact_digest,
            "labels": list(labels),
            "smoothing": config.smoothing,
            "unknown_token": config.unknown_token,
            "fold_modulus": config.calibration.fold_modulus,
            "fold_remainder": config.calibration.fold_remainder,
        }
    )


def calibration_fingerprint(
    train_fp: str,
    train_artifact_digest: str,
    labels: tuple[str, ...],
    config: Config,
) -> str:
    return canonical_digest(
        {
            "stage_schema": "calibration-v2",
            "train_score_fingerprint": train_fp,
            "train_score_artifact_digest": train_artifact_digest,
            "labels": list(labels),
            "thresholds": config.thresholds,
            "target_fpr": config.target_fpr,
            "threshold_max": config.threshold_max,
            "min_samples_per_class": config.calibration.min_samples_per_class,
            "fold_modulus": config.calibration.fold_modulus,
            "fold_remainder": config.calibration.fold_remainder,
        }
    )


def summarize_fingerprint(
    calibration_fp: str,
    calibration_artifact_digest: str,
    labels: tuple[str, ...],
    config: Config,
) -> str:
    return canonical_digest(
        {
            "stage_schema": "summarize-v2",
            "calibration_fingerprint": calibration_fp,
            "calibration_artifact_digest": calibration_artifact_digest,
            "labels": list(labels),
            "class_weights": config.class_weights,
        }
    )
