from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, UnidentifiedImageError

from .config import WorkbenchConfig
from .manifest import Dataset, ManifestSample, scores_for_digest, truth_rows
from .util import (
    digest,
    file_sha256,
    relpath,
    stable_round,
    strict_float,
)


def preprocess_artifact(
    sample: ManifestSample,
    dataset: Dataset,
    config: WorkbenchConfig,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    missing = sample.missing_fields()
    if missing:
        errors.append(
            _error(
                "E_MISSING_FIELD",
                "manifest sample is missing required fields",
                sample,
                {"fields": missing},
            )
        )

    declared = {"width": sample.raw.get("width"), "height": sample.raw.get("height")}
    actual: dict[str, Any] = {"width": None, "height": None}
    image_sha: str | None = None
    profile: dict[str, Any] | None = None
    profile_digest: str | None = None
    decoded = False

    if sample.image_path is None:
        if "path" not in sample.raw:
            pass
        else:
            errors.append(
                _error(
                    "E_MISSING_FIELD",
                    "sample path is not a usable string",
                    sample,
                    {"field": "path"},
                )
            )
    elif not sample.image_path.exists():
        errors.append(
            _error(
                "E_NOT_FOUND",
                "sample image file does not exist",
                sample,
                {"path": relpath(dataset.root, sample.image_path)},
            )
        )
    else:
        image_sha = file_sha256(sample.image_path)
        try:
            with Image.open(sample.image_path) as opened:
                if opened.format != "PNG":
                    raise ValueError(f"expected PNG, got {opened.format}")
                opened.load()
                actual["width"], actual["height"] = opened.size
                pixels = np.asarray(opened.convert("RGB"), dtype=np.int64)
            decoded = True
            profile = _profile(pixels, int(config.data["preprocess"]["block_size"]), config.data["preprocess"])
            profile_digest = digest({"profile": profile})
        except (UnidentifiedImageError, OSError, ValueError) as exc:
            errors.append(
                _error(
                    "E_IMAGE_DECODE",
                    "PNG image could not be decoded",
                    sample,
                    {"path": relpath(dataset.root, sample.image_path), "error": str(exc)},
                )
            )

    if decoded:
        declared_width = declared["width"]
        declared_height = declared["height"]
        if (
            not isinstance(declared_width, int)
            or isinstance(declared_width, bool)
            or not isinstance(declared_height, int)
            or isinstance(declared_height, bool)
            or declared_width != actual["width"]
            or declared_height != actual["height"]
        ):
            errors.append(
                _error(
                    "E_DIM_MISMATCH",
                    "declared image dimensions do not match the decoded PNG",
                    sample,
                    {"declared": declared, "actual": actual},
                )
            )

    status = "error" if errors else "ok"
    return {
        "status": status,
        "image_id": sample.image_id,
        "sample_key": sample.key,
        "source": {
            **sample.source_summary(dataset.root),
            "image_sha256": image_sha,
            "declared": declared,
            "actual": actual,
        },
        "diagnostics": {
            "decoded": decoded,
            "dimensions_match": decoded
            and isinstance(declared["width"], int)
            and isinstance(declared["height"], int)
            and declared["width"] == actual["width"]
            and declared["height"] == actual["height"],
            "normalization": config.data["preprocess"]["normalization"],
            "block_size": int(config.data["preprocess"]["block_size"]),
        },
        "profile": profile,
        "profile_digest": profile_digest,
        "errors": errors,
    }


def score_artifact(
    sample: ManifestSample,
    dataset: Dataset,
    config: WorkbenchConfig,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    decisions: dict[str, Any] = {}
    conflicts: list[dict[str, Any]] = []
    truth = truth_rows(sample)
    scores = sample.scores
    class_set = set(dataset.classes)

    if truth is None:
        errors.append(
            _error(
                "E_MISSING_FIELD",
                "truth must be a list of {label, lower_bound} objects",
                sample,
                {"field": "truth"},
            )
        )
    if not isinstance(scores, dict):
        errors.append(
            _error(
                "E_SCORE_LENGTH",
                "score table must be an object keyed by label",
                sample,
                {
                    "field": "scores",
                    "expected_labels": len(dataset.classes),
                    "actual_labels": None,
                },
            )
        )

    truth_labels: list[str] = [row["label"] for row in truth] if truth is not None else []
    score_labels: list[str] = list(scores.keys()) if isinstance(scores, dict) else []
    unknown_labels = sorted((set(truth_labels) | set(score_labels)) - class_set)
    if unknown_labels:
        errors.append(
            _error(
                "E_UNKNOWN_LABEL",
                "label is outside the manifest class table",
                sample,
                {"labels": unknown_labels, "classes": dataset.classes},
            )
        )

    if isinstance(scores, dict):
        actual_set = set(score_labels)
        if actual_set != class_set:
            errors.append(
                _error(
                    "E_SCORE_LENGTH",
                    "score table must contain exactly one score for every class label",
                    sample,
                    {
                        "expected_labels": len(dataset.classes),
                        "actual_labels": len(score_labels),
                        "missing_labels": sorted(class_set - actual_set),
                        "extra_labels": sorted(actual_set - class_set),
                    },
                )
            )

    parsed_scores: dict[str, float] = {}
    if isinstance(scores, dict):
        for label in score_labels:
            try:
                parsed = strict_float(scores[label])
            except (TypeError, ValueError):
                errors.append(
                    _error(
                        "E_NUMERIC",
                        "score is not numeric",
                        sample,
                        {"field": "scores", "label": label, "value": str(scores[label])},
                    )
                )
                continue
            if not math.isfinite(parsed):
                errors.append(
                    _error(
                        "E_NUMERIC",
                        "score must be finite",
                        sample,
                        {
                            "field": "scores",
                            "label": label,
                            "value": _number_token(parsed),
                        },
                    )
                )
                continue
            parsed_scores[label] = parsed

    if truth is not None and any(row["lower_bound"] is None for row in truth):
        errors.append(
            _error(
                "E_NUMERIC",
                "truth lower_bound must be finite",
                sample,
                {"field": "truth.lower_bound"},
            )
        )

    if not errors:
        lower_by_label = {row["label"]: float(row["lower_bound"]) for row in truth}
        default_threshold = float(config.data["scoring"]["default_threshold"])
        positive_labels: list[str] = []
        for label in dataset.classes:
            score = parsed_scores[label]
            if label in lower_by_label:
                threshold = lower_by_label[label]
                if score >= threshold:
                    outcome = "hit"
                    predicted = True
                else:
                    outcome = "miss"
                    predicted = False
                role = "truth"
            else:
                threshold = default_threshold
                if score >= threshold:
                    outcome = "false_positive"
                    predicted = True
                else:
                    outcome = "true_negative"
                    predicted = False
                role = "non_truth"
            if predicted:
                positive_labels.append(label)
            decisions[label] = {
                "score": stable_round(score, 12),
                "role": role,
                "threshold": stable_round(threshold, 12),
                "predicted": predicted,
                "outcome": outcome,
            }
        truth_set = set(truth_labels)
        predicted_set = set(positive_labels)
        for left, right in config.data["scoring"]["exclusive_pairs"]:
            kinds: list[str] = []
            if left in truth_set and right in truth_set:
                kinds.append("ground_truth")
            if left in predicted_set and right in predicted_set:
                kinds.append("prediction")
            if kinds:
                conflict = {
                    "code": "E_LABEL_CONFLICT",
                    "image_id": sample.image_id,
                    "sample_key": sample.key,
                    "label_pair": [left, right],
                    "kinds": kinds,
                    "message": "mutually exclusive labels are simultaneously positive",
                }
                conflicts.append(conflict)
                errors.append({**conflict, "details": {"label_pair": [left, right], "kinds": kinds}})

    status = "error" if any(error["code"] != "E_LABEL_CONFLICT" for error in errors) else ("warning" if errors else "ok")
    counts = {
        "hit": sum(1 for item in decisions.values() if item["outcome"] == "hit"),
        "miss": sum(1 for item in decisions.values() if item["outcome"] == "miss"),
        "false_positive": sum(1 for item in decisions.values() if item["outcome"] == "false_positive"),
        "true_negative": sum(1 for item in decisions.values() if item["outcome"] == "true_negative"),
    }
    return {
        "status": status,
        "image_id": sample.image_id,
        "sample_key": sample.key,
        "source": {
            "manifest_path": relpath(dataset.root, sample.manifest_path),
            "manifest_record_index": sample.source_index,
            "classes": dataset.classes,
            "truth": truth if truth is not None else sample.raw.get("truth"),
            "scores": scores_for_digest(sample),
        },
        "decisions": decisions,
        "positive_labels": [label for label in dataset.classes if label in decisions and decisions[label]["predicted"]],
        "counts": counts,
        "conflicts": conflicts,
        "errors": errors,
    }


def metric_contribution(score: dict[str, Any]) -> dict[str, Any]:
    decisions = score["decisions"]
    truth_labels = [
        label for label, item in decisions.items() if item["role"] == "truth"
    ]
    predicted_labels = [
        label for label, item in decisions.items() if item["predicted"]
    ]
    labels = {
        label: {
            "truth": item["role"] == "truth",
            "score": item["score"],
            "predicted": item["predicted"],
            "outcome": item["outcome"],
        }
        for label, item in decisions.items()
    }
    return {
        "status": score["status"],
        "image_id": score["image_id"],
        "sample_key": score["sample_key"],
        "labels": labels,
        "truth_positive_labels": truth_labels,
        "predicted_positive_labels": predicted_labels,
        "counts": dict(score["counts"]),
        "source": {
            "scoring_stage": "score",
            "sample_key": score["sample_key"],
        },
    }


def report_artifact(
    sample: ManifestSample,
    dataset: Dataset,
    config: WorkbenchConfig,
    preprocess_record: dict[str, Any] | None,
    score_record: dict[str, Any] | None,
    metrics_record: dict[str, Any] | None,
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    stage_artifacts = [
        ("preprocess", 1, preprocess_record),
        ("score", 2, score_record),
        ("metrics", 3, metrics_record),
    ]
    for stage_name, stage_index, record in stage_artifacts:
        if record is None:
            continue
        artifact = record["artifact"]
        if stage_name == "score":
            conflicts.extend(artifact.get("conflicts", []))
        if artifact.get("status") == "error":
            for error in artifact.get("errors", []):
                failures.append(
                    {
                        "stage": stage_name,
                        "stage_index": stage_index,
                        "code": error.get("code", "E_UNKNOWN"),
                        "message": error.get("message", ""),
                        "details": error.get("details", {}),
                        "image_id": sample.image_id,
                        "sample_key": sample.key,
                        "image_path": sample.relative_image_path,
                        "manifest_path": relpath(dataset.root, sample.manifest_path),
                    }
                )
    if preprocess_record and preprocess_record["artifact"].get("status") == "error":
        status = "error"
    elif score_record and score_record["artifact"].get("status") == "error":
        status = "error"
    elif conflicts:
        status = "warning"
    else:
        status = "ok"
    decision_summary = None
    if score_record and score_record["artifact"].get("status") in {"ok", "warning"}:
        decision_summary = {
            "counts": score_record["artifact"]["counts"],
            "positive_labels": score_record["artifact"]["positive_labels"],
        }
    return {
        "status": status,
        "image_id": sample.image_id,
        "sample_key": sample.key,
        "source": {
            "manifest_path": relpath(dataset.root, sample.manifest_path),
            "manifest_record_index": sample.source_index,
            "image_path": sample.relative_image_path,
            "image_sha256": preprocess_record["artifact"]["source"].get("image_sha256")
            if preprocess_record
            else None,
        },
        "failures": failures,
        "conflicts": conflicts,
        "decision_summary": decision_summary,
        "stage_statuses": {
            "preprocess": preprocess_record["artifact"].get("status") if preprocess_record else "missing",
            "score": score_record["artifact"].get("status") if score_record else "not_run",
            "metrics": metrics_record["artifact"].get("status") if metrics_record else "not_run",
        },
        "config_version": config.config_version,
    }


def _profile(pixels: np.ndarray, block_size: int, preprocess_config: dict[str, Any]) -> dict[str, Any]:
    height, width, _ = pixels.shape
    center = float(preprocess_config["normalization"]["center"])
    scale = float(preprocess_config["normalization"]["scale"])
    rgb = pixels.astype(np.float64) / 255.0
    normalized = (rgb - center) / scale
    gray = (pixels[:, :, 0] * 299 + pixels[:, :, 1] * 587 + pixels[:, :, 2] * 114) // 1000
    blocks: list[dict[str, Any]] = []
    for y0 in range(0, height, block_size):
        for x0 in range(0, width, block_size):
            x1 = min(width, x0 + block_size)
            y1 = min(height, y0 + block_size)
            count = (x1 - x0) * (y1 - y0)
            block_rgb = normalized[y0:y1, x0:x1, :]
            means = [stable_round(float(block_rgb[:, :, channel].mean()), 8) for channel in range(3)]
            gray_block = gray[y0:y1, x0:x1]
            gradient_values: list[np.ndarray] = []
            if x1 - x0 > 1:
                gradient_values.append(np.abs(np.diff(gray_block, axis=1)).reshape(-1))
            if y1 - y0 > 1:
                gradient_values.append(np.abs(np.diff(gray_block, axis=0)).reshape(-1))
            if gradient_values:
                gradient_sum = int(sum(int(values.sum()) for values in gradient_values))
                gradient_count = int(sum(values.size for values in gradient_values))
                gradient_mean = gradient_sum / (255.0 * gradient_count)
            else:
                gradient_mean = 0.0
            blocks.append(
                {
                    "row": y0 // block_size,
                    "col": x0 // block_size,
                    "x": [x0, x1],
                    "y": [y0, y1],
                    "pixel_count": count,
                    "mean_normalized_rgb": means,
                    "mean_abs_gradient": stable_round(gradient_mean, 8),
                }
            )
    return {
        "width": width,
        "height": height,
        "block_size": block_size,
        "normalization": {
            "rule": preprocess_config["normalization"]["rule"],
            "center": center,
            "scale": scale,
        },
        "global_mean_normalized_rgb": [
            stable_round(float(normalized[:, :, channel].mean()), 8) for channel in range(3)
        ],
        "blocks": blocks,
    }


def _error(
    code: str,
    message: str,
    sample: ManifestSample,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "code": code,
        "message": message,
        "image_id": sample.image_id,
        "sample_key": sample.key,
        "details": {"image_id": sample.image_id, **details},
    }


def _number_token(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if value == math.inf:
        return "Inf"
    return "-Inf"


__all__ = [
    "metric_contribution",
    "preprocess_artifact",
    "report_artifact",
    "score_artifact",
]
