"""Per-sample detector normalization, combination and threshold decisions."""

from __future__ import annotations

from typing import Any

import numpy as np

from .detectors import score_isolation_forest, score_robust_covariance
from .errors import WorkbenchError
from .preprocessing import matrix_from_preprocess
from .util import jsonable_array


def _bad_sample(
    row: int, values: np.ndarray, source_ids: list[str], message: str, details: dict[str, Any]
) -> WorkbenchError:
    sample_id = source_ids[row] if row < len(source_ids) else None
    return WorkbenchError(
        "E_NUMERIC",
        message,
        stage="score",
        sample_id=sample_id,
        details={"row": int(row), "value": float(values[row]), **details},
    )


def _quantile(values: np.ndarray, quantile: float) -> float:
    return float(np.quantile(values, quantile, method="linear"))


def _transform(raw: np.ndarray, kind: str) -> np.ndarray:
    if kind == "log1p":
        return np.log1p(np.maximum(raw, 0.0))
    if kind == "logit":
        clipped = np.clip(raw, 1e-12, 1.0 - 1e-12)
        return np.log(clipped / (1.0 - clipped))
    if kind == "identity":
        return raw
    raise WorkbenchError("E_INVALID_CONFIG", f"unsupported score transform: {kind}", stage="score")


def _normalize_detector(
    raw: np.ndarray,
    *,
    transform: str,
    center_quantile: float,
    scale_quantiles: list[float],
    scale_floor: float,
    clip: float,
) -> tuple[np.ndarray, dict[str, Any]]:
    transformed = _transform(np.asarray(raw, dtype=np.float64), transform)
    center = _quantile(transformed, center_quantile)
    low = _quantile(transformed, scale_quantiles[0])
    high = _quantile(transformed, scale_quantiles[1])
    scale = high - low
    if not np.isfinite(scale) or scale <= scale_floor:
        scale = scale_floor
    normalized = (transformed - center) / scale
    normalized = np.clip(normalized, -clip, clip)
    model = {
        "transform": transform,
        "center_quantile": float(center_quantile),
        "center": float(center),
        "scale_quantiles": [float(value) for value in scale_quantiles],
        "scale": float(scale),
        "clip": float(clip),
        "raw_min": float(np.min(raw)),
        "raw_max": float(np.max(raw)),
        "raw_mean": float(np.mean(raw)),
        "transformed_min": float(np.min(transformed)),
        "transformed_max": float(np.max(transformed)),
    }
    return normalized, model


def score_and_decide(
    preprocess_artifact: dict[str, Any],
    fit_artifact: dict[str, Any],
    config: dict[str, Any],
    declaration: dict[str, Any],
) -> dict[str, Any]:
    payload = preprocess_artifact["payload"]
    values = matrix_from_preprocess(preprocess_artifact)
    row_count = int(values.shape[0])
    if row_count != int(payload["row_count"]):
        raise WorkbenchError("E_DATA", "preprocessed matrix row count mismatch", stage="score")
    if int(declaration.get("rows", -1)) != row_count:
        raise WorkbenchError(
            "E_DATA",
            "declaration row count differs from preprocessed matrix",
            stage="score",
            details={"declared": declaration.get("rows"), "observed": row_count},
        )
    source_ids = [str(value) for value in payload["source_ids"]]
    if len(source_ids) != row_count:
        raise WorkbenchError("E_DATA", "source id count differs from score count", stage="score")

    scoring = config["scoring"]
    weights = scoring["weights"]
    for detector in ("covariance", "forest"):
        try:
            weight = float(weights[detector])
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                "E_NUMERIC",
                "detector weight is not numeric",
                stage="score",
                sample_id=source_ids[0] if source_ids else None,
                details={"detector": detector, "weight": weights[detector]},
            ) from exc
        if not np.isfinite(weight) or weight < 0.0:
            raise WorkbenchError(
                "E_NUMERIC",
                "detector weights must be finite and non-negative",
                stage="score",
                sample_id=source_ids[0] if source_ids else None,
                details={"detector": detector, "weight": weight, "weights": weights},
            )
    covariance_weight = float(weights["covariance"])
    forest_weight = float(weights["forest"])
    weight_sum = covariance_weight + forest_weight
    if not np.isfinite(weight_sum) or weight_sum <= 0.0:
        raise WorkbenchError(
            "E_NUMERIC",
            "at least one positive detector weight is required",
            stage="score",
            sample_id=source_ids[0] if source_ids else None,
            details={"weights": weights},
        )

    cov_raw = score_robust_covariance(values, fit_artifact["payload"]["covariance"])
    forest_raw = score_isolation_forest(values, fit_artifact["payload"]["forest"])
    for name, raw in (("covariance", cov_raw), ("forest", forest_raw)):
        invalid = np.flatnonzero(~np.isfinite(raw))
        if len(invalid):
            row = int(invalid[0])
            raise _bad_sample(
                row,
                raw,
                source_ids,
                f"{name} detector emitted NaN or Inf",
                {"detector": name},
            )

    normalization = scoring["normalization"]
    cov_normalized, cov_normalization = _normalize_detector(
        cov_raw,
        transform=normalization["covariance_transform"],
        center_quantile=float(normalization["center_quantile"]),
        scale_quantiles=[float(value) for value in normalization["scale_quantiles"]],
        scale_floor=float(normalization["scale_floor"]),
        clip=float(normalization["clip"]),
    )
    forest_normalized, forest_normalization = _normalize_detector(
        forest_raw,
        transform=normalization["forest_transform"],
        center_quantile=float(normalization["center_quantile"]),
        scale_quantiles=[float(value) for value in normalization["scale_quantiles"]],
        scale_floor=float(normalization["scale_floor"]),
        clip=float(normalization["clip"]),
    )
    combined = (
        (covariance_weight / weight_sum) * cov_normalized
        + (forest_weight / weight_sum) * forest_normalized
    )
    if np.any(np.isnan(combined)) or np.any(np.isinf(combined)):
        invalid = np.flatnonzero(~np.isfinite(combined))
        row = int(invalid[0])
        raise _bad_sample(
            row,
            combined,
            source_ids,
            "combined anomaly score contains NaN or Inf",
            {
                "covariance_weight": covariance_weight,
                "forest_weight": forest_weight,
                "covariance_normalized": float(cov_normalized[row]),
                "forest_normalized": float(forest_normalized[row]),
            },
        )
    if not np.isfinite(np.min(combined)) or not np.isfinite(np.max(combined)):
        raise WorkbenchError(
            "E_NUMERIC",
            "combined score range is not finite",
            stage="score",
            details={"combined_min": float(np.min(combined)), "combined_max": float(np.max(combined))},
        )

    threshold_quantile = float(scoring["threshold_quantile"])
    if not np.isfinite(threshold_quantile) or not (0.0 < threshold_quantile < 1.0):
        raise WorkbenchError(
            "E_INVALID_CONFIG",
            "threshold quantile must be strictly between zero and one",
            stage="score",
            details={"threshold_quantile": threshold_quantile},
        )
    threshold = _quantile(combined, threshold_quantile)
    if scoring["comparison"] == "greater_equal":
        decisions = combined >= threshold
    elif scoring["comparison"] == "greater_than":
        decisions = combined > threshold
    else:
        raise WorkbenchError("E_INVALID_CONFIG", "unsupported threshold comparison", stage="score")
    decision_values = [1 if bool(value) else 0 for value in decisions.tolist()]
    return {
        "tier": payload["tier"],
        "row_count": row_count,
        "column_count": int(values.shape[1]),
        "source_ids": source_ids,
        "batch_ids": [int(value) for value in payload["batch_ids"]],
        "raw_scores": {
            "covariance": jsonable_array(cov_raw),
            "forest": jsonable_array(forest_raw),
        },
        "normalization": {
            "covariance": cov_normalization,
            "forest": forest_normalization,
            "domain": "per-detector log/logit transformed robust standardization",
        },
        "normalized_scores": {
            "covariance": jsonable_array(cov_normalized),
            "forest": jsonable_array(forest_normalized),
        },
        "weights": {
            "configured": {
                "covariance": covariance_weight,
                "forest": forest_weight,
            },
            "effective": {
                "covariance": covariance_weight / weight_sum,
                "forest": forest_weight / weight_sum,
            },
        },
        "combined_scores": jsonable_array(combined),
        "threshold": {
            "quantile": threshold_quantile,
            "value": float(threshold),
            "comparison": scoring["comparison"],
            "method": "empirical_quantile_linear",
        },
        "decisions": decision_values,
        "detected_count": int(sum(decision_values)),
        "detection_rate": float(sum(decision_values) / row_count),
        "numeric_guards": {
            "nan_or_inf": False,
            "negative_weights": False,
            "weights_sum": weight_sum,
        },
        "fit_artifact_digest": fit_artifact.get("artifact_digest"),
        "preprocess_artifact_digest": preprocess_artifact.get("artifact_digest"),
    }
