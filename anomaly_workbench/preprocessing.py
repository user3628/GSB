"""Deterministic preprocessing with explicit column observations."""

from __future__ import annotations

from typing import Any

import numpy as np

from .data import TierData
from .errors import WorkbenchError
from .util import digest_object, jsonable_array


def _column_moments(column: np.ndarray) -> tuple[float, float]:
    finite = column[np.isfinite(column)]
    if len(finite) < 3:
        return 0.0, 0.0
    center = float(np.mean(finite))
    scale = float(np.std(finite, ddof=0))
    if scale <= 1e-15:
        return 0.0, 0.0
    standardized = (finite - center) / scale
    skew = float(np.mean(standardized**3))
    kurtosis = float(np.mean(standardized**4) - 3.0)
    return skew, kurtosis


def _fill_value(column: np.ndarray, strategy: str) -> float:
    finite = column[np.isfinite(column)]
    if len(finite) == 0:
        raise WorkbenchError("E_DATA", "a feature column contains no finite values", stage="preprocess")
    if strategy == "median":
        return float(np.median(finite))
    if strategy == "mean":
        return float(np.mean(finite))
    if strategy == "zero":
        return 0.0
    raise WorkbenchError("E_INVALID_CONFIG", "unsupported missing strategy", stage="preprocess")


def _robust_scale(column: np.ndarray, epsilon: float) -> tuple[float, float]:
    center = float(np.median(column))
    q25, q75 = np.quantile(column, [0.25, 0.75], method="linear")
    scale = float((q75 - q25) / 1.349)
    if scale <= epsilon:
        mad = float(1.4826 * np.median(np.abs(column - center)))
        scale = mad if mad > epsilon else float(np.std(column, ddof=0))
    if scale <= epsilon:
        scale = 1.0
    return center, scale


def _redundant_pairs(matrix: np.ndarray, threshold: float) -> list[list[int]]:
    if matrix.shape[1] < 2:
        return []
    with np.errstate(invalid="ignore", divide="ignore"):
        correlation = np.corrcoef(matrix, rowvar=False)
    pairs: list[list[int]] = []
    for first in range(matrix.shape[1]):
        for second in range(first + 1, matrix.shape[1]):
            value = correlation[first, second]
            if np.isfinite(value) and abs(float(value)) >= threshold:
                pairs.append([first, second])
    return pairs


def preprocess_tier(data: TierData, config: dict[str, Any]) -> dict[str, Any]:
    features = np.asarray(data.features, dtype=np.float64)
    if features.ndim != 2 or len(features) == 0:
        raise WorkbenchError("E_DATA", "feature matrix must be non-empty and two-dimensional", stage="preprocess")
    if np.any(np.isinf(features)):
        raise WorkbenchError("E_DATA", "feature matrix contains Inf", stage="preprocess")
    row_count, column_count = features.shape
    preprocessing = config["preprocessing"]
    epsilon = float(preprocessing["robust_epsilon"])
    missing_indices = sorted(
        {
            int(value)
            for value in data.declaration.get("missing_column_indices", [])
        }
        | {int(value) for value in np.flatnonzero(np.any(np.isnan(features), axis=0)).tolist()}
    )

    filled = np.array(features, copy=True)
    fill_values: list[float] = []
    missing_counts: list[int] = []
    column_observations: list[dict[str, Any]] = []
    for column_index in range(column_count):
        column = features[:, column_index]
        missing_count = int(np.count_nonzero(np.isnan(column)))
        fill = _fill_value(column, preprocessing["missing_strategy"])
        fill_values.append(fill)
        missing_counts.append(missing_count)
        if missing_count:
            filled[np.isnan(column), column_index] = fill
        skew, kurtosis = _column_moments(filled[:, column_index])
        column_observations.append(
            {
                "column_index": column_index,
                "continuous": True,
                "missing_count": missing_count,
                "missing_fraction": float(missing_count / row_count),
                "skewness": skew,
                "excess_kurtosis": kurtosis,
            }
        )

    if not np.all(np.isfinite(filled)):
        raise WorkbenchError("E_DATA", "missing-value filling left non-finite values", stage="preprocess")

    declared_heavy = {int(value) for value in data.declaration.get("heavy_tail_column_indices", [])}
    heavy_tail: set[int] = set(declared_heavy)
    for observation in column_observations:
        if (
            abs(float(observation["skewness"])) >= float(preprocessing["heavy_tail_abs_skew"])
            or float(observation["excess_kurtosis"]) >= float(preprocessing["heavy_tail_kurtosis"])
        ):
            heavy_tail.add(int(observation["column_index"]))
        observation["declared_heavy_tail"] = int(observation["column_index"]) in declared_heavy
        observation["heavy_tail"] = int(observation["column_index"]) in heavy_tail
        observation["missing"] = int(observation["column_index"]) in set(missing_indices)

    centers = np.empty(column_count, dtype=np.float64)
    scales = np.empty(column_count, dtype=np.float64)
    for column_index in range(column_count):
        column = filled[:, column_index]
        if preprocessing["standardization"] == "robust":
            center, scale = _robust_scale(column, epsilon)
        elif preprocessing["standardization"] == "zscore":
            center = float(np.mean(column))
            scale = float(np.std(column, ddof=0))
            if scale <= epsilon:
                scale = 1.0
        else:  # guarded by configuration validation
            raise WorkbenchError("E_INVALID_CONFIG", "unsupported standardization", stage="preprocess")
        centers[column_index] = center
        scales[column_index] = scale
        observation = column_observations[column_index]
        observation["center"] = float(center)
        observation["scale"] = float(scale)

    standardized = (filled - centers) / scales
    clip = float(preprocessing["clip_standardized"])
    standardized = np.clip(standardized, -clip, clip)
    if not np.all(np.isfinite(standardized)):
        bad_row = int(np.flatnonzero(np.any(~np.isfinite(standardized), axis=1))[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "standardized matrix contains NaN or Inf",
            stage="preprocess",
            sample_id=str(data.source_ids[bad_row]),
            details={"row": bad_row},
        )

    declared_redundant = [
        [int(group[0]), int(group[1])]
        for group in data.declaration.get("redundant_column_groups", [])
    ]
    measured_redundant = _redundant_pairs(
        standardized, float(preprocessing["redundancy_threshold"])
    )
    redundant_map = {tuple(pair) for pair in declared_redundant} | {
        tuple(pair) for pair in measured_redundant
    }
    redundant_groups = [list(pair) for pair in sorted(redundant_map)]

    payload = {
        "tier": data.tier,
        "row_count": int(row_count),
        "column_count": int(column_count),
        "source_ids": [str(value) for value in data.source_ids.tolist()],
        "batch_ids": [int(value) for value in data.batch_ids.tolist()],
        "values": jsonable_array(standardized),
        "fill_values": jsonable_array(fill_values),
        "center": jsonable_array(centers),
        "scale": jsonable_array(scales),
        "missing_strategy": preprocessing["missing_strategy"],
        "standardization": preprocessing["standardization"],
        "clip_standardized": clip,
        "missing_column_indices": missing_indices,
        "heavy_tail_column_indices": sorted(heavy_tail),
        "continuous_column_indices": list(range(column_count)),
        "redundant_column_groups": redundant_groups,
        "measured_redundant_column_groups": measured_redundant,
        "declared_redundant_column_groups": declared_redundant,
        "missing_counts": missing_counts,
        "column_observations": column_observations,
        "data_fingerprint": data.data_fingerprint,
        "declaration_digest": data.declaration_digest,
        "matrix_digest": digest_object({"values": jsonable_array(standardized)}),
    }
    return payload


def matrix_from_preprocess(artifact: dict[str, Any]) -> np.ndarray:
    values = np.asarray(artifact["payload"]["values"], dtype=np.float64)
    if values.ndim != 2:
        raise WorkbenchError("E_DATA", "preprocess artifact has invalid matrix shape", stage="preprocess")
    return values
