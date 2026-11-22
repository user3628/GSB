from __future__ import annotations

import math
from typing import Any

import numpy as np

from .config import validate_threshold
from .errors import WorkbenchError
from .util import (
    digest_object,
    stable_float,
    stable_matrix_digest,
    stable_vector_digest,
    to_jsonable,
)


def _entropy(counts: np.ndarray) -> float:
    total = float(np.sum(counts))
    if total <= 0:
        return 0.0
    probabilities = counts.astype(np.float64) / total
    probabilities = probabilities[probabilities > 0]
    return float(-np.sum(probabilities * np.log(probabilities)))


def mutual_information_score(feature: np.ndarray, labels: np.ndarray) -> float:
    count = int(feature.size)
    if count == 0:
        return 0.0
    unique = np.unique(feature)
    max_bins = min(16, max(2, int(math.sqrt(count))))
    if unique.size <= max_bins:
        bins = np.searchsorted(unique, feature).astype(np.int64)
    else:
        order = np.argsort(feature, kind="mergesort")
        ranks = np.empty(count, dtype=np.int64)
        ranks[order] = np.arange(count, dtype=np.int64)
        bins = (ranks * max_bins) // max(1, count)
    label_values = np.asarray(labels, dtype=np.int64)
    if label_values.min(initial=0) < 0 or label_values.max(initial=0) > 1:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            "Mutual information currently requires a binary 0/1 label",
        )
    bin_count = int(bins.max(initial=0)) + 1
    joint = np.bincount(
        bins * 2 + label_values,
        minlength=bin_count * 2,
    ).reshape(bin_count, 2).astype(np.float64)
    total = float(np.sum(joint))
    x_counts = np.sum(joint, axis=1)
    y_counts = np.sum(joint, axis=0)
    h_x = _entropy(x_counts)
    h_y = _entropy(y_counts)
    joint_probabilities = joint / total
    x_probabilities = x_counts / total
    y_probabilities = y_counts / total
    h_xy = 0.0
    for x_index in range(bin_count):
        for y_index in range(2):
            probability = float(joint_probabilities[x_index, y_index])
            if probability > 0:
                h_xy -= probability * math.log(probability)
    value = h_x + h_y - h_xy
    if value < 0 and value > -1e-12:
        value = 0.0
    return stable_float(max(0.0, value))


def _sorted_by_score(
    scores: dict[str, float],
    feature_names: list[str],
    *,
    descending: bool,
) -> list[str]:
    index = {name: position for position, name in enumerate(feature_names)}
    return sorted(
        feature_names,
        key=lambda name: ((-scores[name]) if descending else scores[name], index[name]),
    )


def _rfe(
    matrix: np.ndarray,
    labels: np.ndarray,
    feature_names: list[str],
    threshold: float,
) -> tuple[list[str], list[str], dict[str, float], list[dict[str, Any]]]:
    rows, feature_count = matrix.shape
    if feature_count == 0:
        return [], [], {}, []
    keep_count = max(1, int(math.ceil(float(threshold) * feature_count)))
    if keep_count >= feature_count:
        keep_count = feature_count

    means = np.mean(matrix, axis=0)
    standard_deviations = np.std(matrix, axis=0)
    standard_deviations[standard_deviations == 0.0] = 1.0
    standardized = (matrix - means) / standard_deviations
    centered_labels = labels.astype(np.float64) - float(np.mean(labels))
    gram = standardized.T @ standardized
    cross = standardized.T @ centered_labels

    active = list(range(feature_count))
    elimination: list[dict[str, Any]] = []
    coefficients_by_feature: dict[int, float] = {}
    while len(active) > keep_count:
        indices = np.asarray(active, dtype=np.int64)
        sub_gram = gram[np.ix_(indices, indices)].copy()
        sub_cross = cross[indices].copy()
        ridge = 1e-10 * max(1, rows)
        sub_gram.flat[:: sub_gram.shape[0] + 1] += ridge
        try:
            coefficients = np.linalg.solve(sub_gram, sub_cross)
        except np.linalg.LinAlgError:
            coefficients = np.linalg.lstsq(sub_gram, sub_cross, rcond=None)[0]
        importances = np.abs(coefficients)
        for feature_index, importance in zip(active, importances.tolist()):
            coefficients_by_feature[int(feature_index)] = stable_float(float(importance))
        min_index_position = min(
            range(len(active)),
            key=lambda position: (float(importances[position]), -active[position]),
        )
        eliminated_feature = active.pop(min_index_position)
        elimination.append(
            {
                "feature": feature_names[eliminated_feature],
                "importance": coefficients_by_feature[eliminated_feature],
                "remaining_after": len(active),
            }
        )

    indices = np.asarray(active, dtype=np.int64)
    if active:
        sub_gram = gram[np.ix_(indices, indices)].copy()
        sub_cross = cross[indices].copy()
        ridge = 1e-10 * max(1, rows)
        sub_gram.flat[:: sub_gram.shape[0] + 1] += ridge
        try:
            coefficients = np.linalg.solve(sub_gram, sub_cross)
        except np.linalg.LinAlgError:
            coefficients = np.linalg.lstsq(sub_gram, sub_cross, rcond=None)[0]
        for feature_index, importance in zip(active, np.abs(coefficients).tolist()):
            coefficients_by_feature[int(feature_index)] = stable_float(float(importance))

    selected_indices = set(active)
    selected = [name for index, name in enumerate(feature_names) if index in selected_indices]
    eliminated = [name for index, name in enumerate(feature_names) if index not in selected_indices]
    scores = {
        name: coefficients_by_feature.get(index, 0.0)
        for index, name in enumerate(feature_names)
    }
    return selected, eliminated, scores, elimination


def select_features(
    shard_results: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    if not shard_results:
        raise WorkbenchError("E_INPUT_INVALID", "No encoded shards are available for selection")
    ordered = sorted(shard_results, key=lambda item: int(item["summary"]["shard_index"]))
    feature_names = list(ordered[0]["summary"]["feature_names"])
    for result in ordered[1:]:
        names = list(result["summary"]["feature_names"])
        if names != feature_names:
            mismatch = next(
                (
                    (index, left, right)
                    for index, (left, right) in enumerate(zip(feature_names, names))
                    if left != right
                ),
                None,
            )
            raise WorkbenchError(
                "E_COLUMN_MISMATCH",
                "Encoded feature names are not aligned across shards",
                {
                    "column_index": mismatch[0] if mismatch else None,
                    "declared": mismatch[1] if mismatch else feature_names,
                    "observed": mismatch[2] if mismatch else names,
                    "shard_index": result["summary"]["shard_index"],
                },
            )
    matrix = np.concatenate([np.asarray(result["matrix"]) for result in ordered], axis=0)
    labels = np.concatenate([np.asarray(result["labels"]) for result in ordered], axis=0)
    method = config["selection"]["method"]
    threshold = validate_threshold(method, config["selection"]["threshold"])
    rows, feature_count = matrix.shape

    elimination: list[dict[str, Any]] = []
    score_basis = ""
    if feature_count == 0:
        selected: list[str] = []
        eliminated: list[str] = []
        scores: dict[str, float] = {}
        ranking: list[str] = []
        score_basis = "no_encoded_features"
    elif method == "variance":
        variance = np.var(matrix, axis=0, ddof=0)
        scores = {
            name: stable_float(float(variance[index]))
            for index, name in enumerate(feature_names)
        }
        selected = [name for name in feature_names if scores[name] >= threshold]
        eliminated = [name for name in feature_names if scores[name] < threshold]
        ranking = _sorted_by_score(scores, feature_names, descending=True)
        score_basis = "population_variance"
    elif method == "mutual_info":
        scores = {
            name: mutual_information_score(matrix[:, index], labels)
            for index, name in enumerate(feature_names)
        }
        selected = [name for name in feature_names if scores[name] >= threshold]
        eliminated = [name for name in feature_names if scores[name] < threshold]
        ranking = _sorted_by_score(scores, feature_names, descending=True)
        score_basis = "empirical_mutual_information_binary_label"
    elif method == "rfe":
        selected, eliminated, scores, elimination = _rfe(
            matrix,
            labels,
            feature_names,
            threshold,
        )
        ranking = _sorted_by_score(scores, feature_names, descending=True)
        score_basis = "ridge_linear_coefficient_absolute_importance"
    else:
        raise WorkbenchError(
            "E_INVALID_THRESHOLD",
            "Unknown feature selection method",
            {"method": method, "threshold": threshold},
        )

    result: dict[str, Any] = {
        "schema_version": 1,
        "tier": ordered[0]["summary"]["tier"],
        "method": method,
        "threshold": threshold,
        "score_basis": score_basis,
        "rows": int(rows),
        "encoded_feature_count": int(feature_count),
        "feature_names": feature_names,
        "shards": [int(result["summary"]["shard_index"]) for result in ordered],
        "input_matrix_digest": stable_matrix_digest(matrix),
        "label_digest": stable_vector_digest(labels),
        "selected": selected,
        "eliminated": eliminated,
        "scores": scores,
        "ranking": ranking,
        "elimination_order": elimination,
    }
    result = to_jsonable(result)
    result["selection_digest"] = digest_object(result)
    return result


def project_matrix(matrix: np.ndarray, selection: dict[str, Any]) -> np.ndarray:
    selected = list(selection["selected"])
    feature_names = list(selection.get("feature_names", []))
    if not feature_names:
        raise WorkbenchError(
            "E_STATE_INVALID",
            "Selection projection requires the source feature order",
        )
    positions = {name: index for index, name in enumerate(feature_names)}
    indices = [positions[name] for name in selected]
    return np.ascontiguousarray(matrix[:, indices], dtype=np.float64)

