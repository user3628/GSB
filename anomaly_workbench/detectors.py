"""Deterministic robust-covariance and isolation-forest detectors."""

from __future__ import annotations

import hashlib
import math
from typing import Any

import numpy as np

from .errors import WorkbenchError
from .util import digest_object, jsonable_array

EULER_GAMMA = 0.5772156649015329


def _numeric_error(message: str, details: dict[str, Any] | None = None) -> WorkbenchError:
    return WorkbenchError("E_NUMERIC", message, stage="score", details=details or {})


def _derive_tree_seed(base_seed: int, tree_index: int) -> int:
    payload = f"isolation-forest:{int(base_seed)}:{int(tree_index)}".encode("ascii")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "little", signed=False)


def average_path_length(size: int) -> float:
    if size <= 1:
        return 0.0
    if size == 2:
        return 1.0
    harmonic = math.log(size - 1.0) + EULER_GAMMA
    return 2.0 * harmonic - 2.0 * (size - 1.0) / size


def fit_robust_covariance(values: np.ndarray, config: dict[str, Any]) -> dict[str, Any]:
    data = np.asarray(values, dtype=np.float64)
    if data.ndim != 2 or len(data) < 2:
        raise WorkbenchError("E_DATA", "covariance fitting requires at least two rows", stage="fit")
    if not np.all(np.isfinite(data)):
        raise _numeric_error("preprocessed matrix contains non-finite values before covariance fit")
    center = np.median(data, axis=0)
    deviation = np.abs(data - center)
    robust_scale = 1.4826 * np.median(deviation, axis=0)
    fallback = np.std(data, axis=0, ddof=0)
    epsilon = float(config.get("epsilon", 1e-12))
    robust_scale = np.where(robust_scale > epsilon, robust_scale, np.where(fallback > epsilon, fallback, 1.0))
    winsor = float(config["winsor_limit"])
    clipped = np.clip((data - center) / robust_scale, -winsor, winsor)
    denominator = max(1, len(data) - 1)
    covariance = (clipped.T @ clipped) / denominator
    covariance = 0.5 * (covariance + covariance.T)
    dimensions = covariance.shape[0]
    shrinkage = float(config["shrinkage"])
    target = (float(np.trace(covariance)) / dimensions) * np.eye(dimensions)
    covariance = (1.0 - shrinkage) * covariance + shrinkage * target
    covariance = covariance + epsilon * np.eye(dimensions)
    try:
        precision = np.linalg.inv(covariance)
    except np.linalg.LinAlgError as exc:
        raise WorkbenchError("E_DATA", f"covariance matrix is singular: {exc}", stage="fit") from exc
    if not np.all(np.isfinite(precision)):
        raise _numeric_error("covariance precision matrix is not finite")
    sign, logdet = np.linalg.slogdet(covariance)
    if sign <= 0 or not np.isfinite(logdet):
        raise WorkbenchError("E_DATA", "covariance matrix is not positive definite", stage="fit")
    eigenvalues = np.linalg.eigvalsh(covariance)
    return {
        "method": "winsorized_robust_covariance_shrinkage",
        "center": jsonable_array(center),
        "robust_scale": jsonable_array(robust_scale),
        "winsor_limit": winsor,
        "shrinkage": shrinkage,
        "covariance": jsonable_array(covariance),
        "precision": jsonable_array(precision),
        "log_determinant": float(logdet),
        "trace": float(np.trace(covariance)),
        "min_eigenvalue": float(eigenvalues[0]),
        "max_eigenvalue": float(eigenvalues[-1]),
        "condition_number": float(eigenvalues[-1] / max(eigenvalues[0], epsilon)),
        "row_count": int(len(data)),
        "column_count": int(data.shape[1]),
    }


def score_robust_covariance(values: np.ndarray, model: dict[str, Any]) -> np.ndarray:
    data = np.asarray(values, dtype=np.float64)
    center = np.asarray(model["center"], dtype=np.float64)
    scale = np.asarray(model["robust_scale"], dtype=np.float64)
    precision = np.asarray(model["precision"], dtype=np.float64)
    if not (np.all(np.isfinite(data)) and np.all(np.isfinite(center)) and np.all(np.isfinite(scale))):
        raise _numeric_error("non-finite value encountered while computing Mahalanobis scores")
    clipped = np.clip((data - center) / scale, -float(model["winsor_limit"]), float(model["winsor_limit"]))
    quadratic = np.einsum("ij,jk,ik->i", clipped, precision, clipped, optimize=True)
    if np.any(quadratic < -1e-8) or not np.all(np.isfinite(quadratic)):
        bad = int(np.flatnonzero(~np.isfinite(quadratic))[0]) if np.any(~np.isfinite(quadratic)) else int(np.argmin(quadratic))
        raise _numeric_error(
            "Mahalanobis quadratic form is invalid",
            {"row": bad, "value": float(quadratic[bad])},
        )
    scores = np.sqrt(np.maximum(quadratic, 0.0))
    if not np.all(np.isfinite(scores)):
        raise _numeric_error("Mahalanobis score vector contains NaN or Inf")
    return scores


def _build_tree(
    values: np.ndarray,
    indices: np.ndarray,
    *,
    rng: np.random.Generator,
    max_depth: int,
    depth: int,
    nodes: list[list[float | int]],
) -> dict[str, Any]:
    node_index = len(nodes)
    nodes.append([-1, 0.0, -1, -1, depth, int(len(indices))])
    if depth >= max_depth or len(indices) <= 1:
        return {"leaf": True, "index": node_index, "depth": depth, "size": int(len(indices))}

    feature_count = values.shape[1]
    order = rng.permutation(feature_count)
    chosen_feature = -1
    split_value = 0.0
    left_indices = np.empty(0, dtype=np.int64)
    right_indices = np.empty(0, dtype=np.int64)
    for feature in order:
        column = values[indices, int(feature)]
        low = float(np.min(column))
        high = float(np.max(column))
        if not high > low:
            continue
        candidate = low + (high - low) * float(rng.random())
        if candidate <= low:
            candidate = np.nextafter(low, high)
        if candidate >= high:
            candidate = np.nextafter(high, low)
        mask = column <= candidate
        left = indices[mask]
        right = indices[~mask]
        if len(left) == 0 or len(right) == 0:
            continue
        chosen_feature = int(feature)
        split_value = float(candidate)
        left_indices = left.astype(np.int64, copy=False)
        right_indices = right.astype(np.int64, copy=False)
        break

    if chosen_feature < 0:
        return {"leaf": True, "index": node_index, "depth": depth, "size": int(len(indices))}

    left_info = _build_tree(
        values,
        left_indices,
        rng=rng,
        max_depth=max_depth,
        depth=depth + 1,
        nodes=nodes,
    )
    right_info = _build_tree(
        values,
        right_indices,
        rng=rng,
        max_depth=max_depth,
        depth=depth + 1,
        nodes=nodes,
    )
    nodes[node_index] = [
        chosen_feature,
        split_value,
        int(left_info["index"]),
        int(right_info["index"]),
        depth,
        int(len(indices)),
    ]
    return {
        "leaf": False,
        "index": node_index,
        "depth": depth,
        "size": int(len(indices)),
        "left": left_info,
        "right": right_info,
    }


def _walk_tree_info(info: dict[str, Any], leaf_depths: list[int], leaf_sizes: list[int], depth_counter: list[int]) -> None:
    depth_counter[0] = max(depth_counter[0], int(info["depth"]))
    if info["leaf"]:
        leaf_depths.append(int(info["depth"]))
        leaf_sizes.append(int(info["size"]))
        return
    _walk_tree_info(info["left"], leaf_depths, leaf_sizes, depth_counter)
    _walk_tree_info(info["right"], leaf_depths, leaf_sizes, depth_counter)


def fit_isolation_forest(values: np.ndarray, config: dict[str, Any]) -> dict[str, Any]:
    data = np.asarray(values, dtype=np.float64)
    if data.ndim != 2 or len(data) < 2:
        raise WorkbenchError("E_DATA", "forest fitting requires at least two rows", stage="fit")
    if not np.all(np.isfinite(data)):
        raise _numeric_error("preprocessed matrix contains non-finite values before forest fit")
    row_count = len(data)
    sample_size = min(int(config["sample_size"]), row_count)
    if sample_size < 2:
        raise WorkbenchError("E_DATA", "forest sample size must be at least two", stage="fit")
    trees: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for tree_index in range(int(config["n_estimators"])):
        rng = np.random.Generator(np.random.PCG64(_derive_tree_seed(int(config["seed"]), tree_index)))
        indices = rng.choice(row_count, size=sample_size, replace=False).astype(np.int64, copy=False)
        nodes: list[list[float | int]] = []
        info = _build_tree(
            data,
            indices,
            rng=rng,
            max_depth=int(config["max_depth"]),
            depth=0,
            nodes=nodes,
        )
        leaf_depths: list[int] = []
        leaf_sizes: list[int] = []
        depth_counter = [0]
        _walk_tree_info(info, leaf_depths, leaf_sizes, depth_counter)
        summary = {
            "tree_index": tree_index,
            "derived_seed": _derive_tree_seed(int(config["seed"]), tree_index),
            "node_count": len(nodes),
            "max_depth": int(depth_counter[0]),
            "leaf_count": len(leaf_sizes),
            "leaf_size_min": int(min(leaf_sizes)),
            "leaf_size_max": int(max(leaf_sizes)),
            "leaf_size_mean": float(sum(leaf_sizes) / len(leaf_sizes)),
            "leaf_depth_mean": float(sum(leaf_depths) / len(leaf_depths)),
        }
        trees.append({"nodes": nodes, "summary": summary})
        summaries.append(summary)
    return {
        "method": "deterministic_isolation_forest",
        "seed": int(config["seed"]),
        "n_estimators": int(config["n_estimators"]),
        "requested_sample_size": int(config["sample_size"]),
        "effective_sample_size": int(sample_size),
        "max_depth": int(config["max_depth"]),
        "row_count": row_count,
        "column_count": int(data.shape[1]),
        "trees": trees,
        "tree_summaries": summaries,
        "summary_digest": digest_object(summaries),
    }


def _tree_path_length(nodes: list[list[float | int]], row: np.ndarray) -> float:
    node_index = 0
    path = 0
    while True:
        node = nodes[node_index]
        feature = int(node[0])
        if feature < 0:
            leaf_size = int(node[5])
            return float(path) + average_path_length(leaf_size)
        threshold = float(node[1])
        node_index = int(node[2]) if row[feature] <= threshold else int(node[3])
        path += 1


def score_isolation_forest(values: np.ndarray, model: dict[str, Any]) -> np.ndarray:
    data = np.asarray(values, dtype=np.float64)
    if not np.all(np.isfinite(data)):
        raise _numeric_error("non-finite value encountered while traversing isolation forest")
    denominator = average_path_length(int(model["effective_sample_size"]))
    if denominator <= 0:
        raise _numeric_error("forest normalization path length is invalid")
    scores = np.empty(len(data), dtype=np.float64)
    for row_index, row in enumerate(data):
        path_sum = 0.0
        for tree in model["trees"]:
            path_sum += _tree_path_length(tree["nodes"], row)
        mean_path = path_sum / len(model["trees"])
        scores[row_index] = 2.0 ** (-mean_path / denominator)
    if not np.all(np.isfinite(scores)):
        raise _numeric_error("isolation-forest score vector contains NaN or Inf")
    return scores


def fit_detectors(values: np.ndarray, detector_config: dict[str, Any]) -> dict[str, Any]:
    covariance = fit_robust_covariance(values, detector_config["covariance"])
    forest = fit_isolation_forest(values, detector_config["forest"])
    return {"covariance": covariance, "forest": forest}


def detector_summary(models: dict[str, Any]) -> dict[str, Any]:
    covariance = models["covariance"]
    forest = models["forest"]
    return {
        "covariance": {
            "method": covariance["method"],
            "condition_number": covariance["condition_number"],
            "log_determinant": covariance["log_determinant"],
            "row_count": covariance["row_count"],
            "column_count": covariance["column_count"],
        },
        "forest": {
            "method": forest["method"],
            "seed": forest["seed"],
            "n_estimators": forest["n_estimators"],
            "effective_sample_size": forest["effective_sample_size"],
            "max_depth": forest["max_depth"],
            "summary_digest": forest["summary_digest"],
            "trees": forest["tree_summaries"],
        },
    }
