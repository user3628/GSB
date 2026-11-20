"""Small deterministic histogram regression-tree implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from .config import Config
from .errors import WorkbenchError
from .util import float_to_hex


@dataclass
class GBDTModel:
    base_score: float
    trees: list[dict[str, Any]]

    def to_payload(self) -> dict[str, Any]:
        return {"base_score_hex": float_to_hex(self.base_score), "trees": self.trees}

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "GBDTModel":
        return cls(base_score=float.fromhex(payload["base_score_hex"]), trees=payload["trees"])


def _leaf_value(g_sum: float, h_sum: float, l2: float, learning_rate: float) -> float:
    return float(-(g_sum / (h_sum + l2)) * learning_rate)


def _leaf(g_sum: float, h_sum: float, config: Config, sample_count: int) -> dict[str, Any]:
    return {
        "kind": "leaf",
        "value_hex": float_to_hex(_leaf_value(g_sum, h_sum, config.l2, config.learning_rate)),
        "samples": int(sample_count),
    }


def _fit_node(
    binned: np.ndarray,
    gradients: np.ndarray,
    hessians: np.ndarray,
    indices: np.ndarray,
    depth: int,
    config: Config,
    histogram_bins: int,
) -> dict[str, Any]:
    g_node = gradients[indices]
    h_node = hessians[indices]
    g_total = float(np.sum(g_node, dtype=np.float64))
    h_total = float(np.sum(h_node, dtype=np.float64))
    if depth >= config.max_depth or indices.size < config.min_leaf * 2:
        return _leaf(g_total, h_total, config, indices.size)

    best_gain = -1.0
    best_feature = -1
    best_bin = -1
    parent_denominator = h_total + config.l2
    parent_score = (g_total * g_total) / parent_denominator if parent_denominator > 0 else 0.0

    for feature in range(binned.shape[1]):
        feature_bins = binned[indices, feature]
        counts = np.bincount(feature_bins, minlength=histogram_bins).astype(np.float64, copy=False)
        g_hist = np.bincount(feature_bins, weights=g_node, minlength=histogram_bins)
        h_hist = np.bincount(feature_bins, weights=h_node, minlength=histogram_bins)
        count_left = np.cumsum(counts)[:-1]
        count_right = float(indices.size) - count_left
        valid = (count_left >= config.min_leaf) & (count_right >= config.min_leaf)
        if not np.any(valid):
            continue
        g_left = np.cumsum(g_hist)[:-1]
        h_left = np.cumsum(h_hist)[:-1]
        g_right = g_total - g_left
        h_right = h_total - h_left
        left_denominator = h_left + config.l2
        right_denominator = h_right + config.l2
        gains = (g_left * g_left) / left_denominator + (g_right * g_right) / right_denominator - parent_score
        gains = np.where(valid, gains, -np.inf)
        bin_index = int(np.argmax(gains))
        gain = float(gains[bin_index])
        if gain > best_gain + 1e-15:
            best_gain = gain
            best_feature = feature
            best_bin = bin_index

    if best_feature < 0 or best_gain <= 1e-12:
        return _leaf(g_total, h_total, config, indices.size)

    split_mask = binned[indices, best_feature] <= best_bin
    left_indices = indices[split_mask]
    right_indices = indices[~split_mask]
    if left_indices.size < config.min_leaf or right_indices.size < config.min_leaf:
        return _leaf(g_total, h_total, config, indices.size)
    return {
        "kind": "split",
        "feature": int(best_feature),
        "bin": int(best_bin),
        "gain_hex": float_to_hex(best_gain),
        "samples": int(indices.size),
        "left": _fit_node(binned, gradients, hessians, left_indices, depth + 1, config, histogram_bins),
        "right": _fit_node(binned, gradients, hessians, right_indices, depth + 1, config, histogram_bins),
    }


def fit_tree(
    binned: np.ndarray,
    gradients: np.ndarray,
    hessians: np.ndarray,
    config: Config,
    *,
    indices: np.ndarray | None = None,
) -> dict[str, Any]:
    binned = np.asarray(binned)
    gradients = np.asarray(gradients, dtype=np.float64)
    hessians = np.asarray(hessians, dtype=np.float64)
    if binned.ndim != 2 or gradients.shape != (binned.shape[0],) or hessians.shape != gradients.shape:
        raise WorkbenchError("E_INVALID_PARAM", "tree training arrays have incompatible shapes", {"stage": "boosting"})
    if indices is None:
        indices = np.arange(binned.shape[0], dtype=np.int64)
    histogram_bins = config.bins + 1
    return _fit_node(binned, gradients, hessians, np.asarray(indices, dtype=np.int64), 0, config, histogram_bins)


def _apply_tree(tree: dict[str, Any], binned: np.ndarray) -> np.ndarray:
    output = np.zeros(binned.shape[0], dtype=np.float64)

    def visit(node: dict[str, Any], indices: np.ndarray) -> None:
        if indices.size == 0:
            return
        if node["kind"] == "leaf":
            output[indices] = float.fromhex(node["value_hex"])
            return
        feature = int(node["feature"])
        threshold = int(node["bin"])
        mask = binned[indices, feature] <= threshold
        visit(node["left"], indices[mask])
        visit(node["right"], indices[~mask])

    visit(tree, np.arange(binned.shape[0], dtype=np.int64))
    return output


def predict_model_raw(model: GBDTModel, binned: np.ndarray) -> np.ndarray:
    binned = np.asarray(binned)
    raw = np.full(binned.shape[0], model.base_score, dtype=np.float64)
    for tree in model.trees:
        raw += _apply_tree(tree, binned)
    return raw


def _stable_sigmoid(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    output = np.empty_like(values)
    positive = values >= 0.0
    output[positive] = 1.0 / (1.0 + np.exp(-values[positive]))
    exp_values = np.exp(values[~positive])
    output[~positive] = exp_values / (1.0 + exp_values)
    return output


def base_score_for_labels(y: np.ndarray) -> float:
    y = np.asarray(y)
    positive = float(np.sum(y == 1, dtype=np.int64))
    total = int(y.size)
    probability = (positive + 1.0) / (total + 2.0)
    return float(np.log(probability / (1.0 - probability)))


def train_one_round(
    model: GBDTModel,
    raw_scores: np.ndarray,
    binned: np.ndarray,
    y: np.ndarray,
    config: Config,
) -> None:
    probability = _stable_sigmoid(raw_scores)
    gradients = probability - y.astype(np.float64)
    hessians = np.maximum(probability * (1.0 - probability), 1e-12)
    tree = fit_tree(binned, gradients, hessians, config)
    raw_scores += _apply_tree(tree, binned)
    model.trees.append(tree)


def fit_boosting_model(binned: np.ndarray, y: np.ndarray, config: Config) -> tuple[GBDTModel, np.ndarray]:
    config.require_training_ready()
    y = np.asarray(y, dtype=np.uint8)
    model = GBDTModel(base_score=base_score_for_labels(y), trees=[])
    raw_scores = np.full(y.shape[0], model.base_score, dtype=np.float64)
    for _ in range(config.rounds):
        train_one_round(model, raw_scores, binned, y, config)
    return model, raw_scores


def flatten_tree_values(node: dict[str, Any]) -> Iterable[float]:
    if node["kind"] == "leaf":
        yield float.fromhex(node["value_hex"])
        return
    yield from flatten_tree_values(node["left"])
    yield from flatten_tree_values(node["right"])
