"""Log-domain dual logistic regression with an ESMM-style joint objective."""

from __future__ import annotations

import hashlib
import math
import traceback
from concurrent.futures import ProcessPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import ErrorCode
from .util import canonical_hash


MODEL_FORMAT = "ctr-cvr-fixed-point-v1"


def log_sigmoid(logit: np.ndarray) -> np.ndarray:
    """Stable log(sigmoid(logit)) without evaluating log(sigmoid)."""

    values = np.asarray(logit, dtype=np.float64)
    output = np.empty_like(values)
    positive = values >= 0.0
    output[positive] = -np.log1p(np.exp(-values[positive]))
    negative = ~positive
    output[negative] = values[negative] - np.log1p(np.exp(values[negative]))
    return output


def log1mexp(log_value: np.ndarray) -> np.ndarray:
    """Stable log(1-exp(log_value)) for log_value <= 0."""

    values = np.asarray(log_value, dtype=np.float64)
    output = np.empty_like(values)
    threshold = -math.log(2.0)
    high = values > threshold
    low = ~high
    output[high] = np.log(-np.expm1(values[high]))
    output[low] = np.log1p(-np.exp(values[low]))
    return output


def quantize_values(values: np.ndarray, scale: int) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    magnitudes = np.floor(np.abs(array) * float(scale) + 0.5)
    quantized = np.copysign(magnitudes, array).astype(np.int64)
    return quantized


def dequantize_values(values: Any, scale: int) -> np.ndarray:
    return np.asarray(values, dtype=np.int64).astype(np.float64) / float(scale)


@dataclass
class DualParams:
    ctr_weights: np.ndarray
    ctr_bias: float
    cvr_weights: np.ndarray
    cvr_bias: float

    def copy(self) -> "DualParams":
        return DualParams(
            ctr_weights=self.ctr_weights.copy(),
            ctr_bias=float(self.ctr_bias),
            cvr_weights=self.cvr_weights.copy(),
            cvr_bias=float(self.cvr_bias),
        )

    def fixed_model(self, feature_dim: int, scale: int) -> dict[str, Any]:
        return {
            "format": MODEL_FORMAT,
            "feature_dimension": int(feature_dim),
            "scale": int(scale),
            "quantization": "symmetric-round-half-away-from-zero-int64",
            "ctr": {
                "weights": [int(value) for value in quantize_values(self.ctr_weights, scale)],
                "bias": int(quantize_values(np.asarray([self.ctr_bias]), scale)[0]),
            },
            "cvr": {
                "weights": [int(value) for value in quantize_values(self.cvr_weights, scale)],
                "bias": int(quantize_values(np.asarray([self.cvr_bias]), scale)[0]),
            },
        }

    def fingerprint(self, feature_dim: int, scale: int) -> str:
        return canonical_hash(self.fixed_model(feature_dim, scale))

    @classmethod
    def from_fixed_model(cls, payload: dict[str, Any]) -> "DualParams":
        scale = int(payload["scale"])
        return cls(
            ctr_weights=dequantize_values(payload["ctr"]["weights"], scale),
            ctr_bias=float(payload["ctr"]["bias"]) / float(scale),
            cvr_weights=dequantize_values(payload["cvr"]["weights"], scale),
            cvr_bias=float(payload["cvr"]["bias"]) / float(scale),
        )


@dataclass
class ShardTrainData:
    shard_id: str
    shard_index: int
    impression_ids: list[str]
    row_numbers: list[int]
    features: np.ndarray
    ctr_labels: np.ndarray
    cvr_labels: np.ndarray
    cvr_observed: np.ndarray
    combo_targets: np.ndarray
    combo_mask: np.ndarray

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "shard_index": self.shard_index,
            "rows": len(self.impression_ids),
            "ctr_positive_count": int(self.ctr_labels.sum()),
            "cvr_observed_count": int(self.cvr_observed.sum()),
            "combo_observed_count": int(self.combo_mask.sum()),
        }


def initial_params(
    feature_dim: int,
    ctr_rate: float,
    cvr_rate: float,
    scale: int,
) -> DualParams:
    def logit(rate: float, count: int) -> float:
        smoothed = (rate * max(count, 1) + 0.5) / (max(count, 1) + 1.0)
        smoothed = min(max(smoothed, 1e-6), 1.0 - 1e-6)
        return math.log(smoothed / (1.0 - smoothed))

    params = DualParams(
        ctr_weights=np.zeros(feature_dim, dtype=np.float64),
        ctr_bias=logit(ctr_rate, 200),
        cvr_weights=np.zeros(feature_dim, dtype=np.float64),
        cvr_bias=logit(cvr_rate, 100),
    )
    return quantize_params(params, scale)


def quantize_params(params: DualParams, scale: int) -> DualParams:
    def quantize(values: np.ndarray) -> np.ndarray:
        return dequantize_values(quantize_values(values, scale), scale)

    return DualParams(
        ctr_weights=quantize(params.ctr_weights),
        ctr_bias=float(quantize(np.asarray([params.ctr_bias]))[0]),
        cvr_weights=quantize(params.cvr_weights),
        cvr_bias=float(quantize(np.asarray([params.cvr_bias]))[0]),
    )


def traversal_order(shard_id: str, impression_ids: list[str], seed: int, epoch: int) -> list[int]:
    """Hash-keyed shuffle: reproducible across processes and Python hash seeds."""

    keyed: list[tuple[bytes, int]] = []
    prefix = f"{seed}|{epoch}|{shard_id}|".encode("utf-8")
    for position, impression_id in enumerate(impression_ids):
        digest = hashlib.sha256(prefix + impression_id.encode("utf-8")).digest()
        keyed.append((digest, position))
    keyed.sort(key=lambda item: (item[0], item[1]))
    return [position for _, position in keyed]


def _train_epoch_shard(task: dict[str, Any]) -> dict[str, Any]:
    """Top-level process-pool worker. Never lets an exception escape unstructured."""

    shard_id = task.get("shard_id")
    try:
        order = np.asarray(task["order"], dtype=np.int64)
        matrix = np.asarray(task["features"], dtype=np.float64)[order]
        ctr_labels = np.asarray(task["ctr_labels"], dtype=np.float64)[order]
        cvr_labels = np.asarray(task["cvr_labels"], dtype=np.float64)[order]
        cvr_observed = np.asarray(task["cvr_observed"], dtype=bool)[order]
        combo_targets = np.asarray(task["combo_targets"], dtype=np.float64)[order]
        combo_mask = np.asarray(task["combo_mask"], dtype=bool)[order]
        params = task["params"]

        z_ctr = matrix @ params.ctr_weights + float(params.ctr_bias)
        z_cvr = matrix @ params.cvr_weights + float(params.cvr_bias)
        log_p_ctr = log_sigmoid(z_ctr)
        log_p_cvr = log_sigmoid(z_cvr)
        p_ctr = np.exp(log_p_ctr)
        p_cvr = np.exp(log_p_cvr)

        log_one_minus_ctr = log1mexp(log_p_ctr)
        ctr_loss_values = -(ctr_labels * log_p_ctr + (1.0 - ctr_labels) * log_one_minus_ctr)
        ctr_loss_sum = float(np.sum(ctr_loss_values, dtype=np.float64))
        ctr_residual = p_ctr - ctr_labels
        ctr_grad_weights = np.einsum("ij,i->j", matrix, ctr_residual, optimize=False)
        ctr_grad_bias = float(np.sum(ctr_residual, dtype=np.float64))

        exposed_observed = (ctr_labels > 0.5) & cvr_observed
        cvr_count = int(np.sum(exposed_observed, dtype=np.int64))
        if cvr_count:
            cvr_matrix = matrix[exposed_observed]
            cvr_target = cvr_labels[exposed_observed]
            cvr_log_prob = log_p_cvr[exposed_observed]
            cvr_log_one_minus = log1mexp(cvr_log_prob)
            cvr_loss_sum = float(
                -np.sum(cvr_target * cvr_log_prob + (1.0 - cvr_target) * cvr_log_one_minus, dtype=np.float64)
            )
            cvr_residual = p_cvr[exposed_observed] - cvr_target
            cvr_grad_weights = np.einsum("ij,i->j", cvr_matrix, cvr_residual, optimize=False)
            cvr_grad_bias = float(np.sum(cvr_residual, dtype=np.float64))
        else:
            cvr_loss_sum = 0.0
            cvr_grad_weights = np.zeros_like(params.cvr_weights)
            cvr_grad_bias = 0.0

        combo_count = int(np.sum(combo_mask, dtype=np.int64))
        if combo_count:
            combo_matrix = matrix[combo_mask]
            target = combo_targets[combo_mask]
            log_q = log_p_ctr[combo_mask] + log_p_cvr[combo_mask]
            log_one_minus_q = log1mexp(log_q)
            combo_loss_sum = float(
                -np.sum(target * log_q + (1.0 - target) * log_one_minus_q, dtype=np.float64)
            )
            coefficient = np.empty_like(log_q)
            positive_target = target > 0.5
            coefficient[positive_target] = -1.0
            negative_target = ~positive_target
            coefficient[negative_target] = np.exp(log_q[negative_target]) / (
                -np.expm1(log_q[negative_target])
            )
            ctr_combo_residual = coefficient * (1.0 - p_cvr[combo_mask])
            cvr_combo_residual = coefficient * (1.0 - p_ctr[combo_mask])
            combo_ctr_grad_weights = np.einsum(
                "ij,i->j", combo_matrix, ctr_combo_residual, optimize=False
            )
            combo_ctr_grad_bias = float(np.sum(ctr_combo_residual, dtype=np.float64))
            combo_cvr_grad_weights = np.einsum(
                "ij,i->j", combo_matrix, cvr_combo_residual, optimize=False
            )
            combo_cvr_grad_bias = float(np.sum(cvr_combo_residual, dtype=np.float64))
        else:
            combo_loss_sum = 0.0
            combo_ctr_grad_weights = np.zeros_like(params.ctr_weights)
            combo_cvr_grad_weights = np.zeros_like(params.cvr_weights)
            combo_ctr_grad_bias = 0.0
            combo_cvr_grad_bias = 0.0

        for name, value in (
            ("ctr_loss_sum", ctr_loss_sum),
            ("cvr_loss_sum", cvr_loss_sum),
            ("combo_loss_sum", combo_loss_sum),
            ("ctr_grad_bias", ctr_grad_bias),
            ("cvr_grad_bias", cvr_grad_bias),
        ):
            if not math.isfinite(float(value)):
                raise FloatingPointError(f"{name} is not finite")
        if not np.isfinite(ctr_grad_weights).all() or not np.isfinite(cvr_grad_weights).all():
            raise FloatingPointError("gradient contains non-finite values")
        if not np.isfinite(combo_ctr_grad_weights).all() or not np.isfinite(combo_cvr_grad_weights).all():
            raise FloatingPointError("combo gradient contains non-finite values")

        return {
            "ok": True,
            "shard_id": shard_id,
            "shard_index": int(task["shard_index"]),
            "counts": {
                "rows": int(len(order)),
                "ctr_positive": int(np.sum(ctr_labels, dtype=np.int64)),
                "cvr_observed_on_exposure": cvr_count,
                "combo_observed": combo_count,
            },
            "ctr_loss_sum": ctr_loss_sum,
            "cvr_loss_sum": cvr_loss_sum,
            "combo_loss_sum": combo_loss_sum,
            "ctr_grad_weights": ctr_grad_weights,
            "ctr_grad_bias": ctr_grad_bias,
            "cvr_grad_weights": cvr_grad_weights,
            "cvr_grad_bias": cvr_grad_bias,
            "combo_ctr_grad_weights": combo_ctr_grad_weights,
            "combo_ctr_grad_bias": combo_ctr_grad_bias,
            "combo_cvr_grad_weights": combo_cvr_grad_weights,
            "combo_cvr_grad_bias": combo_cvr_grad_bias,
        }
    except BaseException as exc:
        return {
            "ok": False,
            "shard_id": shard_id,
            "shard_index": int(task.get("shard_index", -1)),
            "error": {
                "code": ErrorCode.E_WORKER_FAILED,
                "message": f"training worker failed: {exc}",
                "shard_id": shard_id,
                "row_number": None,
                "details": {
                    "error_type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                },
            },
        }


def _score_shard(task: dict[str, Any]) -> dict[str, Any]:
    shard_id = task.get("shard_id")
    try:
        matrix = np.asarray(task["features"], dtype=np.float64)
        params = task["params"]
        z_ctr = matrix @ params.ctr_weights + float(params.ctr_bias)
        z_cvr = matrix @ params.cvr_weights + float(params.cvr_bias)
        log_ctr = log_sigmoid(z_ctr)
        log_cvr = log_sigmoid(z_cvr)
        return {
            "ok": True,
            "shard_id": shard_id,
            "shard_index": int(task["shard_index"]),
            "log_ctr": log_ctr,
            "log_cvr": log_cvr,
        }
    except BaseException as exc:
        return {
            "ok": False,
            "shard_id": shard_id,
            "shard_index": int(task.get("shard_index", -1)),
            "error": {
                "code": ErrorCode.E_WORKER_FAILED,
                "message": f"inference worker failed: {exc}",
                "shard_id": shard_id,
                "row_number": None,
                "details": {"error_type": type(exc).__name__, "traceback": traceback.format_exc()},
            },
        }


def execute_tasks(
    function: Any,
    tasks: list[dict[str, Any]],
    workers: int,
    mode: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Execute shard tasks; return (ordered successes, structured errors)."""

    if not tasks:
        return [], []
    if mode == "sequential" or workers <= 1:
        raw_results = []
        for task in tasks:
            try:
                raw_results.append(function(task))
            except BaseException as exc:
                raw_results.append(
                    {
                        "ok": False,
                        "shard_id": task.get("shard_id"),
                        "shard_index": int(task.get("shard_index", -1)),
                        "error": {
                            "code": ErrorCode.E_WORKER_FAILED,
                            "message": f"worker failed in sequential mode: {exc}",
                            "shard_id": task.get("shard_id"),
                            "row_number": None,
                            "details": {
                                "error_type": type(exc).__name__,
                                "traceback": traceback.format_exc(),
                            },
                        },
                    }
                )
    else:
        import multiprocessing as mp

        context = mp.get_context("spawn")
        with ProcessPoolExecutor(max_workers=workers, mp_context=context) as executor:
            futures = [executor.submit(function, task) for task in tasks]
            wait(futures)
            raw_results = []
            for index, future in enumerate(futures):
                try:
                    raw_results.append(future.result())
                except BaseException as exc:
                    task = tasks[index]
                    raw_results.append(
                        {
                            "ok": False,
                            "shard_id": task.get("shard_id"),
                            "shard_index": int(task.get("shard_index", -1)),
                            "error": {
                                "code": ErrorCode.E_WORKER_FAILED,
                                "message": f"process worker terminated without a structured result: {exc}",
                                "shard_id": task.get("shard_id"),
                                "row_number": None,
                                "details": {"error_type": type(exc).__name__},
                            },
                        }
                    )
    raw_results.sort(key=lambda result: int(result.get("shard_index", -1)))
    successes = [result for result in raw_results if result.get("ok")]
    errors = [result["error"] for result in raw_results if not result.get("ok")]
    return successes, errors


def make_train_tasks(
    shard_data: list[ShardTrainData],
    orders: dict[str, list[int]],
    params: DualParams,
) -> list[dict[str, Any]]:
    tasks = []
    for shard in sorted(shard_data, key=lambda item: item.shard_index):
        tasks.append(
            {
                "shard_id": shard.shard_id,
                "shard_index": shard.shard_index,
                "features": shard.features,
                "ctr_labels": shard.ctr_labels,
                "cvr_labels": shard.cvr_labels,
                "cvr_observed": shard.cvr_observed,
                "combo_targets": shard.combo_targets,
                "combo_mask": shard.combo_mask,
                "order": orders[shard.shard_id],
                "params": params,
            }
        )
    return tasks


def reduce_epoch(
    results: list[dict[str, Any]],
    params: DualParams,
    learning_rate: float,
    combo_weight: float,
    quantization_scale: int,
) -> tuple[DualParams, dict[str, Any]]:
    if not results:
        raise ValueError("cannot reduce an empty epoch result set")
    ordered = sorted(results, key=lambda item: int(item["shard_index"]))
    total_rows = sum(int(item["counts"]["rows"]) for item in ordered)
    total_cvr = sum(int(item["counts"]["cvr_observed_on_exposure"]) for item in ordered)
    total_combo = sum(int(item["counts"]["combo_observed"]) for item in ordered)
    ctr_loss_sum = 0.0
    cvr_loss_sum = 0.0
    combo_loss_sum = 0.0
    ctr_grad_weights = np.zeros_like(params.ctr_weights)
    cvr_grad_weights = np.zeros_like(params.cvr_weights)
    ctr_grad_bias = 0.0
    cvr_grad_bias = 0.0
    combo_ctr_grad_weights = np.zeros_like(params.ctr_weights)
    combo_cvr_grad_weights = np.zeros_like(params.cvr_weights)
    combo_ctr_grad_bias = 0.0
    combo_cvr_grad_bias = 0.0
    for item in ordered:
        ctr_loss_sum += float(item["ctr_loss_sum"])
        cvr_loss_sum += float(item["cvr_loss_sum"])
        combo_loss_sum += float(item["combo_loss_sum"])
        ctr_grad_weights = ctr_grad_weights + np.asarray(item["ctr_grad_weights"], dtype=np.float64)
        cvr_grad_weights = cvr_grad_weights + np.asarray(item["cvr_grad_weights"], dtype=np.float64)
        ctr_grad_bias += float(item["ctr_grad_bias"])
        cvr_grad_bias += float(item["cvr_grad_bias"])
        combo_ctr_grad_weights = combo_ctr_grad_weights + np.asarray(
            item["combo_ctr_grad_weights"], dtype=np.float64
        )
        combo_cvr_grad_weights = combo_cvr_grad_weights + np.asarray(
            item["combo_cvr_grad_weights"], dtype=np.float64
        )
        combo_ctr_grad_bias += float(item["combo_ctr_grad_bias"])
        combo_cvr_grad_bias += float(item["combo_cvr_grad_bias"])

    ctr_loss = ctr_loss_sum / total_rows if total_rows else 0.0
    cvr_loss = cvr_loss_sum / total_cvr if total_cvr else 0.0
    combo_loss = combo_loss_sum / total_combo if total_combo else 0.0
    total_loss = ctr_loss + cvr_loss + combo_weight * combo_loss

    ctr_grad_weights = ctr_grad_weights / max(total_rows, 1)
    cvr_grad_weights = cvr_grad_weights / max(total_cvr, 1)
    ctr_grad_bias = ctr_grad_bias / max(total_rows, 1)
    cvr_grad_bias = cvr_grad_bias / max(total_cvr, 1)
    combo_denominator = max(total_combo, 1)
    ctr_grad_weights = ctr_grad_weights + combo_weight * combo_ctr_grad_weights / combo_denominator
    cvr_grad_weights = cvr_grad_weights + combo_weight * combo_cvr_grad_weights / combo_denominator
    ctr_grad_bias += combo_weight * combo_ctr_grad_bias / combo_denominator
    cvr_grad_bias += combo_weight * combo_cvr_grad_bias / combo_denominator

    updated = DualParams(
        ctr_weights=params.ctr_weights - learning_rate * ctr_grad_weights,
        ctr_bias=float(params.ctr_bias - learning_rate * ctr_grad_bias),
        cvr_weights=params.cvr_weights - learning_rate * cvr_grad_weights,
        cvr_bias=float(params.cvr_bias - learning_rate * cvr_grad_bias),
    )
    updated = quantize_params(updated, quantization_scale)
    delta_ctr = np.concatenate(
        [updated.ctr_weights - params.ctr_weights, np.asarray([updated.ctr_bias - params.ctr_bias])]
    )
    delta_cvr = np.concatenate(
        [updated.cvr_weights - params.cvr_weights, np.asarray([updated.cvr_bias - params.cvr_bias])]
    )
    metrics = {
        "ctr_loss": ctr_loss,
        "cvr_loss": cvr_loss,
        "combo_loss": combo_loss,
        "total_loss": total_loss,
        "delta_ctr_l2": float(np.sqrt(np.sum(delta_ctr * delta_ctr, dtype=np.float64))),
        "delta_cvr_l2": float(np.sqrt(np.sum(delta_cvr * delta_cvr, dtype=np.float64))),
        "rows": total_rows,
        "cvr_observed_on_exposure": total_cvr,
        "combo_observed": total_combo,
        "shard_order": [item["shard_id"] for item in ordered],
    }
    return updated, metrics


def score_tasks(shard_data: list[ShardTrainData], params: DualParams) -> list[dict[str, Any]]:
    tasks = []
    for shard in sorted(shard_data, key=lambda item: item.shard_index):
        tasks.append(
            {
                "shard_id": shard.shard_id,
                "shard_index": shard.shard_index,
                "features": shard.features,
                "params": params,
            }
        )
    return tasks


def auc_rank(scores: list[float], labels: list[int]) -> float | None:
    if not scores:
        return None
    positives = sum(1 for label in labels if label == 1)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        return None
    # Rank 1 is the lowest score, so the Mann-Whitney rank-sum formula below
    # directly counts positive/negative pairs.
    order = sorted(range(len(scores)), key=lambda index: (scores[index], index))
    ranks = [0.0] * len(scores)
    position = 0
    while position < len(order):
        end = position + 1
        score = scores[order[position]]
        while end < len(order) and scores[order[end]] == score:
            end += 1
        average_rank = (position + 1 + end) / 2.0
        for grouped in range(position, end):
            ranks[order[grouped]] = average_rank
        position = end
    positive_rank_sum = sum(ranks[index] for index, label in enumerate(labels) if label == 1)
    return (positive_rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives)


def log_probability(value: float) -> float:
    # Score arrays always come from log_sigmoid; this validates rather than clips.
    if not math.isfinite(value) or value > 0.0:
        raise ValueError(f"illegal log probability: {value}")
    return float(value)
