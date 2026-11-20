"""Platt scaling and isotonic probability calibration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .config import Config
from .errors import WorkbenchError
from .util import float_to_hex


def _stable_sigmoid(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    output = np.empty_like(values)
    positive = values >= 0.0
    output[positive] = 1.0 / (1.0 + np.exp(-values[positive]))
    exp_values = np.exp(values[~positive])
    output[~positive] = exp_values / (1.0 + exp_values)
    return output


def _validate_calibration_inputs(scores: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    scores = np.asarray(scores, dtype=np.float64)
    y = np.asarray(y)
    if scores.ndim != 1 or y.ndim != 1 or scores.shape != y.shape:
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "calibration scores and labels must be one-dimensional and equally sized",
            {"scores_shape": list(scores.shape), "labels_shape": list(y.shape)},
        )
    invalid = np.flatnonzero(~np.isfinite(scores))
    if invalid.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "calibration score is NaN or Inf",
            {"sample_index": int(invalid[0]), "stage": "calibration"},
        )
    if not np.all((y == 0) | (y == 1)):
        raise WorkbenchError("E_INVALID_PARAM", "calibration labels must be binary", {"stage": "calibration"})
    classes = np.unique(y.astype(np.uint8))
    if classes.size < 2:
        raise WorkbenchError(
            "E_CALIBRATION_INFEASIBLE",
            "calibration requires both classes",
            {"sample_count": int(scores.size), "classes": [int(value) for value in classes]},
        )
    return scores, y.astype(np.float64, copy=False)


@dataclass(frozen=True)
class Calibrator:
    kind: str
    iterations: int
    converged: bool
    sample_count: int
    class_counts: tuple[int, int]
    parameters: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "type": self.kind,
            "iterations": int(self.iterations),
            "converged": bool(self.converged),
            "sample_count": int(self.sample_count),
            "class_counts": [int(value) for value in self.class_counts],
            "parameters": self.parameters,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "Calibrator":
        return cls(
            kind=str(payload["type"]),
            iterations=int(payload["iterations"]),
            converged=bool(payload["converged"]),
            sample_count=int(payload["sample_count"]),
            class_counts=(int(payload["class_counts"][0]), int(payload["class_counts"][1])),
            parameters=payload["parameters"],
        )

    def human_parameters(self) -> dict[str, Any]:
        if self.kind == "platt":
            return {
                "a": float.fromhex(self.parameters["a_hex"]),
                "b": float.fromhex(self.parameters["b_hex"]),
            }
        return {
            "knots": len(self.parameters.get("x_hex", [])),
            "x_min": float.fromhex(self.parameters["x_hex"][0]) if self.parameters.get("x_hex") else None,
            "x_max": float.fromhex(self.parameters["x_hex"][-1]) if self.parameters.get("x_hex") else None,
        }


def _platt_nll(a: float, b: float, scores: np.ndarray, y: np.ndarray) -> float:
    z = a * scores + b
    return float(np.mean(np.logaddexp(0.0, z) - y * z, dtype=np.float64))


def fit_platt(scores: np.ndarray, y: np.ndarray, config: Config) -> Calibrator:
    scores, y = _validate_calibration_inputs(scores, y)
    a = 1.0
    b = 0.0
    loss = _platt_nll(a, b, scores, y)
    converged = False
    iterations = 0
    for _ in range(config.platt_max_iter):
        z = a * scores + b
        probability = _stable_sigmoid(z)
        residual = probability - y
        weight = np.maximum(probability * (1.0 - probability), 1e-15)
        g_a = float(np.sum(residual * scores, dtype=np.float64))
        g_b = float(np.sum(residual, dtype=np.float64))
        h_aa = float(np.sum(weight * scores * scores, dtype=np.float64)) + 1e-12
        h_ab = float(np.sum(weight * scores, dtype=np.float64))
        h_bb = float(np.sum(weight, dtype=np.float64)) + 1e-12
        determinant = h_aa * h_bb - h_ab * h_ab
        if not np.isfinite(determinant) or abs(determinant) < 1e-24:
            break
        delta_a = (h_bb * g_a - h_ab * g_b) / determinant
        delta_b = (-h_ab * g_a + h_aa * g_b) / determinant
        if not (np.isfinite(delta_a) and np.isfinite(delta_b)):
            break
        scale = 1.0
        accepted = False
        for _ in range(40):
            candidate_a = a - scale * delta_a
            candidate_b = b - scale * delta_b
            candidate_loss = _platt_nll(candidate_a, candidate_b, scores, y)
            if np.isfinite(candidate_loss) and candidate_loss <= loss + 1e-15:
                a, b, loss = candidate_a, candidate_b, candidate_loss
                accepted = True
                break
            scale *= 0.5
        if not accepted:
            break
        iterations += 1
        if max(abs(scale * delta_a), abs(scale * delta_b)) < config.platt_tol:
            converged = True
            break
    parameters = {"a_hex": float_to_hex(a), "b_hex": float_to_hex(b), "final_nll_hex": float_to_hex(loss)}
    return Calibrator(
        kind="platt",
        iterations=iterations,
        converged=converged,
        sample_count=int(scores.size),
        class_counts=(int(np.sum(y == 0)), int(np.sum(y == 1))),
        parameters=parameters,
    )


def fit_isotonic(scores: np.ndarray, y: np.ndarray) -> Calibrator:
    scores, y = _validate_calibration_inputs(scores, y)
    order = np.lexsort((np.arange(scores.size, dtype=np.int64), scores))
    sorted_scores = scores[order]
    sorted_y = y[order]
    unique_scores, group_index, group_counts = np.unique(
        sorted_scores, return_inverse=True, return_counts=True
    )
    group_sums = np.bincount(group_index, weights=sorted_y, minlength=unique_scores.size)

    block_starts: list[int] = []
    block_counts: list[int] = []
    block_sums: list[float] = []
    block_values: list[float] = []
    merges = 0
    for group in range(unique_scores.size):
        block_starts.append(group)
        block_counts.append(int(group_counts[group]))
        block_sums.append(float(group_sums[group]))
        block_values.append(float(group_sums[group] / group_counts[group]))
        while len(block_values) >= 2 and block_values[-2] > block_values[-1]:
            count = block_counts[-2] + block_counts[-1]
            total = block_sums[-2] + block_sums[-1]
            value = total / count
            block_starts.pop()
            block_counts.pop()
            block_sums.pop()
            block_values.pop()
            block_counts[-1] = count
            block_sums[-1] = total
            block_values[-1] = value
            merges += 1

    fitted = np.empty(unique_scores.size, dtype=np.float64)
    block_end = 0
    for start, count, value in zip(block_starts, block_counts, block_values):
        block_end = start + count
        fitted[start:block_end] = value
    parameters = {
        "x_hex": [float_to_hex(value) for value in unique_scores.tolist()],
        "y_hex": [float_to_hex(value) for value in fitted.tolist()],
        "merge_count": int(merges),
    }
    return Calibrator(
        kind="isotonic",
        iterations=merges,
        converged=True,
        sample_count=int(scores.size),
        class_counts=(int(np.sum(y == 0)), int(np.sum(y == 1))),
        parameters=parameters,
    )


def fit_calibrator(config: Config, scores: np.ndarray, y: np.ndarray) -> Calibrator:
    if config.calibrator == "platt":
        return fit_platt(scores, y, config)
    if config.calibrator == "isotonic":
        return fit_isotonic(scores, y)
    raise WorkbenchError("E_INVALID_PARAM", "unknown calibrator", {"calibrator": config.calibrator})


def apply_calibrator(calibrator: Calibrator, scores: np.ndarray, *, stage: str = "calibration") -> np.ndarray:
    scores = np.asarray(scores, dtype=np.float64)
    invalid = np.flatnonzero(~np.isfinite(scores))
    if invalid.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "raw score is NaN or Inf",
            {"sample_index": int(invalid[0]), "stage": stage},
        )
    if calibrator.kind == "platt":
        a = float.fromhex(calibrator.parameters["a_hex"])
        b = float.fromhex(calibrator.parameters["b_hex"])
        probability = _stable_sigmoid(a * scores + b)
    elif calibrator.kind == "isotonic":
        x = np.asarray([float.fromhex(value) for value in calibrator.parameters["x_hex"]], dtype=np.float64)
        fitted = np.asarray([float.fromhex(value) for value in calibrator.parameters["y_hex"]], dtype=np.float64)
        probability = np.interp(scores, x, fitted, left=fitted[0], right=fitted[-1])
    else:  # pragma: no cover - guarded by construction
        raise WorkbenchError("E_INVALID_STAGE", "unknown calibrator in artifact", {"kind": calibrator.kind})
    bad = np.flatnonzero(~np.isfinite(probability))
    if bad.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "calibrated probability is NaN or Inf",
            {"sample_index": int(bad[0]), "stage": stage},
        )
    out_of_range = np.flatnonzero((probability < 0.0) | (probability > 1.0))
    if out_of_range.size:
        index = int(out_of_range[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "calibrated probability is outside [0, 1]",
            {"sample_index": index, "probability": float(probability[index]), "stage": stage},
        )
    return probability
