"""Fixed-point quantization and high-precision numerical diagnostics."""

from __future__ import annotations

import base64
from decimal import Decimal, localcontext
from typing import Any

import numpy as np

from .errors import WorkbenchError
from .util import sha256_bytes


def scale_for_decimals(decimals: int) -> int:
    if not isinstance(decimals, int) or decimals < 0 or decimals > 15:
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "quantize_decimals must be an integer in [0, 15]",
            {"quantize_decimals": decimals},
        )
    return 10**decimals


def quantize_raw_scores(values: np.ndarray, decimals: int, *, stage: str) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    if values.ndim != 1:
        raise WorkbenchError("E_INVALID_PARAM", "raw scores must be one-dimensional", {"stage": stage})
    invalid = np.flatnonzero(~np.isfinite(values))
    if invalid.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "raw score is NaN or Inf",
            {"sample_index": int(invalid[0]), "stage": stage},
        )
    scale = scale_for_decimals(decimals)
    scaled = values * float(scale)
    limit = float(np.iinfo(np.int64).max)
    overflow = np.flatnonzero(np.abs(scaled) > limit)
    if overflow.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "fixed-point raw score overflow",
            {"sample_index": int(overflow[0]), "stage": stage, "decimals": decimals},
        )
    return np.rint(scaled).astype(np.int64)


def quantize_probabilities(values: np.ndarray, decimals: int, *, stage: str) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    if values.ndim != 1:
        raise WorkbenchError("E_INVALID_PARAM", "probabilities must be one-dimensional", {"stage": stage})
    invalid = np.flatnonzero(~np.isfinite(values))
    if invalid.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "probability is NaN or Inf",
            {"sample_index": int(invalid[0]), "stage": stage},
        )
    out_of_range = np.flatnonzero((values < 0.0) | (values > 1.0))
    if out_of_range.size:
        index = int(out_of_range[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "probability is outside [0, 1]",
            {"sample_index": index, "probability": float(values[index]), "stage": stage},
        )
    scale = scale_for_decimals(decimals)
    quantized = np.rint(values * float(scale)).astype(np.int64)
    bad = np.flatnonzero((quantized < 0) | (quantized > scale))
    if bad.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "fixed-point probability is outside the valid range",
            {"sample_index": int(bad[0]), "stage": stage},
        )
    return quantized


def dequantize(values: np.ndarray, decimals: int) -> np.ndarray:
    values = np.asarray(values, dtype=np.int64)
    scale = scale_for_decimals(decimals)
    return values.astype(np.float64) / float(scale)


def encode_int64(values: np.ndarray) -> str:
    return base64.b64encode(np.asarray(values, dtype="<i8").tobytes(order="C")).decode("ascii")


def decode_int64(encoded: str, expected_length: int) -> np.ndarray:
    data = base64.b64decode(encoded.encode("ascii"), validate=True)
    if len(data) != expected_length * 8:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "fixed-point payload has invalid length",
            {"expected": expected_length * 8, "actual": len(data)},
        )
    return np.frombuffer(data, dtype="<i8").astype(np.int64, copy=True)


def int64_digest(values: np.ndarray) -> str:
    return sha256_bytes(np.asarray(values, dtype="<i8").tobytes(order="C"))


def probability_terms(
    probability_q: np.ndarray,
    labels: np.ndarray,
    decimals: int,
    *,
    stage: str,
) -> tuple[np.ndarray, np.ndarray]:
    probability_q = np.asarray(probability_q, dtype=np.int64)
    labels = np.asarray(labels)
    if probability_q.shape != labels.shape or probability_q.ndim != 1:
        raise WorkbenchError("E_INVALID_PARAM", "probability and label vectors must align", {"stage": stage})
    if not np.all((labels == 0) | (labels == 1)):
        raise WorkbenchError("E_INVALID_PARAM", "labels must be binary", {"stage": stage})
    scale_int = scale_for_decimals(decimals)
    if not np.all((probability_q >= 0) & (probability_q <= scale_int)):
        bad = int(np.flatnonzero((probability_q < 0) | (probability_q > scale_int))[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "probability quantization is outside [0, 1]",
            {"sample_index": bad, "stage": stage},
        )
    probability = probability_q.astype(np.float64) / float(scale_int)
    y = labels.astype(np.float64)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        losses = np.empty_like(probability)
        positive = labels == 1
        losses[positive] = -np.log(probability[positive])
        losses[~positive] = -np.log1p(-probability[~positive])
    bad_loss = np.flatnonzero(~np.isfinite(losses))
    if bad_loss.size:
        index = int(bad_loss[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "log loss is NaN or Inf at the quantized probability",
            {
                "sample_index": index,
                "stage": stage,
                "probability": float(probability[index]),
                "label": int(labels[index]),
            },
        )
    brier = (probability - y) * (probability - y)
    loss_q = np.rint(losses * float(scale_int)).astype(np.int64)
    brier_q = np.rint(brier * float(scale_int)).astype(np.int64)
    return loss_q, brier_q


def reliability_table(probability_q: np.ndarray, labels: np.ndarray, bins: int, decimals: int) -> dict[str, Any]:
    if not isinstance(bins, int) or bins <= 0:
        raise WorkbenchError("E_INVALID_PARAM", "diagnostic bins must be positive", {"bins": bins})
    probability_q = np.asarray(probability_q, dtype=np.int64)
    labels = np.asarray(labels)
    if probability_q.shape != labels.shape or probability_q.ndim != 1:
        raise WorkbenchError("E_INVALID_PARAM", "probability and label vectors must align", {"stage": "diagnostics"})
    scale = scale_for_decimals(decimals)
    probability = probability_q.astype(np.float64) / float(scale)
    invalid = np.flatnonzero(~np.isfinite(probability) | (probability < 0.0) | (probability > 1.0))
    if invalid.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "probability is invalid for reliability diagnostics",
            {"sample_index": int(invalid[0]), "stage": "diagnostics"},
        )
    bin_indices = np.minimum((probability * bins).astype(np.int64), bins - 1)
    counts = np.bincount(bin_indices, minlength=bins)
    positives = np.bincount(bin_indices, weights=labels.astype(np.float64), minlength=bins)
    probability_sums = np.zeros(bins, dtype=object)
    for index in range(bins):
        probability_sums[index] = 0
    for value, bin_index in zip(probability_q.tolist(), bin_indices.tolist()):
        probability_sums[bin_index] += int(value)
    rows: list[dict[str, Any]] = []
    ece_terms: list[float] = []
    deviations: list[float] = []
    total = int(probability_q.size)
    for index in range(bins):
        count = int(counts[index])
        lower = index / float(bins)
        upper = (index + 1) / float(bins)
        if count == 0:
            rows.append(
                {
                    "index": index,
                    "lower": lower,
                    "upper": upper,
                    "count": 0,
                    "mean_probability": None,
                    "observed_rate": None,
                    "deviation": None,
                }
            )
            continue
        mean_probability = float(probability_sums[index]) / (count * scale)
        observed_rate = float(positives[index]) / count
        deviation = abs(mean_probability - observed_rate)
        deviations.append(deviation)
        ece_terms.append(count / total * deviation)
        rows.append(
            {
                "index": index,
                "lower": lower,
                "upper": upper,
                "count": count,
                "mean_probability": mean_probability,
                "observed_rate": observed_rate,
                "deviation": deviation,
            }
        )
    return {
        "bin_count": bins,
        "sample_count": total,
        "rows": rows,
        "ece": float(sum(ece_terms)),
        "max_bin_deviation": float(max(deviations)) if deviations else 0.0,
    }


def high_precision_error_report(
    reference_probability: np.ndarray,
    labels: np.ndarray,
    probability_q: np.ndarray,
    loss_q: np.ndarray,
    brier_q: np.ndarray,
    decimals: int,
) -> dict[str, Any]:
    reference_probability = np.asarray(reference_probability, dtype=np.float64)
    labels = np.asarray(labels)
    probability_q = np.asarray(probability_q, dtype=np.int64)
    loss_q = np.asarray(loss_q, dtype=np.int64)
    brier_q = np.asarray(brier_q, dtype=np.int64)
    invalid = np.flatnonzero(~np.isfinite(reference_probability) | (reference_probability < 0.0) | (reference_probability > 1.0))
    if invalid.size:
        raise WorkbenchError(
            "E_NUMERIC",
            "reference probability is invalid",
            {"sample_index": int(invalid[0]), "stage": "diagnostics"},
        )
    scale = scale_for_decimals(decimals)
    step = Decimal(1) / Decimal(scale)
    rounding = step / Decimal(2)
    reference_loss = Decimal(0)
    reference_brier = Decimal(0)
    loss_derivative_sum = Decimal(0)
    unbounded = False
    with localcontext() as context:
        context.prec = 60
        for probability, label in zip(reference_probability.tolist(), labels.tolist()):
            p = Decimal.from_float(float(probability))
            y = Decimal(int(label))
            if p <= 0:
                if y == 0:
                    term_loss = Decimal(0)
                    derivative = Decimal(1)
                else:
                    term_loss = Decimal("Infinity")
                    derivative = Decimal("Infinity")
                    unbounded = True
            elif p >= 1:
                if y == 1:
                    term_loss = Decimal(0)
                    derivative = Decimal(1)
                else:
                    term_loss = Decimal("Infinity")
                    derivative = Decimal("Infinity")
                    unbounded = True
            else:
                term_loss = -(y * p.ln() + (Decimal(1) - y) * (Decimal(1) - p).ln())
                derivative = (Decimal(1) / p) if y == 1 else (Decimal(1) / (Decimal(1) - p))
            reference_loss += term_loss
            reference_brier += (p - y) * (p - y)
            loss_derivative_sum += derivative
        actual_loss = Decimal(sum(int(value) for value in loss_q.tolist())) / Decimal(scale)
        actual_brier = Decimal(sum(int(value) for value in brier_q.tolist())) / Decimal(scale)
        observed_loss_error = abs(actual_loss - reference_loss)
        observed_brier_error = abs(actual_brier - reference_brier)
        arithmetic_allowance = Decimal(max(1, reference_probability.size)) * Decimal("1e-15")
        if unbounded:
            loss_bound = None
            loss_bound_unbounded = True
        else:
            loss_bound = loss_derivative_sum * rounding + rounding * Decimal(reference_probability.size) + arithmetic_allowance
            loss_bound_unbounded = False
        brier_bound = (
            Decimal(2) * rounding * Decimal(reference_probability.size)
            + rounding * Decimal(reference_probability.size)
            + arithmetic_allowance
        )
        return {
            "reference": "decimal-local-precision-60",
            "effective_decimals": int(decimals),
            "quantization_step": float(step),
            "reference_log_loss": str(reference_loss),
            "workbench_log_loss": str(actual_loss),
            "observed_log_loss_error": float(observed_loss_error),
            "log_loss_error_upper_bound": None if loss_bound is None else float(loss_bound),
            "log_loss_bound_unbounded": loss_bound_unbounded,
            "reference_brier": str(reference_brier),
            "workbench_brier": str(actual_brier),
            "observed_brier_error": float(observed_brier_error),
            "brier_error_upper_bound": float(brier_bound),
            "bound_method": "probability-rounding propagation + per-term fixed-point rounding + float arithmetic allowance",
        }
