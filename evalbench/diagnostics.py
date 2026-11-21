"""Reliability diagnostics and metric aggregation.

Per-bucket deviation between predicted probability and empirical frequency,
reliability table, ECE, log loss and Brier score - overall and stratified
by language tag and source batch. Every metric reports the effective sample
count and composition it relies on. Strata below the configured minimum
sample count are flagged as insufficient and listed separately, never
silently merged into the overall layer.
"""
from __future__ import annotations

import numpy as np

_EPS = 1e-12
INSUFFICIENT = "insufficient_samples"
OK = "ok"


def _clip(p: np.ndarray) -> np.ndarray:
    return np.clip(p, _EPS, 1.0 - _EPS)


def log_loss(labels: np.ndarray, probs: np.ndarray) -> float:
    p = _clip(probs)
    return float(-np.mean(labels * np.log(p) + (1.0 - labels) * np.log(1.0 - p)))


def brier_score(labels: np.ndarray, probs: np.ndarray) -> float:
    return float(np.mean((probs - labels) ** 2))


def reliability_table(labels: np.ndarray, probs: np.ndarray, buckets: np.ndarray,
                      n_bins: int) -> list[dict]:
    table = []
    n_total = len(labels)
    for b in range(n_bins):
        mask = buckets == b
        count = int(mask.sum())
        if count == 0:
            table.append({"bucket": b, "count": 0, "mean_predicted": None,
                          "empirical_freq": None, "abs_deviation": None})
            continue
        mean_pred = float(probs[mask].mean())
        freq = float(labels[mask].mean())
        table.append({
            "bucket": b,
            "count": count,
            "share": count / n_total,
            "mean_predicted": mean_pred,
            "empirical_freq": freq,
            "abs_deviation": abs(mean_pred - freq),
        })
    return table


def expected_calibration_error(table: list[dict], n_total: int) -> float:
    ece = 0.0
    for row in table:
        if row["count"]:
            ece += (row["count"] / n_total) * row["abs_deviation"]
    return float(ece)


def _composition(records: list, indices: np.ndarray) -> dict:
    by_lang: dict[str, int] = {}
    by_batch: dict[str, int] = {}
    for i in indices:
        rec = records[int(i)]
        by_lang[rec.lang] = by_lang.get(rec.lang, 0) + 1
        by_batch[rec.batch] = by_batch.get(rec.batch, 0) + 1
    return {"by_lang": dict(sorted(by_lang.items())),
            "by_batch": dict(sorted(by_batch.items()))}


def _layer_metrics(records: list, labels: np.ndarray, raw: np.ndarray,
                   calibrated: np.ndarray, buckets: np.ndarray, n_bins: int,
                   indices: np.ndarray) -> dict:
    idx = np.asarray(indices, dtype=int)
    table = reliability_table(labels[idx], calibrated[idx], buckets[idx], n_bins)
    n = int(len(idx))
    return {
        "n_samples": n,
        "composition": _composition(records, idx),
        "positive_rate": float(labels[idx].mean()),
        "reliability_table": table,
        "ece": expected_calibration_error(table, n),
        "log_loss": log_loss(labels[idx], calibrated[idx]),
        "brier_score": brier_score(labels[idx], calibrated[idx]),
        "raw_log_loss": log_loss(labels[idx], raw[idx]),
        "raw_brier_score": brier_score(labels[idx], raw[idx]),
    }


def diagnose(records: list, labels: np.ndarray, raw_scores: np.ndarray,
             calibrated: np.ndarray, buckets: np.ndarray, n_bins: int,
             min_stratum_samples: int,
             expected_positive_rate: tuple[float, float]) -> dict:
    """Full diagnostic output: overall layer + strata by lang and batch."""
    n = len(records)
    all_idx = np.arange(n)
    overall = _layer_metrics(records, labels, raw_scores, calibrated, buckets,
                             n_bins, all_idx)
    lo, hi = expected_positive_rate
    overall["expected_positive_rate"] = {
        "interval": [lo, hi],
        "observed": overall["positive_rate"],
        "within_interval": bool(lo <= overall["positive_rate"] <= hi),
    }

    strata: dict[str, list[dict]] = {"by_lang": [], "by_batch": []}
    insufficient: list[dict] = []
    for axis, attr in (("by_lang", "lang"), ("by_batch", "batch")):
        values = sorted({getattr(r, attr) for r in records})
        for value in values:
            idx = np.array([i for i, r in enumerate(records)
                            if getattr(r, attr) == value], dtype=int)
            if len(idx) < min_stratum_samples:
                insufficient.append({
                    "axis": axis, "value": value, "n_samples": int(len(idx)),
                    "min_required": min_stratum_samples,
                    "status": INSUFFICIENT,
                    "composition": _composition(records, idx),
                })
                continue
            layer = _layer_metrics(records, labels, raw_scores, calibrated,
                                   buckets, n_bins, idx)
            layer["status"] = OK
            layer["value"] = value
            strata[axis].append(layer)
    return {
        "overall": overall,
        "strata": strata,
        "insufficient_strata": insufficient,
        "min_stratum_samples": min_stratum_samples,
    }
