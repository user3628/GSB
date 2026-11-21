"""Deterministic per-fold errors and aggregate forecasting metrics.

Aggregation is order independent (sum/count based), while every exported
record is emitted in a canonical sorted order so that serial and parallel
runs serialize to byte-identical JSON.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List

from .config import BacktestConfig
from .models import FitResult, interval_halfwidth


@dataclass
class FoldResult:
    shard_id: str
    series_id: str
    fold_index: int
    worker_id: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    has_changepoint: bool
    method: str
    params: Dict[str, float]
    predictions: List[float]
    actual: List[float]
    timestamps: List[int]
    errors: List[float]
    abs_errors: List[float]
    squared_errors: List[float]
    absolute_percentage_errors: List[float]
    interval_covered: List[bool]
    interval_halfwidth: float
    zero_denominator_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "series_id": self.series_id,
            "fold_index": self.fold_index,
            "worker_id": self.worker_id,
            "train_start": self.train_start,
            "train_end": self.train_end,
            "test_start": self.test_start,
            "test_end": self.test_end,
            "has_changepoint": self.has_changepoint,
            "method": self.method,
            "params": {key: self.params[key] for key in sorted(self.params)},
            "predictions": self.predictions,
            "actual": self.actual,
            "timestamps": self.timestamps,
            "errors": self.errors,
            "abs_errors": self.abs_errors,
            "squared_errors": self.squared_errors,
            "absolute_percentage_errors": self.absolute_percentage_errors,
            "interval_covered": self.interval_covered,
            "interval_halfwidth": self.interval_halfwidth,
            "zero_denominator_count": self.zero_denominator_count,
        }


@dataclass
class FoldFailure:
    shard_id: str
    series_id: str
    fold_index: int
    worker_id: int
    code: str
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "series_id": self.series_id,
            "fold_index": self.fold_index,
            "worker_id": self.worker_id,
            "code": self.code,
            "message": self.message,
        }


def build_fold_result(task, fit: FitResult, actual: List[float],
                      timestamps: List[int], cfg: BacktestConfig) -> FoldResult:
    predictions = list(fit.predictions)
    errors = [pred - real for pred, real in zip(predictions, actual)]
    abs_errors = [abs(value) for value in errors]
    squared_errors = [value * value for value in errors]
    # MAPE skips zero (or near-zero) denominators and counts them
    ape: List[float] = []
    zero_denominators = 0
    for pred, real in zip(predictions, actual):
        if abs(real) < 1e-12:
            zero_denominators += 1
            continue
        ape.append(abs((real - pred) / real))
    halfwidth = interval_halfwidth(fit.sigma, cfg.confidence_level)
    covered = [abs(real - pred) <= halfwidth
               for pred, real in zip(predictions, actual)]
    result = FoldResult(
        shard_id=task.shard_id,
        series_id=task.series_id,
        fold_index=task.fold_index,
        worker_id=task.worker_id,
        train_start=task.train_start,
        train_end=task.train_end,
        test_start=task.test_start,
        test_end=task.test_end,
        has_changepoint=task.has_changepoint,
        method=fit.method,
        params=dict(fit.params),
        predictions=predictions,
        actual=list(actual),
        timestamps=list(timestamps),
        errors=errors,
        abs_errors=abs_errors,
        squared_errors=squared_errors,
        absolute_percentage_errors=ape,
        interval_covered=covered,
        interval_halfwidth=halfwidth,
        zero_denominator_count=zero_denominators,
    )
    return result


def aggregate(results: List[FoldResult]) -> Dict[str, Any]:
    """Compute global metrics and per-series/per-shard breakdowns."""
    summary = _summarize(results)

    def group_metrics(key_name: str, key_func) -> List[Dict[str, Any]]:
        groups: Dict[str, List[FoldResult]] = {}
        for item in results:
            groups.setdefault(str(key_func(item)), []).append(item)
        rows = []
        for key in sorted(groups):
            row = _summarize(groups[key])
            row[key_name] = key
            rows.append(row)
        return rows

    return {
        "summary": summary,
        "by_series": group_metrics("series_id", lambda item: item.series_id),
        "by_shard": group_metrics("shard_id", lambda item: item.shard_id),
    }


def _summarize(results: List[FoldResult]) -> Dict[str, Any]:
    total_abs = sum(sum(item.abs_errors) for item in results)
    total_sq = sum(sum(item.squared_errors) for item in results)
    total_ape: List[float] = []
    zero_denominator = 0
    covered_steps = 0
    total_steps = 0
    for item in results:
        total_ape.extend(item.absolute_percentage_errors)
        zero_denominator += getattr(item, "zero_denominator_count", 0)
        covered_steps += sum(item.interval_covered)
        total_steps += len(item.interval_covered)

    step_count = sum(len(item.predictions) for item in results)
    return {
        "fold_count": len(results),
        "step_count": step_count,
        "mae": total_abs / step_count if step_count else None,
        "rmse": math.sqrt(total_sq / step_count) if step_count else None,
        "mape": (sum(total_ape) / len(total_ape)) if total_ape else None,
        "mape_evaluated_steps": len(total_ape),
        "mape_skipped_zero_denominator": zero_denominator,
        "coverage": covered_steps / total_steps if total_steps else None,
        "interval_covered_steps": covered_steps,
    }
