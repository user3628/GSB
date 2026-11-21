"""Rolling window splitting with explicit missing-point/changepoint handling.

A *fold* is a contiguous slice of a series' regular time grid consisting of
``window_length`` training slots followed by ``horizon`` test slots.  Folds
advance by ``step`` slots and overlap.  Any fold whose train or test span
contains a missing slot (missing timestamp row or missing target value) is
excluded with an explicit reason, rather than silently producing forecasts
without ground truth.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np

from .config import BacktestConfig
from .data import SeriesData, estimate_slots, ShardData
from .errors import (E_MISSING_RATIO, E_SERIES_TOO_SHORT, FoldError)


@dataclass
class FoldSpec:
    series_id: str
    fold_index: int          # zero-based ordinal among candidate folds
    train_start: int
    train_end: int           # exclusive
    test_start: int
    test_end: int            # exclusive
    has_changepoint: bool

    @property
    def key(self) -> str:
        return f"{self.series_id}|{self.fold_index}"


@dataclass
class SeriesPlan:
    series_id: str
    slot_count: int
    missing_ratio: float
    missing_timestamp_count: int
    missing_target_count: int
    changepoints: List[int]
    folds: List[FoldSpec] = field(default_factory=list)
    excluded: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[FoldError] = None


def candidate_fold_count(slot_count: int, cfg: BacktestConfig) -> int:
    span = cfg.window_length + cfg.horizon
    if slot_count < span:
        return 0
    return (slot_count - span) // cfg.step + 1


def estimate_fold_count(shard_entry: Dict[str, Any],
                        cfg: BacktestConfig) -> int:
    """Manifest-driven estimate used by the scheduler before reading data."""
    default_slots = estimate_slots(shard_entry)
    grid_lengths = dict(shard_entry.get("series_grid_slots", {}))
    total = 0
    for series_id in shard_entry["series"]:
        slots = int(grid_lengths.get(series_id, default_slots))
        total += candidate_fold_count(slots, cfg)
    return total


def detect_changepoints(series: SeriesData,
                        threshold: float) -> List[int]:
    """Mark slots whose relative change vs the previous value exceeds the
    configured threshold.  Missing slots break the comparison chain."""
    points: List[int] = []
    values = series.target
    for slot in range(1, len(values)):
        previous, current = values[slot - 1], values[slot]
        if np.isnan(previous) or np.isnan(current):
            continue
        denominator = max(abs(previous), 1e-12)
        if abs(current - previous) / denominator >= threshold:
            points.append(slot)
    return points


def build_series_plan(series: SeriesData, cfg: BacktestConfig) -> SeriesPlan:
    slot_count = series.slot_count
    missing_mask = series.missing_timestamp | series.missing_target
    missing_count = int(missing_mask.sum())
    missing_ratio = missing_count / slot_count if slot_count else 1.0
    changepoints = detect_changepoints(series, cfg.changepoint_threshold)

    plan = SeriesPlan(
        series_id=series.series_id,
        slot_count=slot_count,
        missing_ratio=missing_ratio,
        missing_timestamp_count=int(series.missing_timestamp.sum()),
        missing_target_count=int(series.missing_target.sum()),
        changepoints=changepoints,
    )

    if missing_ratio > cfg.max_missing_ratio:
        plan.error = FoldError(
            code=E_MISSING_RATIO,
            message=("missing ratio "
                     f"{missing_ratio:.4f} exceeds limit "
                     f"{cfg.max_missing_ratio:.4f}"),
            series_id=series.series_id,
            context={"missing_ratio": missing_ratio,
                     "max_missing_ratio": cfg.max_missing_ratio,
                     "missing_count": missing_count,
                     "slot_count": slot_count})
        return plan

    if slot_count < cfg.window_length:
        plan.error = FoldError(
            code=E_SERIES_TOO_SHORT,
            message=(f"series length {slot_count} is shorter than one window "
                     f"of {cfg.window_length}"),
            series_id=series.series_id,
            context={"slot_count": slot_count,
                     "window_length": cfg.window_length})
        return plan

    fold_count = candidate_fold_count(slot_count, cfg)
    if fold_count == 0:
        plan.error = FoldError(
            code=E_SERIES_TOO_SHORT,
            message=("series has no complete fold: need "
                     f"{cfg.window_length + cfg.horizon} slots, got "
                     f"{slot_count}"),
            series_id=series.series_id,
            context={"slot_count": slot_count,
                     "required": cfg.window_length + cfg.horizon})
        return plan

    change_set = set(changepoints)
    for fold_index in range(fold_count):
        train_start = fold_index * cfg.step
        train_end = train_start + cfg.window_length
        test_end = train_end + cfg.horizon
        missing_in_train = np.flatnonzero(missing_mask[train_start:train_end])
        missing_in_test = np.flatnonzero(missing_mask[train_end:test_end])
        if missing_in_train.size or missing_in_test.size:
            reason = {
                "series_id": series.series_id,
                "fold_index": fold_index,
                "reason": "MISSING_TRAIN_SLOT" if missing_in_train.size
                else "MISSING_TEST_SLOT",
                "train_missing": int(missing_in_train.size),
                "test_missing": int(missing_in_test.size),
            }
            plan.excluded.append(reason)
            continue
        plan.folds.append(FoldSpec(
            series_id=series.series_id,
            fold_index=fold_index,
            train_start=train_start,
            train_end=train_end,
            test_start=train_end,
            test_end=test_end,
            has_changepoint=bool(change_set & set(range(train_start,
                                                        train_end))),
        ))

    if not plan.folds:
        plan.error = FoldError(
            code=E_SERIES_TOO_SHORT,
            message="all candidate folds were excluded due to missing slots",
            series_id=series.series_id,
            context={"candidate_folds": fold_count,
                     "excluded": len(plan.excluded)})
    return plan


def build_shard_plan(shard: ShardData,
                     cfg: BacktestConfig) -> Dict[str, SeriesPlan]:
    return {series_id: build_series_plan(series, cfg)
            for series_id, series in sorted(shard.series.items())}


def all_eligible_folds(plans: Dict[str, SeriesPlan]) -> List[FoldSpec]:
    folds: List[FoldSpec] = []
    for series_id in sorted(plans):
        folds.extend(plans[series_id].folds)
    return folds
