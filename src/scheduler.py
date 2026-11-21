"""Fold-level admission control under three hard budget constraints.

Constraints (all enforced *before* any fit is launched):
    1. global_fit_budget  - total fits accepted across the whole run;
    2. worker_fit_quota   - maximum fits assigned to any single worker;
    3. max_workers        - process-pool size / admitted concurrency.

Folds are considered in a deterministic canonical order
(shard_id, series_id, fold_index).  Every rejected fold carries an explicit
reason code, so the cumulative number of fits can never exceed the global
cap even if the process pool misbehaves.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from .config import BacktestConfig
from .errors import E_BUDGET_EXCEEDED, E_WORKER_QUOTA
from .windows import FoldSpec


@dataclass
class FoldTask:
    shard_id: str
    series_id: str
    fold_index: int
    worker_id: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    has_changepoint: bool

    @property
    def key(self) -> str:
        return f"{self.shard_id}|{self.series_id}|{self.fold_index}"


@dataclass
class SchedulingResult:
    accepted: List[FoldTask] = field(default_factory=list)
    rejected: List[Dict[str, Any]] = field(default_factory=list)
    worker_loads: Dict[int, int] = field(default_factory=dict)
    estimated_folds: int = 0

    @property
    def accepted_count(self) -> int:
        return len(self.accepted)


def _reject(fold: FoldSpec, shard_id: str, code: str,
            detail: str, context: Dict[str, Any]) -> Dict[str, Any]:
    record = {
        "shard_id": shard_id,
        "series_id": fold.series_id,
        "fold_index": fold.fold_index,
        "code": code,
        "reason": detail,
    }
    record.update(context)
    return record


def schedule_folds(folds_by_shard: Dict[str, List[FoldSpec]],
                   cfg: BacktestConfig) -> SchedulingResult:
    """Admit folds in canonical order under all three budget constraints."""
    result = SchedulingResult()
    worker_loads = {worker_id: 0 for worker_id in range(cfg.max_workers)}

    ordered: List[tuple] = []
    for shard_id in sorted(folds_by_shard):
        for fold in folds_by_shard[shard_id]:
            ordered.append((shard_id, fold))
            result.estimated_folds += 1

    global_used = 0
    for shard_id, fold in ordered:
        if global_used >= cfg.global_fit_budget:
            result.rejected.append(_reject(
                fold, shard_id, E_BUDGET_EXCEEDED,
                "global fit budget exhausted",
                {"global_fit_budget": cfg.global_fit_budget}))
            continue

        # deterministic worker choice: least-loaded, then smallest worker id
        worker_id = min(range(cfg.max_workers),
                        key=lambda wid: (worker_loads[wid], wid))
        if worker_loads[worker_id] >= cfg.worker_fit_quota:
            result.rejected.append(_reject(
                fold, shard_id, E_WORKER_QUOTA,
                "every worker reached its per-worker fit quota",
                {"worker_id": worker_id,
                 "worker_fit_quota": cfg.worker_fit_quota}))
            continue

        result.accepted.append(FoldTask(
            shard_id=shard_id,
            series_id=fold.series_id,
            fold_index=fold.fold_index,
            worker_id=worker_id,
            train_start=fold.train_start,
            train_end=fold.train_end,
            test_start=fold.test_start,
            test_end=fold.test_end,
            has_changepoint=fold.has_changepoint,
        ))
        worker_loads[worker_id] += 1
        global_used += 1

    result.worker_loads = dict(sorted(worker_loads.items()))
    return result
