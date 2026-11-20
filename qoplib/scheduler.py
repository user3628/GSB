"""Budget-constrained shard admission and parallelism scheduler.

Three hard constraints decide admission and worker count:

1. ``total_element_budget``  - cumulative compute elements never exceeded.
2. ``worker_element_quota``  - a single shard may not cost more than a lane.
3. ``peak_memory_bytes``     - sum of concurrently live working sets bounded.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List

from .errors import (
    E_COMPUTE_BUDGET,
    E_MEMORY_BUDGET,
    E_WORKER_QUOTA,
)
from .manifest import (
    BUDGET_REASON_COMPUTE,
    BUDGET_REASON_MEMORY,
    BUDGET_REASON_QUOTA,
    Budget,
    Manifest,
    ShardEstimate,
    ShardSpec,
    estimate_shard,
)


@dataclass
class RejectedShard:
    shard_id: str
    index: int
    reason_code: str
    reason: str
    budget_detail: Dict[str, Any]


@dataclass
class LaneAssignment:
    shard_id: str
    index: int
    lane: int


@dataclass
class ScheduleDecision:
    accepted: List[ShardEstimate] = field(default_factory=list)
    rejected: List[RejectedShard] = field(default_factory=list)
    lane_assignments: List[LaneAssignment] = field(default_factory=list)
    worker_count: int = 0
    total_accepted_compute: int = 0
    total_accepted_memory: int = 0
    budget: Dict[str, int] = field(default_factory=dict)
    per_shard_decisions: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "worker_count": self.worker_count,
            "total_accepted_compute": self.total_accepted_compute,
            "total_accepted_memory": self.total_accepted_memory,
            "budget": self.budget,
            "accepted": [
                {"shard_id": item.shard_id, "index": item.index,
                 "compute_elements": item.compute_elements,
                 "working_set_bytes": item.working_set_bytes}
                for item in self.accepted
            ],
            "rejected": [item.__dict__ for item in self.rejected],
            "lane_assignments": [item.__dict__ for item in
                                 self.lane_assignments],
            "per_shard_decisions": self.per_shard_decisions,
        }


def _cpu_lanes() -> int:
    cpu = os.cpu_count() or 1
    return max(1, cpu)


def plan_schedule(manifest: Manifest,
                  max_workers: int | None = None) -> ScheduleDecision:
    """Plan admission deterministically from manifest declarations."""

    budget: Budget = manifest.budget
    decision = ScheduleDecision(budget=budget.to_dict())
    used_compute = 0
    estimates: List[tuple[ShardSpec, ShardEstimate]] = [
        (shard, estimate_shard(shard)) for shard in manifest.shards
    ]

    for shard, estimate in estimates:
        detail = {
            "shard_id": shard.shard_id,
            "compute_elements": estimate.compute_elements,
            "working_set_bytes": estimate.working_set_bytes,
            "used_compute_before": used_compute,
            "total_element_budget": budget.total_element_budget,
            "worker_element_quota": budget.worker_element_quota,
            "peak_memory_bytes": budget.peak_memory_bytes,
        }
        if estimate.compute_elements > budget.worker_element_quota:
            decision.rejected.append(RejectedShard(
                shard_id=shard.shard_id, index=shard.index,
                reason_code=E_WORKER_QUOTA, reason=BUDGET_REASON_QUOTA,
                budget_detail=dict(detail),
            ))
            decision.per_shard_decisions.append(
                {**detail, "admitted": False,
                 "reason_code": E_WORKER_QUOTA})
            continue
        if used_compute + estimate.compute_elements > \
                budget.total_element_budget:
            decision.rejected.append(RejectedShard(
                shard_id=shard.shard_id, index=shard.index,
                reason_code=E_COMPUTE_BUDGET, reason=BUDGET_REASON_COMPUTE,
                budget_detail=dict(detail),
            ))
            decision.per_shard_decisions.append(
                {**detail, "admitted": False,
                 "reason_code": E_COMPUTE_BUDGET})
            continue
        if estimate.working_set_bytes > budget.peak_memory_bytes:
            decision.rejected.append(RejectedShard(
                shard_id=shard.shard_id, index=shard.index,
                reason_code=E_MEMORY_BUDGET, reason=BUDGET_REASON_MEMORY,
                budget_detail=dict(detail),
            ))
            decision.per_shard_decisions.append(
                {**detail, "admitted": False,
                 "reason_code": E_MEMORY_BUDGET})
            continue

        decision.accepted.append(estimate)
        used_compute += estimate.compute_elements
        decision.per_shard_decisions.append(
            {**detail, "admitted": True,
             "used_compute_after": used_compute})

    worker_count = _resolve_worker_count(decision, budget, max_workers)
    decision.worker_count = worker_count
    decision.total_accepted_compute = used_compute
    decision.total_accepted_memory = sum(
        item.working_set_bytes for item in decision.accepted)
    decision.lane_assignments = _assign_lanes(decision.accepted, worker_count,
                                              budget)
    return decision


def _resolve_worker_count(decision: ScheduleDecision, budget: Budget,
                          max_workers: int | None) -> int:
    if not decision.accepted:
        return 0
    cap = min(len(decision.accepted), _cpu_lanes())
    if max_workers is not None:
        cap = min(cap, max(1, max_workers))
    # Peak memory: find the largest lane count for which every lane's worst
    # case concurrent working set stays inside the peak memory budget.
    workers = cap
    while workers > 1:
        lanes = _assign_lanes(decision.accepted, workers, budget)
        peak = 0
        per_lane: Dict[int, int] = {}
        for assignment, estimate in zip(lanes, decision.accepted):
            per_lane[assignment.lane] = per_lane.get(assignment.lane, 0) + \
                estimate.working_set_bytes
        peak = max(per_lane.values())
        if peak <= budget.peak_memory_bytes:
            break
        workers -= 1
    return max(1, workers)


def _assign_lanes(accepted: List[ShardEstimate], worker_count: int,
                  budget: Budget) -> List[LaneAssignment]:
    """Greedy least-loaded lane assignment (deterministic tie break)."""

    lanes = [LaneLoad(lane=idx, memory=0, compute=0)
             for idx in range(max(1, worker_count))]
    assignments: List[LaneAssignment] = []
    for estimate in accepted:
        target = min(lanes, key=lambda lane: (lane.memory, lane.lane))
        assignments.append(LaneAssignment(shard_id=estimate.shard_id,
                                          index=estimate.index,
                                          lane=target.lane))
        target.memory += estimate.working_set_bytes
        target.compute += estimate.compute_elements
    return assignments


class LaneLoad:
    __slots__ = ("lane", "memory", "compute")

    def __init__(self, lane: int, memory: int, compute: int):
        self.lane = lane
        self.memory = memory
        self.compute = compute


def enforce_runtime_admission(estimate: ShardEstimate,
                              budget: Budget,
                              used_compute: int) -> None:
    """Second line of defence invoked right before a shard is dispatched.

    Guarantees cumulative consumption can never exceed the budget even when
    callers construct their own shard lists.
    """

    if estimate.compute_elements > budget.worker_element_quota:
        from .errors import WorkerQuotaError

        raise WorkerQuotaError(
            f"shard {estimate.shard_id} exceeds single worker quota",
            details={"shard_id": estimate.shard_id,
                     "compute_elements": estimate.compute_elements,
                     "worker_element_quota": budget.worker_element_quota},
        )
    if used_compute + estimate.compute_elements > \
            budget.total_element_budget:
        from .errors import ComputeBudgetError

        raise ComputeBudgetError(
            f"shard {estimate.shard_id} would exceed total compute budget",
            details={"shard_id": estimate.shard_id,
                     "compute_elements": estimate.compute_elements,
                     "used_compute": used_compute,
                     "total_element_budget": budget.total_element_budget},
        )
    if estimate.working_set_bytes > budget.peak_memory_bytes:
        from .errors import MemoryBudgetError

        raise MemoryBudgetError(
            f"shard {estimate.shard_id} working set exceeds peak memory",
            details={"shard_id": estimate.shard_id,
                     "working_set_bytes": estimate.working_set_bytes,
                     "peak_memory_bytes": budget.peak_memory_bytes},
        )
