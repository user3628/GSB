"""Deterministic three-budget task scheduler.

Admission is based on operation-step cost before execution.  Real wall-clock
latency is measured after each admitted task completes; thread timeouts are
never used to decide the latency budget.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence


@dataclass(frozen=True)
class TaskEstimate:
    task_id: str
    estimated_steps: int


@dataclass
class ScheduledTask:
    task_id: str
    estimated_steps: int
    lane: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "estimated_steps": self.estimated_steps,
            "lane": self.lane,
        }


@dataclass
class RejectedTask:
    task_id: str
    reason: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"task_id": self.task_id, "reason": self.reason, "details": dict(self.details)}


@dataclass
class SchedulingPlan:
    total_step_budget: int
    worker_step_quota: int
    max_workers: int
    accepted: list[ScheduledTask]
    rejected: list[RejectedTask]
    lane_loads: list[int]
    total_estimated_steps: int

    @property
    def lanes(self) -> dict[int, list[ScheduledTask]]:
        result: dict[int, list[ScheduledTask]] = {}
        for task in self.accepted:
            result.setdefault(task.lane, []).append(task)
        return result

    @property
    def planned_parallelism(self) -> int:
        return len(self.lanes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "gspipe-scheduling-plan-v1",
            "total_step_budget": self.total_step_budget,
            "worker_step_quota": self.worker_step_quota,
            "max_workers": self.max_workers,
            "acceptance_order": "task_id_ascending",
            "accepted": [item.to_dict() for item in self.accepted],
            "rejected": [item.to_dict() for item in self.rejected],
            "lane_loads": list(self.lane_loads),
            "total_estimated_steps": self.total_estimated_steps,
            "planned_parallelism": self.planned_parallelism,
        }


def build_schedule(estimates: Sequence[TaskEstimate], scheduler_config: dict[str, Any]) -> SchedulingPlan:
    total_budget = int(scheduler_config["total_step_budget"])
    quota = int(scheduler_config["worker_step_quota"])
    workers = int(scheduler_config["max_workers"])
    if total_budget < 1 or quota < 1 or workers < 1:
        raise ValueError("scheduler budgets and max_workers must be positive")

    accepted: list[ScheduledTask] = []
    rejected: list[RejectedTask] = []
    loads = [0] * workers
    total_used = 0

    for estimate in sorted(estimates, key=lambda item: item.task_id):
        steps = int(estimate.estimated_steps)
        if steps < 1:
            steps = 1
        if steps > quota:
            rejected.append(
                RejectedTask(
                    estimate.task_id,
                    "WORKER_STEP_QUOTA",
                    {"estimated_steps": steps, "worker_step_quota": quota, "phase": "single_task"},
                )
            )
            continue
        available = [index for index, load in enumerate(loads) if load + steps <= quota]
        if not available:
            rejected.append(
                RejectedTask(
                    estimate.task_id,
                    "WORKER_STEP_QUOTA",
                    {
                        "estimated_steps": steps,
                        "worker_step_quota": quota,
                        "lane_loads": list(loads),
                        "phase": "no_lane_capacity",
                    },
                )
            )
            continue
        lane = min(available, key=lambda index: (loads[index], index))
        if total_used + steps > total_budget:
            rejected.append(
                RejectedTask(
                    estimate.task_id,
                    "TOTAL_STEP_BUDGET",
                    {
                        "estimated_steps": steps,
                        "total_step_budget": total_budget,
                        "already_admitted_steps": total_used,
                    },
                )
            )
            continue
        loads[lane] += steps
        total_used += steps
        accepted.append(ScheduledTask(estimate.task_id, steps, lane))

    return SchedulingPlan(
        total_step_budget=total_budget,
        worker_step_quota=quota,
        max_workers=workers,
        accepted=accepted,
        rejected=rejected,
        lane_loads=loads,
        total_estimated_steps=total_used,
    )
