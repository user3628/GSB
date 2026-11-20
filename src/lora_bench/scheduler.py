"""Budget-based admission scheduling.

Three constraints govern admission:
  * ``max_workers``        - hard cap on concurrent worker processes;
  * ``per_task_sample_quota`` - no single task may consume more samples;
  * ``global_sample_cap``  - the running total may never exceed this value.

The scheduler never assumes a task will fit: consumption is estimated from
the configuration and the actual number of selected samples before admission,
and every rejection is persisted with a reason classification.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from .config import Budget, ExperimentConfig, TaskConfig
from .data import Sample
from .errors import BudgetExceededError

REASON_QUOTA = "per_task_quota_exceeded"
REASON_GLOBAL = "global_cap_exceeded"
REASON_NO_SAMPLES = "no_samples_selected"


@dataclass
class AdmissionDecision:
    task_id: str
    admitted: bool
    estimated_samples: int
    reason: str | None
    detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def estimated_sample_consumption(task: TaskConfig, selected_count: int) -> int:
    """Samples read for the whole run: batch * accumulation * steps.

    Reuse of samples (cycling) still counts as consumption against the quota.
    """
    if selected_count == 0:
        return 0
    return task.batch_size * task.grad_accum * task.max_steps


def select_task_samples(
    task: TaskConfig, samples: list[Sample]
) -> list[Sample]:
    if task.sample_ids:
        wanted = set(task.sample_ids)
        ordered = [sample for sample in samples if sample.id in wanted]
        found = {sample.id for sample in ordered}
        missing = wanted - found
        if missing:
            raise BudgetExceededError(
                f"task {task.task_id} references unknown samples {sorted(missing)}",
                task_id=task.task_id,
                field="sample_ids",
                detail={"missing": sorted(missing)},
            )
        return ordered
    return list(samples)


def admit_tasks(
    config: ExperimentConfig, samples: list[Sample]
) -> tuple[list[TaskConfig], list[AdmissionDecision], list[int]]:
    """Return admitted tasks, per-task decisions and per-task consumption."""
    budget: Budget = config.budget
    remaining_global = budget.global_sample_cap
    admitted: list[TaskConfig] = []
    decisions: list[AdmissionDecision] = []
    consumptions: list[int] = []

    for task in config.tasks:
        try:
            selected = select_task_samples(task, samples)
        except BudgetExceededError as exc:
            decisions.append(
                AdmissionDecision(task.task_id, False, 0, REASON_NO_SAMPLES, exc.to_dict())
            )
            continue
        estimate = estimated_sample_consumption(task, len(selected))
        if estimate == 0:
            decisions.append(
                AdmissionDecision(
                    task.task_id, False, 0, REASON_NO_SAMPLES, {"selected": 0}
                )
            )
            continue
        if estimate > budget.per_task_sample_quota:
            decisions.append(
                AdmissionDecision(
                    task.task_id,
                    False,
                    estimate,
                    REASON_QUOTA,
                    {
                        "quota": budget.per_task_sample_quota,
                        "estimate": estimate,
                    },
                )
            )
            continue
        if estimate > remaining_global:
            decisions.append(
                AdmissionDecision(
                    task.task_id,
                    False,
                    estimate,
                    REASON_GLOBAL,
                    {
                        "global_cap": budget.global_sample_cap,
                        "remaining": remaining_global,
                        "estimate": estimate,
                    },
                )
            )
            continue
        remaining_global -= estimate
        admitted.append(task)
        consumptions.append(estimate)
        decisions.append(
            AdmissionDecision(
                task.task_id,
                True,
                estimate,
                None,
                {"remaining_after": remaining_global},
            )
        )
    return admitted, decisions, consumptions


def assert_can_start(
    task: TaskConfig,
    estimate: int,
    budget: Budget,
    remaining_global: int,
) -> None:
    """Explicit start guard used by interactive start requests."""
    if estimate > budget.per_task_sample_quota:
        raise BudgetExceededError(
            f"task {task.task_id} estimate {estimate} exceeds per-task quota",
            task_id=task.task_id,
            detail={"estimate": estimate, "quota": budget.per_task_sample_quota},
        )
    if estimate > remaining_global:
        raise BudgetExceededError(
            f"task {task.task_id} estimate {estimate} exceeds remaining global budget",
            task_id=task.task_id,
            detail={
                "estimate": estimate,
                "remaining": remaining_global,
                "global_cap": budget.global_sample_cap,
            },
        )
