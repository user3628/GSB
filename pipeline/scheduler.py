from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .data import BatchSpec, Dataset
from .errors import E_EVALUATION_BUDGET_EXCEEDED


@dataclass(frozen=True)
class BatchSchedule:
    batch: BatchSpec
    accepted: bool
    estimated_rows: int
    remaining_before: int
    reason: str | None = None


def plan_schedule(dataset: Dataset, budget: int, workers: int) -> dict[str, Any]:
    remaining = budget
    accepted: list[BatchSchedule] = []
    rejected: list[BatchSchedule] = []
    for batch in dataset.batches:
        estimated = batch.row_count
        if estimated > remaining:
            rejected.append(
                BatchSchedule(
                    batch=batch,
                    accepted=False,
                    estimated_rows=estimated,
                    remaining_before=remaining,
                    reason=E_EVALUATION_BUDGET_EXCEEDED,
                )
            )
            continue
        accepted.append(
            BatchSchedule(
                batch=batch,
                accepted=True,
                estimated_rows=estimated,
                remaining_before=remaining,
            )
        )
        remaining -= estimated
    return {
        "schema": "batch-schedule-v1",
        "evaluation_budget": budget,
        "requested_workers": workers,
        "effective_concurrency": min(workers, max(1, len(accepted))),
        "accepted_rows": sum(item.estimated_rows for item in accepted),
        "remaining_budget": remaining,
        "accepted": [
            {
                "batch_id": item.batch.batch_id,
                "batch_no": item.batch.batch_no,
                "estimated_rows": item.estimated_rows,
                "remaining_before": item.remaining_before,
            }
            for item in accepted
        ],
        "rejected": [
            {
                "batch_id": item.batch.batch_id,
                "batch_no": item.batch.batch_no,
                "estimated_rows": item.estimated_rows,
                "remaining_before": item.remaining_before,
                "reason": item.reason,
            }
            for item in rejected
        ],
    }


def accepted_batches(dataset: Dataset, budget: int) -> list[BatchSpec]:
    remaining = budget
    result: list[BatchSpec] = []
    for batch in dataset.batches:
        if batch.row_count > remaining:
            continue
        result.append(batch)
        remaining -= batch.row_count
    return result
