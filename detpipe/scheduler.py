"""Budget- and quota-aware shard admission.

The scheduler decides *before any candidate is evaluated* whether a shard may
run.  Two limits are enforced atomically under one lock:

* ``max_evaluations`` - global candidate evaluation budget
* ``worker_quota``    - per-worker candidate evaluation budget

A shard that cannot fit the global budget is permanently unprocessed
(``E_BUDGET_EXHAUSTED``); one that only exceeds the current worker's quota is
rejected for that worker (``E_WORKER_QUOTA``) and can be picked up by another
worker.  The lock guarantees cumulative usage can never exceed the budget and
that no worker can exceed its quota.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .errors import E_BUDGET_EXHAUSTED, E_WORKER_QUOTA, PipeError


@dataclass(frozen=True)
class ShardPlan:
    index: int
    image_ids: tuple[str, ...]
    candidate_count: int


@dataclass
class Admission:
    accepted: bool
    shard_index: int
    worker_id: int
    reason: str | None = None
    code: str | None = None
    detail: dict | None = None


@dataclass
class SchedulerState:
    used: int = 0
    per_worker_used: dict[int, int] = field(default_factory=dict)

    def snapshot(self) -> dict:
        return {
            "used": self.used,
            "per_worker_used": dict(sorted(self.per_worker_used.items())),
        }


def plan_shards(images: list[dict], shard_size: int) -> list[ShardPlan]:
    ordered = sorted(images, key=lambda img: img["image_id"])
    plans: list[ShardPlan] = []
    for start in range(0, len(ordered), shard_size):
        chunk = ordered[start:start + shard_size]
        plans.append(
            ShardPlan(
                index=start // shard_size,
                image_ids=tuple(img["image_id"] for img in chunk),
                candidate_count=sum(len(img["candidates"]) for img in chunk),
            )
        )
    return plans


class BudgetScheduler:
    def __init__(self, plans: list[ShardPlan], max_evaluations: int,
                 worker_quota: int, workers: int) -> None:
        self.plans = plans
        self.max_evaluations = max_evaluations
        self.worker_quota = worker_quota
        self.workers = workers
        self._lock = threading.Lock()
        self._state = SchedulerState(per_worker_used={w: 0 for w in range(workers)})

    def remaining_global(self) -> int:
        with self._lock:
            return self.max_evaluations - self._state.used

    def remaining_for_worker(self, worker_id: int) -> int:
        with self._lock:
            return self.worker_quota - self._state.per_worker_used[worker_id]

    def state_snapshot(self) -> dict:
        with self._lock:
            return self._state.snapshot()

    def request(self, plan: ShardPlan, worker_id: int) -> Admission:
        """Atomically reserve budget for ``plan`` on behalf of ``worker_id``."""
        with self._lock:
            worker_used = self._state.per_worker_used[worker_id]
            if worker_used + plan.candidate_count > self.worker_quota:
                return Admission(
                    accepted=False,
                    shard_index=plan.index,
                    worker_id=worker_id,
                    reason="worker quota would be exceeded",
                    code=E_WORKER_QUOTA,
                    detail={
                        "worker_used": worker_used,
                        "worker_quota": self.worker_quota,
                        "requested": plan.candidate_count,
                    },
                )
            if self._state.used + plan.candidate_count > self.max_evaluations:
                return Admission(
                    accepted=False,
                    shard_index=plan.index,
                    worker_id=worker_id,
                    reason="global evaluation budget would be exceeded",
                    code=E_BUDGET_EXHAUSTED,
                    detail={
                        "global_used": self._state.used,
                        "max_evaluations": self.max_evaluations,
                        "requested": plan.candidate_count,
                    },
                )
            self._state.used += plan.candidate_count
            self._state.per_worker_used[worker_id] += plan.candidate_count
            return Admission(
                accepted=True,
                shard_index=plan.index,
                worker_id=worker_id,
            )

    def rejection_error(self, admission: Admission) -> PipeError:
        return PipeError(
            admission.code or "E_UNKNOWN",
            admission.reason or "shard rejected",
            shard_index=admission.shard_index,
            details=admission.detail,
        )
