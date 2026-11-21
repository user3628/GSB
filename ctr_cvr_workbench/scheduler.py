"""Manifest-first shard admission under a global sample-round budget."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config import Config
from .data import ManifestShard


@dataclass
class AcceptedShard:
    shard_id: str
    index: int
    manifest_rows: int
    accepted_rows: int
    cost: int


@dataclass
class UnprocessedShard:
    shard_id: str
    index: int
    manifest_rows: int
    estimated_cost: int
    reason: str


@dataclass
class SchedulePlan:
    accepted: list[AcceptedShard] = field(default_factory=list)
    unprocessed: list[UnprocessedShard] = field(default_factory=list)
    consumed: int = 0
    budget: int = 0
    per_shard_sample_quota: int = 0
    workers_requested: int = 1
    workers_effective: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_global_sample_rounds": self.budget,
            "consumed_sample_rounds": self.consumed,
            "remaining_sample_rounds": self.budget - self.consumed,
            "per_shard_sample_quota": self.per_shard_sample_quota,
            "workers_requested": self.workers_requested,
            "workers_effective": self.workers_effective,
            "accepted": [
                {
                    "shard_id": item.shard_id,
                    "index": item.index,
                    "manifest_rows": item.manifest_rows,
                    "accepted_rows": item.accepted_rows,
                    "cost": item.cost,
                }
                for item in self.accepted
            ],
            "unprocessed": [
                {
                    "shard_id": item.shard_id,
                    "index": item.index,
                    "manifest_rows": item.manifest_rows,
                    "estimated_cost": item.estimated_cost,
                    "reason": item.reason,
                }
                for item in self.unprocessed
            ],
        }


def plan_shards(manifest_shards: list[ManifestShard], config: Config, *, worker_override: int | None = None) -> SchedulePlan:
    """Admit shards by manifest index using declared row counts only.

    Cost is `min(declared_rows, per_shard_sample_quota) * epochs` sample-rounds.
    A shard is never partially charged: if its full estimated cost does not fit,
    it is recorded as unprocessed and the next shard is considered.
    """

    budget = config.global_round_budget
    quota = config.shard_quota
    epochs = config.epochs
    workers_requested = config.max_workers if worker_override is None else int(worker_override)
    plan = SchedulePlan(budget=budget, per_shard_sample_quota=quota, workers_requested=workers_requested)
    if workers_requested <= 0:
        # Config validation normally catches this; direct callers get the same stable code.
        from .errors import ErrorCode, PipelineError

        raise PipelineError(
            ErrorCode.E_INVALID_PARAM,
            "max workers must be a positive integer",
            stage="train",
            details={"max_workers": workers_requested},
        )

    consumed = 0
    for shard in sorted(manifest_shards, key=lambda item: item.index):
        accepted_rows = min(shard.rows, quota)
        estimated_cost = accepted_rows * epochs
        if shard.rows <= 0:
            plan.unprocessed.append(
                UnprocessedShard(
                    shard_id=shard.shard_id,
                    index=shard.index,
                    manifest_rows=shard.rows,
                    estimated_cost=0,
                    reason="manifest row count is zero",
                )
            )
            continue
        if estimated_cost <= 0:
            plan.unprocessed.append(
                UnprocessedShard(
                    shard_id=shard.shard_id,
                    index=shard.index,
                    manifest_rows=shard.rows,
                    estimated_cost=estimated_cost,
                    reason="estimated cost is zero",
                )
            )
            continue
        if consumed + estimated_cost > budget:
            plan.unprocessed.append(
                UnprocessedShard(
                    shard_id=shard.shard_id,
                    index=shard.index,
                    manifest_rows=shard.rows,
                    estimated_cost=estimated_cost,
                    reason=(
                        "global sample-round budget would be exceeded: "
                        f"consumed={consumed}, shard_cost={estimated_cost}, budget={budget}"
                    ),
                )
            )
            continue
        consumed += estimated_cost
        plan.accepted.append(
            AcceptedShard(
                shard_id=shard.shard_id,
                index=shard.index,
                manifest_rows=shard.rows,
                accepted_rows=accepted_rows,
                cost=estimated_cost,
            )
        )

    plan.consumed = consumed
    plan.workers_effective = min(workers_requested, max(1, len(plan.accepted)))
    return plan
