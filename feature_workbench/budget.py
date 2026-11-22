from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .dataset import ShardSpec
from .errors import WorkbenchError
from .util import digest_object, to_jsonable


def estimate_shard_evaluations(spec: ShardSpec) -> dict[str, Any]:
    categorical_count = len(spec.categorical_cardinalities)
    categorical_width = sum(int(value) for value in spec.categorical_cardinalities.values())
    numeric_width = max(0, spec.declared_columns - 1 - categorical_count)
    encoded_width_estimate = numeric_width + categorical_width
    estimated = max(1, int(spec.declared_rows) * max(1, encoded_width_estimate))
    return {
        "rows": int(spec.declared_rows),
        "columns": int(spec.declared_columns),
        "numeric_columns": numeric_width,
        "categorical_columns": categorical_count,
        "categorical_width": categorical_width,
        "encoded_width_estimate": encoded_width_estimate,
        "estimated_evaluations": estimated,
    }


@dataclass
class BudgetLedger:
    global_limit: int
    used: int = 0
    reservations: dict[str, int] = field(default_factory=dict)

    @property
    def remaining(self) -> int:
        return max(0, self.global_limit - self.used)

    def charge(self, amount: int, unit_id: str) -> int:
        if amount < 0 or isinstance(amount, bool):
            raise WorkbenchError(
                "E_BUDGET_EXCEEDED",
                "Evaluation charge must be a non-negative integer",
                {"unit_id": unit_id, "amount": amount},
            )
        if self.used + amount > self.global_limit:
            raise WorkbenchError(
                "E_BUDGET_EXCEEDED",
                "Global feature evaluation budget would be exceeded",
                {
                    "unit_id": unit_id,
                    "requested": amount,
                    "used": self.used,
                    "limit": self.global_limit,
                    "remaining": self.remaining,
                },
            )
        self.used += amount
        self.reservations[unit_id] = self.reservations.get(unit_id, 0) + amount
        return self.used


def plan_schedule(specs: Iterable[ShardSpec], config: dict[str, Any]) -> dict[str, Any]:
    budget = config["budget"]
    global_limit = int(budget["global_feature_evaluations"])
    per_worker_quota = int(budget["per_worker_evaluations"])
    max_workers = int(budget["max_workers"])
    ledger = BudgetLedger(global_limit=global_limit)
    lanes: list[dict[str, Any]] = []
    admitted: list[dict[str, Any]] = []
    unprocessed: list[dict[str, Any]] = []

    for spec in sorted(specs, key=lambda item: item.shard_index):
        estimate = estimate_shard_evaluations(spec)
        cost = int(estimate["estimated_evaluations"])
        public = {
            "shard_index": spec.shard_index,
            "file": spec.file,
            "tier": spec.tier,
            "rows": spec.declared_rows,
            "columns": spec.declared_columns,
            "estimate": estimate,
        }
        if cost > per_worker_quota:
            unprocessed.append(
                {
                    **public,
                    "reason": "PER_WORKER_QUOTA",
                    "detail": "estimated evaluations exceed one worker process quota",
                    "estimated_evaluations": cost,
                    "per_worker_quota": per_worker_quota,
                }
            )
            continue
        if cost > ledger.remaining:
            unprocessed.append(
                {
                    **public,
                    "reason": "GLOBAL_BUDGET",
                    "detail": "estimated evaluations exceed the remaining global budget",
                    "estimated_evaluations": cost,
                    "remaining_before": ledger.remaining,
                }
            )
            continue
        lane_index = next(
            (
                index
                for index, lane in enumerate(lanes)
                if int(lane["used"]) + cost <= per_worker_quota
            ),
            None,
        )
        if lane_index is None:
            if len(lanes) < max_workers:
                lane_index = len(lanes)
                lanes.append({"lane": lane_index, "used": 0, "shards": []})
            else:
                unprocessed.append(
                    {
                        **public,
                        "reason": "PER_WORKER_QUOTA",
                        "detail": "all configured worker lanes have insufficient remaining quota",
                        "estimated_evaluations": cost,
                        "per_worker_quota": per_worker_quota,
                        "max_workers": max_workers,
                    }
                )
                continue
        lane = lanes[lane_index]
        ledger.charge(cost, f"shard:{spec.shard_index}")
        lane["used"] = int(lane["used"]) + cost
        lane["shards"].append(spec.shard_index)
        admitted.append(
            {
                **public,
                "lane": lane_index,
                "estimated_evaluations": cost,
            }
        )

    if ledger.used > global_limit:
        raise AssertionError("schedule admission exceeded the global budget")
    plan: dict[str, Any] = {
        "schema_version": 1,
        "cost_model": "declared_rows * max(1, numeric_columns + sum(categorical_cardinality))",
        "admission_order": [entry["shard_index"] for entry in admitted],
        "global_budget": global_limit,
        "per_worker_quota": per_worker_quota,
        "max_workers": max_workers,
        "query_latency_budget_ms": float(budget["query_latency_ms"]),
        "global_used": ledger.used,
        "global_remaining": ledger.remaining,
        "concurrency": len(lanes),
        "lanes": lanes,
        "admitted": admitted,
        "unprocessed": unprocessed,
        "reservations": ledger.reservations,
    }
    plan = to_jsonable(plan)
    plan["plan_digest"] = digest_object(plan)
    return plan


def plan_admitted_specs(specs: list[ShardSpec], plan: dict[str, Any]) -> list[ShardSpec]:
    admitted_indices = {int(entry["shard_index"]) for entry in plan["admitted"]}
    return [spec for spec in sorted(specs, key=lambda item: item.shard_index) if spec.shard_index in admitted_indices]

