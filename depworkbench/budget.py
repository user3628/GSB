"""Admission planning under global and per-worker token budgets."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config import BudgetConfig
from .errors import (E_BUDGET_GLOBAL, E_BUDGET_SENTENCE, E_BUDGET_WORKER,
                     StructuredError)
from .shards import Shard


@dataclass
class ShardDecision:
    shard_index: int
    filename: str
    estimated_tokens: int
    admitted: bool
    worker_id: int | None
    reason_code: str | None
    reason_message: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_index": self.shard_index,
            "filename": self.filename,
            "estimated_tokens": self.estimated_tokens,
            "admitted": self.admitted,
            "worker_id": self.worker_id,
            "reason_code": self.reason_code,
            "reason_message": self.reason_message,
        }


@dataclass
class BudgetPlan:
    decisions: list[ShardDecision]
    global_budget: int
    worker_quota: int
    per_sentence_limit: int
    global_consumed: int
    worker_consumed: dict[int, int] = field(default_factory=dict)
    sentence_skips: list[StructuredError] = field(default_factory=list)

    @property
    def admitted_shards(self) -> list[ShardDecision]:
        return [d for d in self.decisions if d.admitted]

    @property
    def rejected_shards(self) -> list[ShardDecision]:
        return [d for d in self.decisions if not d.admitted]

    def to_dict(self) -> dict[str, Any]:
        return {
            "global_budget": self.global_budget,
            "worker_quota": self.worker_quota,
            "per_sentence_limit": self.per_sentence_limit,
            "global_consumed": self.global_consumed,
            "worker_consumed": dict(sorted(self.worker_consumed.items())),
            "decisions": [d.to_dict() for d in self.decisions],
            "sentence_skips": [e.to_dict() for e in self.sentence_skips],
        }


def plan_admission(shards: list[Shard], budget: BudgetConfig,
                   worker_threads: int) -> BudgetPlan:
    """Decide shard admission before any parsing work begins.

    Shards are offered to workers round-robin in shard order. A shard is
    admitted only when (a) every sentence fits the per-sentence limit,
    (b) the owning worker quota is not exceeded and (c) the remaining global
    budget covers the full estimate. Cumulative consumption can therefore
    never exceed the global budget.
    """
    decisions: list[ShardDecision] = []
    global_consumed = 0
    worker_consumed = {worker: 0 for worker in range(worker_threads)}
    sentence_skips: list[StructuredError] = []

    for shard in shards:
        worker_id = shard.index % worker_threads
        estimate = shard.token_count
        oversized = [
            (position, count)
            for position, count in enumerate(shard.sentence_token_counts)
            if count > budget.per_sentence_max_tokens
        ]
        if oversized:
            position, count = oversized[0]
            reason = StructuredError(
                E_BUDGET_SENTENCE,
                f"sentence {position} needs {count} tokens, "
                f"limit {budget.per_sentence_max_tokens}",
                {"shard_index": shard.index, "sentence_index": position,
                 "token_count": count, "limit": budget.per_sentence_max_tokens},
            )
            sentence_skips.append(reason)
            decisions.append(ShardDecision(
                shard.index, shard.filename, estimate, False, worker_id,
                reason.code, reason.message,
            ))
            continue
        if worker_consumed[worker_id] + estimate > budget.worker_token_quota:
            decisions.append(ShardDecision(
                shard.index, shard.filename, estimate, False, worker_id,
                E_BUDGET_WORKER,
                f"worker {worker_id} quota {budget.worker_token_quota} exhausted",
            ))
            continue
        if global_consumed + estimate > budget.global_total_tokens:
            decisions.append(ShardDecision(
                shard.index, shard.filename, estimate, False, worker_id,
                E_BUDGET_GLOBAL,
                f"global budget {budget.global_total_tokens} exhausted",
            ))
            continue
        worker_consumed[worker_id] += estimate
        global_consumed += estimate
        decisions.append(ShardDecision(
            shard.index, shard.filename, estimate, True, worker_id, None, None,
        ))

    return BudgetPlan(
        decisions=decisions, global_budget=budget.global_total_tokens,
        worker_quota=budget.worker_token_quota,
        per_sentence_limit=budget.per_sentence_max_tokens,
        global_consumed=global_consumed,
        worker_consumed=worker_consumed, sentence_skips=sentence_skips,
    )
