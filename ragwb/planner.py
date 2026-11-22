"""Scale-aware retrieval strategy planning under a global evaluation budget.

Three deterministic strategies exist:

* ``exact_full``       - evaluate every paragraph (small scale only),
* ``layered_truncate`` - evaluate a capped number of paragraphs per turn,
* ``focus_first``      - evaluate only the tightest focus-compatible cap.

The scale tier is derived from paragraph count and session turn count.  The
small tier is never allowed to adopt a large-scale truncation depth.  Every
decision records why the chosen strategy was selected and why the alternatives
were rejected, and the number of evaluations can never exceed the configured
global budget.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List

STRATEGY_EXACT = "exact_full"
STRATEGY_LAYERED = "layered_truncate"
STRATEGY_FOCUS = "focus_first"
ALL_STRATEGIES = (STRATEGY_EXACT, STRATEGY_LAYERED, STRATEGY_FOCUS)


@dataclass
class BudgetLedger:
    """In-memory evaluation ledger; rebuilt from checkpoints on recovery."""

    budget: int
    consumed: int = 0
    history: List[Dict[str, int]] = field(default_factory=list)

    @property
    def remaining(self) -> int:
        return self.budget - self.consumed

    def charge(self, session_id: str, turn_index: int,
               amount: int) -> Dict[str, Any]:
        allowed = min(max(amount, 0), self.remaining)
        self.consumed += allowed
        entry = {
            "session_id": session_id,
            "turn_index": turn_index,
            "requested": amount,
            "charged": allowed,
            "consumed_total": self.consumed,
        }
        self.history.append(entry)
        return dict(entry)

    def to_dict(self) -> Dict[str, Any]:
        return {"budget": self.budget, "consumed": self.consumed,
                "history": self.history}

    @classmethod
    def from_checkpoints(cls, budget: int,
                         records: List[Dict[str, Any]]) -> "BudgetLedger":
        ledger = cls(budget=budget)
        for record in sorted(
            records, key=lambda row: (row["session_id"], row["turn_index"],
                                      row["stage_seq"])
        ):
            detail = record.get("product_summary", {}) \
                .get("budget_charge")
            if detail:
                ledger.consumed += int(detail["charged"])
                ledger.history.append(dict(detail))
        ledger.consumed = min(ledger.consumed, budget)
        return ledger


def _tier_for(config: Dict[str, Any], paragraph_count: int,
              turn_count: int) -> str:
    tiers = config["scale_tiers"]
    small = tiers["small"]
    medium = tiers["medium"]
    if paragraph_count <= small["max_paragraphs"] and \
            turn_count <= small["max_turns"]:
        return "small"
    if paragraph_count <= medium["max_paragraphs"] and \
            turn_count <= medium["max_turns"]:
        return "medium"
    return "large"


def _strategy_cost(strategy: str, paragraph_count: int,
                   config: Dict[str, Any]) -> int:
    tiers = config["scale_tiers"]
    if strategy == STRATEGY_EXACT:
        return paragraph_count
    if strategy == STRATEGY_LAYERED:
        return min(paragraph_count, int(tiers["medium"]["truncate_depth"]))
    return min(paragraph_count, int(tiers["large"]["truncate_depth"]))


def plan_turn(config: Dict[str, Any], *, paragraph_count: int,
              turn_count: int, remaining_budget: int,
              focus_present: bool) -> Dict[str, Any]:
    """Return a fully justified strategy decision for one turn."""
    tier = _tier_for(config, paragraph_count, turn_count)
    tier_strategy = config["scale_tiers"][tier]["strategy"]
    rejected: List[Dict[str, str]] = []

    costs = {strategy: _strategy_cost(strategy, paragraph_count, config)
             for strategy in ALL_STRATEGIES}

    # Enforce the small-scale guard independently of budget pressure.
    if tier == "small":
        for strategy in (STRATEGY_LAYERED, STRATEGY_FOCUS):
            rejected.append({
                "strategy": strategy,
                "reason": "small_tier_forbids_large_scale_truncation_depth",
            })

    chosen = tier_strategy
    reason = f"tier_{tier}_default_strategy"
    if costs[chosen] > remaining_budget:
        # Fall back to progressively cheaper strategies.
        fallback_order = [
            strategy for strategy in (
                STRATEGY_LAYERED, STRATEGY_FOCUS)
            if strategy != chosen
        ]
        if chosen != STRATEGY_EXACT:
            fallback_order = [STRATEGY_FOCUS]
        rejected.append({
            "strategy": chosen,
            "reason": (f"estimated_evaluations_{costs[chosen]}"
                       f"_exceed_remaining_budget_{remaining_budget}"),
        })
        downgraded = False
        for candidate in fallback_order:
            if costs[candidate] <= remaining_budget:
                chosen = candidate
                reason = ("budget_pressure_downgrade_from_"
                          f"{tier_strategy}")
                downgraded = True
                break
            rejected.append({
                "strategy": candidate,
                "reason": (f"estimated_evaluations_{costs[candidate]}"
                           f"_exceed_remaining_budget_{remaining_budget}"),
            })
        if not downgraded:
            chosen = STRATEGY_FOCUS
            reason = "budget_pressure_clamped_to_cheapest_strategy"

    planned_evaluations = min(costs[chosen], max(remaining_budget, 0))
    clamped = planned_evaluations < costs[chosen]
    if clamped:
        reason += ";clamped_to_remaining_budget"

    return {
        "tier": tier,
        "strategy": chosen,
        "reason": reason,
        "focus_present": focus_present,
        "estimated_cost": costs[chosen],
        "planned_evaluations": planned_evaluations,
        "remaining_budget_before": remaining_budget,
        "strategy_costs": costs,
        "rejected_strategies": rejected,
    }


def evaluation_pool(strategy: str, docs: List[Dict[str, Any]],
                    query_tokens: set, limit: int) -> List[Dict[str, Any]]:
    """Deterministically select which paragraphs a strategy may evaluate.

    ``exact_full`` uses every paragraph; truncating strategies first order by
    a cheap deterministic pre-rank (token overlap then doc_id) and cap the
    pool, while ``focus_first`` keeps only paragraphs sharing at least one
    query token before applying the tightest cap.
    """
    ordered = sorted(docs, key=lambda doc: doc["doc_id"])
    if strategy == STRATEGY_EXACT:
        return ordered

    def overlap_count(doc: Dict[str, Any]) -> int:
        from .text import token_set
        return len(query_tokens & token_set(doc["text"]))

    if strategy == STRATEGY_FOCUS:
        focused = [doc for doc in ordered if overlap_count(doc) > 0]
        ranked = sorted(focused,
                        key=lambda doc: (-overlap_count(doc), doc["doc_id"]))
        return ranked[:limit]

    ranked = sorted(ordered,
                    key=lambda doc: (-overlap_count(doc), doc["doc_id"]))
    return ranked[:limit]


def safe_log(value: float) -> float:
    result = math.log(value)
    if not math.isfinite(result):
        raise ValueError("non-finite log component")
    return result
