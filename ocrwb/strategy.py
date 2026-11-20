"""Data-scale driven detection/recognition strategy trade-off decisions.

For every shard the decision stage enumerates all three detection and three
recognition tiers, scores them against the group scale, character volume and
candidate count, applies the configured compute budgets and the rule that
heavy tiers are forbidden on the small group, then records the winning
combination, the per-strategy scoring breakdown and every rejected reason.
"""

from __future__ import annotations

from .config import (
    DETECT_HEAVY,
    DETECT_STRATEGIES,
    RECOGNIZE_HEAVY,
    RECOGNIZE_STRATEGIES,
    WorkbenchConfig,
)
from .detection import detect_cost
from .recognition import recognize_cost

DETECT_QUALITY = {"detect_fast": 0.55, "detect_balanced": 0.78, "detect_deep": 0.95}
RECOGNIZE_QUALITY = {"recog_fast": 0.45, "recog_balanced": 0.7, "recog_deep": 0.95}

GROUP_FIT = {
    "small": {
        "detect_fast": 1.0,
        "detect_balanced": 0.85,
        "detect_deep": 0.0,
        "recog_fast": 1.0,
        "recog_balanced": 0.85,
        "recog_deep": 0.0,
    },
    "medium": {
        "detect_fast": 0.8,
        "detect_balanced": 1.0,
        "detect_deep": 0.85,
        "recog_fast": 0.8,
        "recog_balanced": 1.0,
        "recog_deep": 0.85,
    },
    "large": {
        "detect_fast": 0.6,
        "detect_balanced": 0.9,
        "detect_deep": 1.0,
        "recog_fast": 0.6,
        "recog_balanced": 0.9,
        "recog_deep": 1.0,
    },
}


def _round(value: float, decimals: int = 6) -> float:
    return round(float(value), decimals)


def _detect_candidates(strategy_id: str, group: str, component_count: int, candidate_count: int, budget: int) -> dict:
    cost = detect_cost(strategy_id, component_count, candidate_count)
    heavy = strategy_id in DETECT_HEAVY
    rejected: list[str] = []
    if group == "small" and heavy:
        rejected.append("heavy_tier_forbidden_on_small_group")
    if cost > budget:
        rejected.append(f"cost_{cost}_exceeds_budget_{budget}")
    fit = GROUP_FIT[group][strategy_id]
    quality = DETECT_QUALITY[strategy_id]
    score = _round(0.6 * quality + 0.4 * fit) if not rejected else 0.0
    return {
        "strategy": strategy_id,
        "side": "detect",
        "cost": cost,
        "budget": budget,
        "quality": quality,
        "scale_fit": fit,
        "score": score,
        "rejected_reasons": rejected,
        "eligible": not rejected,
    }


def _recognize_candidates(
    strategy_id: str,
    group: str,
    candidate_count: int,
    template_count: int,
    cell_count: int,
    budget: int,
) -> dict:
    cost = recognize_cost(strategy_id, candidate_count, template_count, cell_count)
    heavy = strategy_id in RECOGNIZE_HEAVY
    rejected: list[str] = []
    if group == "small" and heavy:
        rejected.append("heavy_tier_forbidden_on_small_group")
    if cost > budget:
        rejected.append(f"cost_{cost}_exceeds_budget_{budget}")
    fit = GROUP_FIT[group][strategy_id]
    quality = RECOGNIZE_QUALITY[strategy_id]
    score = _round(0.6 * quality + 0.4 * fit) if not rejected else 0.0
    return {
        "strategy": strategy_id,
        "side": "recognize",
        "cost": cost,
        "budget": budget,
        "quality": quality,
        "scale_fit": fit,
        "score": score,
        "rejected_reasons": rejected,
        "eligible": not rejected,
    }


def decide_shard(
    group: str,
    char_count: int,
    component_count: int,
    candidate_count: int,
    template_count: int,
    cell_count: int,
    config: WorkbenchConfig,
) -> dict:
    """Produce the full deterministic strategy decision record for a shard."""
    detect_budget = config.budget.detect_budget(group)
    recognize_budget = config.budget.recognize_budget(group)
    detect_options = [
        _detect_candidates(strategy_id, group, component_count, candidate_count, detect_budget)
        for strategy_id in DETECT_STRATEGIES
    ]
    recognize_options = [
        _recognize_candidates(
            strategy_id, group, candidate_count, template_count, cell_count, recognize_budget
        )
        for strategy_id in RECOGNIZE_STRATEGIES
    ]

    eligible_detect = [option for option in detect_options if option["eligible"]]
    eligible_recognize = [option for option in recognize_options if option["eligible"]]
    if not eligible_detect or not eligible_recognize:
        return {
            "group": group,
            "char_count": char_count,
            "candidate_count": candidate_count,
            "detect_budget": detect_budget,
            "recognize_budget": recognize_budget,
            "detect_options": detect_options,
            "recognize_options": recognize_options,
            "error": "E_BUDGET_EXCEEDED",
        }

    # Exact score ties resolve to the lightest tier, keeping small-group
    # choices cost-conscious and every decision deterministic.
    detect_winner = max(
        eligible_detect, key=lambda option: (option["score"], -DETECT_STRATEGIES.index(option["strategy"]))
    )
    recognize_winner = max(
        eligible_recognize,
        key=lambda option: (option["score"], -RECOGNIZE_STRATEGIES.index(option["strategy"])),
    )

    reasons = [
        f"group={group} forbids heavy tiers" if group == "small" else f"group={group} permits heavy tiers",
        f"detection winner {detect_winner['strategy']} score={detect_winner['score']} cost={detect_winner['cost']}/{detect_budget}",
        f"recognition winner {recognize_winner['strategy']} score={recognize_winner['score']} cost={recognize_winner['cost']}/{recognize_budget}",
        f"observed chars={char_count} candidates={candidate_count} components={component_count} templates={template_count} cells={cell_count}",
    ]
    return {
        "group": group,
        "char_count": char_count,
        "candidate_count": candidate_count,
        "detect_budget": detect_budget,
        "recognize_budget": recognize_budget,
        "detect_strategy": detect_winner["strategy"],
        "recognize_strategy": recognize_winner["strategy"],
        "detect_cost": detect_winner["cost"],
        "recognize_cost": recognize_winner["cost"],
        "reasons": reasons,
        "detect_options": detect_options,
        "recognize_options": recognize_options,
    }
