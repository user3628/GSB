"""Stage 4: budget-aware strategy selection, commitment and aggregation.

A judgment is one clause-level risk judgment. Each candidate (clause, category)
carries a detailed score so the full candidate scoring ledger stays auditable,
while commitment operates at the clause unit to preserve score/level ordering.
"""
from __future__ import annotations

from .config import CATEGORY_KEYS, Config

STRATEGY_EXACT = "exact_full"
STRATEGY_KEYWORD = "keyword_priority_truncated"
STRATEGY_TIERED = "category_tiered_budget"
ALL_STRATEGIES = [STRATEGY_EXACT, STRATEGY_KEYWORD, STRATEGY_TIERED]


def assign_tier(clause_count: int, tiers: list[dict]) -> str:
    for tier in tiers:
        if clause_count <= int(tier["max_clauses"]):
            return tier["name"]
    return tiers[-1]["name"]


def build_candidates(scored: list[dict]) -> list[dict]:
    """One candidate per (clause, category), deterministic order."""
    candidates = []
    for record in scored:
        for cat in record["category_scores"]:
            candidates.append(
                {
                    "clause_id": record["clause_id"],
                    "category": cat["category"],
                    "score": cat["score"],
                    "basis": cat["basis"],
                    "has_hit": cat["basis"] not in ("no_keyword_hit", "UNKNOWN_KEYWORD_TABLE"),
                    "raw_level": record["level"],
                    "raw_dominant": record["dominant_category"],
                }
            )
    candidates.sort(key=lambda c: (c["clause_id"], c["category"]))
    return candidates


def _clause_top_score(scored_by_id: dict, clause_id: str) -> float:
    return scored_by_id[clause_id]["clause_score"]


def exact_commit(scored: list[dict]) -> set[str]:
    return {record["clause_id"] for record in scored if record["status"] == "ok"}


def keyword_commit(scored: list[dict], features: list[dict], budget: int) -> set[str]:
    hit_ids = {f["clause_id"] for f in features if f["hit_count"] > 0}
    ranked = [s for s in scored if s["clause_id"] in hit_ids]
    ranked.sort(
        key=lambda r: (-r["clause_score"], r["clause_id"]),
    )
    return {r["clause_id"] for r in ranked[:budget]}


def tiered_commit(scored: list[dict], features: list[dict], config: Config) -> set[str]:
    quotas = {key: config.quota(key) for key in CATEGORY_KEYS}
    remaining = dict(quotas)
    hit_ids = {f["clause_id"] for f in features if f["hit_count"] > 0}
    ranked = [s for s in scored if s["clause_id"] in hit_ids]
    ranked.sort(key=lambda r: (-r["clause_score"], r["clause_id"]))
    committed: set[str] = set()
    for record in ranked:
        category = record["dominant_category"]
        if remaining[category] <= 0:
            continue
        committed.add(record["clause_id"])
        remaining[category] -= 1
    # The tiered plan is the guaranteed-feasible fallback: never exceed budget.
    if len(committed) > int(config.budget["global_max_judgments"]):
        ordered = sorted(committed, key=lambda cid: (-_score_of(scored, cid), cid))
        committed = set(ordered[: int(config.budget["global_max_judgments"])])
    return committed


def _score_of(scored: list[dict], clause_id: str) -> float:
    for record in scored:
        if record["clause_id"] == clause_id:
            return record["clause_score"]
    return 0.0


def _quota_counts(committed: set[str], scored: list[dict]) -> dict[str, int]:
    by_id = {r["clause_id"]: r for r in scored}
    counts = {key: 0 for key in CATEGORY_KEYS}
    for clause_id in committed:
        counts[by_id[clause_id]["dominant_category"]] += 1
    return counts


def _in_quota(committed: set[str], scored: list[dict], config: Config) -> bool:
    counts = _quota_counts(committed, scored)
    return all(counts[key] <= config.quota(key) for key in CATEGORY_KEYS)


def select_strategy(
    scored: list[dict], features: list[dict], candidates: list[dict], config: Config, tier: str
) -> dict:
    budget = int(config.budget["global_max_judgments"])
    hit_candidates = [c for c in candidates if c["has_hit"]]
    plans = {
        STRATEGY_EXACT: exact_commit(scored),
        STRATEGY_KEYWORD: keyword_commit(scored, features, budget),
        STRATEGY_TIERED: tiered_commit(scored, features, config),
    }
    feasible: dict[str, list[str]] = {name: [] for name in ALL_STRATEGIES}
    for name, committed in plans.items():
        if len(committed) > budget:
            feasible[name].append(f"global_budget_exceeded:{len(committed)}>{budget}")
        if not _in_quota(committed, scored, config):
            feasible[name].append("category_quota_exceeded")

    chosen = next((name for name in ALL_STRATEGIES if not feasible[name]), STRATEGY_TIERED)
    rejected = {
        name: (";".join(feasible[name]) or "lower_priority")
        for name in ALL_STRATEGIES
        if name != chosen
    }
    rationale = {
        "tier": tier,
        "clause_count": len(scored),
        "candidate_count": len(candidates),
        "hit_candidate_count": len(hit_candidates),
        "global_budget": budget,
        "category_quotas": {key: config.quota(key) for key in CATEGORY_KEYS},
        "plan_judgment_counts": {name: len(committed) for name, committed in plans.items()},
        "plan_quota_ok": {
            name: _in_quota(committed, scored, config) for name, committed in plans.items()
        },
        "rejected_reasons": rejected,
        "chosen_strategy": chosen,
        "selection_rule": "exact_first_then_keyword_then_tiered_by_feasibility_and_priority",
    }
    return {"chosen": chosen, "committed": plans[chosen], "plans": plans, "rationale": rationale}


def apply_commitment(
    scored: list[dict],
    features: list[dict],
    committed: set[str],
    chosen: str,
    config: Config,
) -> list[dict]:
    feature_by_id = {f["clause_id"]: f for f in features}
    results = []
    for record in scored:
        clause_id = record["clause_id"]
        if record["status"] != "ok":
            level = int(config.scoring["default_level"])
            basis = "empty_text_default_low_risk"
        elif clause_id in committed:
            # Exact same level as the full precision stage => no direction contradiction.
            level = record["level"]
            basis = record["level_basis"]
        else:
            level = int(config.scoring["default_level"])
            basis = f"not_committed_under_{chosen}"
        feat = feature_by_id[clause_id]
        results.append(
            {
                "clause_id": clause_id,
                "contract_id": record.get("contract_id", ""),
                "level": level,
                "clause_score": record["clause_score"],
                "dominant_category": record["dominant_category"],
                "level_basis": basis,
                "committed": clause_id in committed and record["status"] == "ok",
                "human_level": record.get("human_level"),
                "diagnostics": list(record["diagnostics"]),
                "hit_count": feat["hit_count"],
                "negated_count": feat["negated_count"],
                "unknown_categories": [u["category"] for u in feat["unknown_categories"]],
            }
        )
    results.sort(key=lambda r: r["clause_id"])
    return results


def summarize(results: list[dict], decision: dict, tier: str, total_chars: int) -> dict:
    level_dist = {str(level): 0 for level in range(4)}
    category_dist = {key: 0 for key in CATEGORY_KEYS}
    for result in results:
        level_dist[str(result["level"])] += 1
        category_dist[result["dominant_category"]] += 1
    return {
        "tier": tier,
        "clause_count": len(results),
        "total_chars": total_chars,
        "strategy": decision["chosen"],
        "committed_judgments": len(decision["committed"]),
        "level_distribution": level_dist,
        "dominant_category_distribution": category_dist,
        "rationale_digest_keys": sorted(decision["rationale"].keys()),
    }


def run(scored: list[dict], features: list[dict], config: Config, total_chars: int) -> dict:
    candidates = build_candidates(scored)
    tier = assign_tier(len(scored), config.budget["tiers"])
    decision = select_strategy(scored, features, candidates, config, tier)
    budget = int(config.budget["global_max_judgments"])
    assert len(decision["committed"]) <= budget, "global budget invariant violated"
    assert _in_quota(decision["committed"], scored, config), "category quota invariant violated"
    results = apply_commitment(
        scored, features, decision["committed"], decision["chosen"], config
    )
    summary = summarize(results, decision, tier, total_chars)
    plan_membership = {
        name: sorted(committed) for name, committed in decision["plans"].items()
    }
    return {
        "tier": tier,
        "strategy_decision": decision["rationale"],
        "committed": sorted(decision["committed"]),
        "candidate_details": candidates,
        "plan_membership": plan_membership,
        "level_list": results,
        "summary": summary,
    }
