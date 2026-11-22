from __future__ import annotations

import math
from typing import Any, Callable, Iterable

from .matching import analyze_shot
from .text import normalize_text


EXHAUSTIVE = "exhaustive"
KEYWORD_TRUNCATED = "keyword_priority_truncated"
CHUNK_INCREMENTAL = "chunk_incremental"
STRATEGIES = (EXHAUSTIVE, KEYWORD_TRUNCATED, CHUNK_INCREMENTAL)


def _seed_candidates(
    strategy: str,
    *,
    shot_count: int,
    total_chars: int,
    term_count: int,
    budget: float,
    config: dict[str, Any],
) -> dict[str, Any]:
    strategy_config = config.get("strategy", {})
    small_shot_threshold = int(strategy_config.get("small_shot_threshold", 12))
    small_char_threshold = int(strategy_config.get("small_char_threshold", 2500))
    truncation_min_shot_count = int(
        strategy_config.get("truncation_min_shot_count", 24)
    )
    truncation_min_chars = int(strategy_config.get("truncation_min_chars", 4000))
    chunk_shots = max(1, int(strategy_config.get("chunk_shots", 8)))
    chunk_overhead = float(strategy_config.get("chunk_overhead_cost", 60))
    term_weight = float(strategy_config.get("term_scan_cost", 0.75))
    char_weight = float(strategy_config.get("char_scan_cost", 4.0))

    exhaustive_cost = max(1.0, total_chars * (char_weight + term_count * term_weight))
    chunk_count = max(1, math.ceil(shot_count / chunk_shots))
    chunk_cost = exhaustive_cost * 0.72 + chunk_count * chunk_overhead
    truncated_cost = exhaustive_cost * 0.58 + shot_count * 8.0

    small_scale = (
        shot_count <= small_shot_threshold or total_chars <= small_char_threshold
    )
    if strategy == EXHAUSTIVE:
        cost = exhaustive_cost
        quality = 1.0
        scale_fit = 1.0 if small_scale else 0.70
        eligible = True
        ineligible_reason = None
    elif strategy == CHUNK_INCREMENTAL:
        cost = chunk_cost
        quality = 0.93
        scale_fit = 0.92 if small_scale else 1.0
        eligible = True
        ineligible_reason = None
    elif strategy == KEYWORD_TRUNCATED:
        cost = truncated_cost
        quality = 0.80
        scale_fit = 1.0
        eligible = (
            not small_scale
            and (
                shot_count >= truncation_min_shot_count
                or total_chars >= truncation_min_chars
            )
        )
        ineligible_reason = (
            None
            if eligible
            else "ineligible_small_scale_truncation_is_for_large_inputs_only"
        )
    else:
        raise ValueError(f"unknown strategy: {strategy}")

    budget_fit = 1.0 if cost <= budget else max(0.0, budget / cost)
    score = 0.55 * quality + 0.35 * budget_fit + 0.10 * scale_fit
    if not eligible:
        score = 0.0
    return {
        "strategy": strategy,
        "eligible": eligible,
        "estimated_cost": round(cost, 6),
        "budget_fit": round(budget_fit, 6),
        "quality_score": round(quality, 6),
        "scale_fit": round(scale_fit, 6),
        "score": round(score, 6),
        "ineligible_reason": ineligible_reason,
        "rejection_reason": ineligible_reason,
    }


def choose_strategy(
    shots: Iterable[dict[str, Any]],
    subject_table: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    shot_list = list(shots)
    shot_count = len(shot_list)
    total_chars = sum(
        len(normalize_text(str(shot.get("prompt", "")))) for shot in shot_list
    )
    negative_count = sum(len(shot.get("negative_terms", [])) for shot in shot_list)
    table_shots = subject_table.get("shots", {})
    term_count = 0
    for shot in shot_list:
        table_entry = table_shots.get(shot["shot_id"], {})
        term_count += len(table_entry.get("required_subjects", []))
        term_count += len(table_entry.get("forbidden_subjects", []))
        term_count += len(shot.get("must_scenes", []))
        term_count += len(shot.get("negative_terms", []))

    budget = float(
        config.get("strategy", {}).get(
            "check_depth_budget",
            config.get("strategy", {}).get("budget", 600_000),
        )
    )
    if budget <= 0:
        raise ValueError("strategy check depth budget must be positive")

    forced = config.get("strategy", {}).get("force_strategy")
    metrics = {
        "shot_count": shot_count,
        "total_prompt_chars": total_chars,
        "negative_constraint_count": negative_count,
        "keyword_count": term_count,
        "check_depth_budget": budget,
    }
    if forced:
        if forced not in STRATEGIES:
            raise ValueError(f"unknown forced strategy: {forced}")
        candidates = [
            _seed_candidates(
                strategy,
                shot_count=shot_count,
                total_chars=total_chars,
                term_count=term_count,
                budget=budget,
                config=config,
            )
            for strategy in STRATEGIES
        ]
        selected = next(item for item in candidates if item["strategy"] == forced)
        selected["eligible"] = True
        selected["ineligible_reason"] = None
        selected["rejection_reason"] = None
        for candidate in candidates:
            if candidate["strategy"] != forced:
                candidate["status"] = "rejected"
                candidate["rejection_reason"] = "overridden_for_audit_or_testing"
            else:
                candidate["status"] = "selected"
        return {
            "strategy": forced,
            "selected_reason": "forced_by_configuration",
            "metrics": metrics,
            "candidates": candidates,
        }

    candidates = [
        _seed_candidates(
            strategy,
            shot_count=shot_count,
            total_chars=total_chars,
            term_count=term_count,
            budget=budget,
            config=config,
        )
        for strategy in STRATEGIES
    ]
    priority = {EXHAUSTIVE: 0, CHUNK_INCREMENTAL: 1, KEYWORD_TRUNCATED: 2}
    selected = sorted(
        candidates,
        key=lambda item: (
            not item["eligible"],
            -item["score"],
            priority[item["strategy"]],
        ),
    )[0]

    for candidate in candidates:
        if candidate is selected:
            candidate["status"] = "selected"
            continue
        candidate["status"] = "rejected"
        if not candidate["eligible"]:
            candidate["rejection_reason"] = candidate["ineligible_reason"]
        elif candidate["estimated_cost"] > budget:
            candidate["rejection_reason"] = "estimated_cost_exceeds_budget"
        else:
            candidate["rejection_reason"] = "lower_weighted_candidate_score"

    small_scale = (
        shot_count <= int(config.get("strategy", {}).get("small_shot_threshold", 12))
        or total_chars
        <= int(config.get("strategy", {}).get("small_char_threshold", 2500))
    )
    if selected["strategy"] == EXHAUSTIVE and small_scale:
        reason = "small_scale_prefers_exact_exhaustive"
    elif selected["strategy"] == EXHAUSTIVE:
        reason = "budget_allows_exact_exhaustive"
    elif selected["strategy"] == CHUNK_INCREMENTAL:
        reason = "balanced_cost_and_exactness"
    else:
        reason = "budget_requires_keyword_priority_truncation"
    return {
        "strategy": selected["strategy"],
        "selected_reason": reason,
        "metrics": metrics,
        "candidates": candidates,
    }


def _candidate_units_for_truncated(
    shot: dict[str, Any],
    subject_entry: dict[str, Any],
    units: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    terms = [
        *subject_entry.get("required_subjects", []),
        *subject_entry.get("forbidden_subjects", []),
        *shot.get("must_scenes", []),
        *shot.get("negative_terms", []),
    ]
    folded_terms = [normalize_text(str(term)).casefold() for term in terms if term]
    selected = [
        unit
        for unit in units
        if any(term in unit["text"].casefold() for term in folded_terms)
    ]
    return selected


def run_strategy(
    strategy: str,
    jobs: list[dict[str, Any]],
    config: dict[str, Any],
    *,
    after_shot: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    """Execute a strategy. All strategies are decision-equivalent."""
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy: {strategy}")
    chunk_shots = max(1, int(config.get("strategy", {}).get("chunk_shots", 8)))
    decisions: dict[str, dict[str, Any]] = {}
    stats = {
        "strategy": strategy,
        "shot_count": len(jobs),
        "chunks": 0,
        "visited_unit_count": 0,
        "skipped_unit_count": 0,
    }
    if strategy == CHUNK_INCREMENTAL:
        for chunk_start in range(0, len(jobs), chunk_shots):
            chunk = jobs[chunk_start : chunk_start + chunk_shots]
            stats["chunks"] += 1
            for job in chunk:
                units = job["normalize_result"]["units"]
                decision = analyze_shot(
                    shot=job["shot"],
                    subject_entry=job["subject_entry"],
                    reference_shot=job["reference_shot"],
                    normalize_result=job["normalize_result"],
                    duration_result=job["duration_result"],
                    config=config,
                )
                decisions[job["shot"]["shot_id"]] = decision
                stats["visited_unit_count"] += len(units)
                if after_shot is not None:
                    after_shot(len(decisions), job["shot"]["shot_id"])
        return {"decisions": decisions, "stats": stats}

    for job in jobs:
        units = job["normalize_result"]["units"]
        candidate_units = units
        if strategy == KEYWORD_TRUNCATED:
            candidate_units = _candidate_units_for_truncated(
                job["shot"], job["subject_entry"], units
            )
        decision = analyze_shot(
            shot=job["shot"],
            subject_entry=job["subject_entry"],
            reference_shot=job["reference_shot"],
            normalize_result=job["normalize_result"],
            duration_result=job["duration_result"],
            config=config,
            candidate_units=candidate_units,
        )
        decisions[job["shot"]["shot_id"]] = decision
        stats["visited_unit_count"] += len(candidate_units)
        stats["skipped_unit_count"] += len(units) - len(candidate_units)
        if after_shot is not None:
            after_shot(len(decisions), job["shot"]["shot_id"])
    return {"decisions": decisions, "stats": stats}
