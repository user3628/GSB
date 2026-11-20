"""Stage 4: deterministic model ranking, tie arbitration, and selection."""

from __future__ import annotations

from typing import Any, Callable

from .util import (
    digest_json,
    fixed_from_number,
    fixed_to_string,
)


def _model_rejection_reasons(
    model: dict[str, Any], ranking_config: dict[str, Any]
) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    if model["total_score_micros"] is None:
        reasons.append(
            {
                "code": "insufficient_profile_evidence",
                "detail": {"evidence_status": model["evidence_status"]},
            }
        )
    if model["valid_samples"] < ranking_config["min_valid_samples"]:
        reasons.append(
            {
                "code": "below_min_valid_samples",
                "detail": {
                    "valid_samples": model["valid_samples"],
                    "minimum": ranking_config["min_valid_samples"],
                },
            }
        )
    if model["benchmark_count"] < ranking_config["min_benchmark_coverage"]:
        reasons.append(
            {
                "code": "below_min_benchmark_coverage",
                "detail": {
                    "benchmark_count": model["benchmark_count"],
                    "minimum": ranking_config["min_benchmark_coverage"],
                },
            }
        )
    return reasons


def _dimension_summary(model: dict[str, Any]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for dimension_id, dimension in sorted(model["dimensions"].items()):
        summary[dimension_id] = {
            "status": dimension["status"],
            "score_micros": dimension["score_micros"],
            "score": dimension["score"],
            "effective_weight_micros": dimension["effective_weight_micros"],
            "valid_samples_in_layers": dimension["valid_samples_in_layers"],
            "reasons": list(dimension["reasons"]),
        }
    return summary


def _tie_groups(eligible: list[dict[str, Any]], tolerance: int) -> list[list[dict[str, Any]]]:
    by_score = sorted(
        eligible,
        key=lambda model: (-model["total_score_micros"], model["model_id"]),
    )
    groups: list[list[dict[str, Any]]] = []
    index = 0
    while index < len(by_score):
        anchor = by_score[index]["total_score_micros"]
        end = index + 1
        while (
            end < len(by_score)
            and anchor - by_score[end]["total_score_micros"] <= tolerance
        ):
            end += 1
        group = by_score[index:end]
        group.sort(
            key=lambda model: (
                -model["valid_samples"],
                -model["benchmark_count"],
                model["model_id"],
            )
        )
        groups.append(group)
        index = end
    return groups


def run_stage4(
    stage3: dict[str, Any],
    config: dict[str, Any],
    dependency_fingerprint: str,
    *,
    on_unit: Callable[[], None] | None = None,
) -> dict[str, Any]:
    ranking_config = config["ranking"]
    tolerance = fixed_from_number(ranking_config["score_tolerance"])
    eligible: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for model in stage3["models"]:
        reasons = _model_rejection_reasons(model, ranking_config)
        if reasons:
            rejected.append(
                {
                    "model_id": model["model_id"],
                    "reason_codes": [reason["code"] for reason in reasons],
                    "reasons": reasons,
                    "total_score_micros": model["total_score_micros"],
                    "valid_samples": model["valid_samples"],
                    "benchmark_count": model["benchmark_count"],
                    "evidence_status": model["evidence_status"],
                }
            )
        else:
            eligible.append(model)
        if on_unit is not None:
            on_unit()

    groups = _tie_groups(eligible, tolerance)
    ranking: list[dict[str, Any]] = []
    tie_breakers: list[dict[str, Any]] = []
    rank = 1
    for group_index, group in enumerate(groups, 1):
        group_winner = group[0]
        score_span = group[0]["total_score_micros"] - group[-1]["total_score_micros"]
        criteria_used: set[str] = set()
        for group_position, model in enumerate(group):
            reasons: list[dict[str, Any]] = []
            if group_position == 0:
                reasons.append(
                    {
                        "code": "group_winner_by_score",
                        "detail": {"tie_group": group_index},
                    }
                )
            else:
                if model["valid_samples"] < group_winner["valid_samples"]:
                    code = "tie_broken_by_min_valid_samples"
                    criteria_used.add("min_valid_samples")
                    detail = {
                        "valid_samples": model["valid_samples"],
                        "winner_valid_samples": group_winner["valid_samples"],
                    }
                elif model["benchmark_count"] < group_winner["benchmark_count"]:
                    code = "tie_broken_by_benchmark_coverage"
                    criteria_used.add("benchmark_coverage")
                    detail = {
                        "benchmark_count": model["benchmark_count"],
                        "winner_benchmark_count": group_winner["benchmark_count"],
                    }
                elif model["model_id"] > group_winner["model_id"]:
                    code = "tie_broken_by_stable_model_id"
                    criteria_used.add("stable_model_id")
                    detail = {
                        "model_id": model["model_id"],
                        "winner_model_id": group_winner["model_id"],
                    }
                else:
                    code = "tie_broken_by_deterministic_order"
                    criteria_used.add("deterministic_order")
                    detail = {}
                reasons.append({"code": code, "detail": detail})
            selected = len(ranking) < ranking_config["top_k"]
            if not selected:
                reasons.append(
                    {
                        "code": "outside_top_k",
                        "detail": {"top_k": ranking_config["top_k"]},
                    }
                )
            ranking.append(
                {
                    "rank": rank,
                    "tie_group": group_index,
                    "model_id": model["model_id"],
                    "selected": selected,
                    "total_score_micros": model["total_score_micros"],
                    "total_score": fixed_to_string(model["total_score_micros"]),
                    "valid_samples": model["valid_samples"],
                    "benchmark_count": model["benchmark_count"],
                    "benchmarks": list(model["benchmarks"]),
                    "evidence_status": model["evidence_status"],
                    "dimensions": _dimension_summary(model),
                    "decision_reasons": reasons,
                }
            )
            if not selected:
                rejected.append(
                    {
                        "model_id": model["model_id"],
                        "reason_codes": ["outside_top_k"],
                        "reasons": reasons,
                        "total_score_micros": model["total_score_micros"],
                        "valid_samples": model["valid_samples"],
                        "benchmark_count": model["benchmark_count"],
                        "evidence_status": model["evidence_status"],
                    }
                )
            rank += 1
        tie_breakers.append(
            {
                "tie_group": group_index,
                "members": [model["model_id"] for model in group],
                "winner": group_winner["model_id"],
                "score_span_micros": score_span,
                "tolerance_micros": tolerance,
                "criteria_used": sorted(criteria_used) or ["scores_distinct_or_single_member"],
            }
        )

    selected_models = [item["model_id"] for item in ranking if item["selected"]]
    rationale = []
    if selected_models:
        rationale.append(
            {
                "code": "selected_top_ranked_models",
                "detail": {
                    "selected_model_ids": selected_models,
                    "top_k": ranking_config["top_k"],
                },
            }
        )
    else:
        rationale.append(
            {
                "code": "no_model_meets_eligibility_constraints",
                "detail": {
                    "min_valid_samples": ranking_config["min_valid_samples"],
                    "min_benchmark_coverage": ranking_config["min_benchmark_coverage"],
                },
            }
        )
    rationale.append(
        {
            "code": "tie_policy",
            "detail": {
                "score_tolerance": fixed_to_string(tolerance),
                "order": [
                    "higher_total_score",
                    "higher_min_valid_samples",
                    "higher_benchmark_coverage",
                    "lexicographically_smaller_model_id",
                ],
            },
        }
    )
    rejected.sort(key=lambda item: item["model_id"])
    return {
        "schema_version": 1,
        "stage": 4,
        "stage_name": "rank_and_select",
        "config_version": config["version"],
        "dependency_fingerprint": dependency_fingerprint,
        "upstream_stage3_digest": digest_json(stage3),
        "selection": {
            "selected_model_ids": selected_models,
            "winner": selected_models[0] if selected_models else None,
            "top_k": ranking_config["top_k"],
            "rationale": rationale,
        },
        "ranking": ranking,
        "tie_breakers": tie_breakers,
        "rejected": rejected,
        "stats": {
            "eligible_models": len(eligible),
            "rejected_models": len(rejected),
            "selected_models": len(selected_models),
        },
    }
