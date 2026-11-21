"""Budget-aware algorithm selection for encoding and sample assembly."""

from __future__ import annotations

import math
from typing import Any

from .config import Config
from .errors import E_BUDGET_EXCEEDED, E_CROSS_LIMIT, E_INVALID_STAGE, PipelineError


def _numeric_slots(config: Config) -> int:
    return sum(len(spec.get("boundaries", [])) + 2 for spec in config.numeric_fields.values())


def _active_fields(config: Config, encoding_strategy: str, sampled_config: dict[str, Any]) -> list[str]:
    fields = list(config.categorical_fields)
    if encoding_strategy != "sampled_features":
        return fields
    ratio = float(sampled_config.get("category_field_ratio", 0.75))
    keep_count = max(1, min(len(fields), int(math.ceil(len(fields) * ratio))))
    weights = config.features.get("feature_weights", {})
    ranked = sorted(fields, key=lambda field: (-float(weights.get(field, 0.0)), field))
    kept = set(ranked[:keep_count])
    return [field for field in fields if field in kept]


def _cross_definitions(
    config: Config,
    declaration: dict[str, Any],
    encoding_strategy: str,
    active_fields: list[str],
    sampled_config: dict[str, Any],
) -> tuple[list[list[str]], list[dict[str, Any]]]:
    cross_limit = int(declaration["cross_limit"])
    hard_limit = int(config.features.get("crossing_limit_hard", 0))
    if cross_limit > hard_limit:
        raise PipelineError(
            E_CROSS_LIMIT,
            "declared cross_limit exceeds the global hard crossing limit",
            declared_cross_limit=cross_limit,
            crossing_limit_hard=hard_limit,
            scale=declaration.get("scale"),
        )
    active_set = set(active_fields)
    eligible: list[list[str]] = [
        list(pair)
        for pair in config.cross_candidates
        if all(field in active_set for field in pair)
    ]
    if encoding_strategy == "sampled_features":
        ratio = float(sampled_config.get("cross_ratio", 0.67))
        target = max(1, min(cross_limit, int(round(cross_limit * ratio)))) if cross_limit else 0
    else:
        target = min(cross_limit, len(eligible))
    chosen = eligible[:target]
    dropped = [
        {
            "definition": pair,
            "reason": "outside_cross_budget_or_inactive_field",
        }
        for pair in config.cross_candidates
        if pair not in chosen
    ]
    return chosen, dropped


def _plan_cost(
    config: Config,
    declaration: dict[str, Any],
    encoding_strategy: str,
    assembly_strategy: str,
    active_field_count: int,
    cross_count: int,
    *,
    sample_count: int | None = None,
) -> dict[str, float]:
    decision = config.decision
    weights = decision["weights"]
    n = int(sample_count if sample_count is not None else declaration["sample_count"])
    numeric_slots = _numeric_slots(config)
    cardinality_total = int(
        declaration.get("feature_cardinality", {}).get("total", 0)
    )
    per_sample = (
        float(weights["per_sample_base"])
        + float(weights["per_numeric_slot"]) * numeric_slots
        + float(weights["per_category_field"]) * active_field_count
        + float(weights["per_cross_feature"]) * cross_count
        + float(weights.get("per_cardinality_unit", 0.0)) * cardinality_total
    )
    normalize = n * float(weights["normalize_per_sample"])
    encode = n * per_sample * float(decision["encode_cost_multiplier"][encoding_strategy])
    assembly_per_sample = float(decision["assembly_cost_per_sample"][assembly_strategy])
    if assembly_strategy == "stratified_reservoir":
        assembly_rows = n * float(decision.get("reservoir_sample_ratio", 0.85))
    else:
        assembly_rows = n
    assembly = assembly_rows * assembly_per_sample
    total = normalize + encode + assembly
    return {
        "normalize": round(normalize, 6),
        "encode": round(encode, 6),
        "assemble": round(assembly, 6),
        "total": round(total, 6),
        "per_sample_encode": round(per_sample, 6),
        "numeric_slots": float(numeric_slots),
    }


def select_algorithm_plan(
    config: Config,
    declaration: dict[str, Any],
    normalized_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    decision = config.decision
    scale = str(declaration.get("scale"))
    budget = float(declaration["budget_units"])
    sampled_config = decision.get("sampled_features", {})
    max_loss = float(decision.get("max_acceptable_loss", 0.05))
    score_weights = decision.get("score_weights", {"cost": 0.8, "loss": 0.2})
    encoding_strategies = list(decision.get("encoding_strategies", []))
    assembly_strategies = list(decision.get("assembly_strategies", []))
    if not encoding_strategies or not assembly_strategies:
        raise PipelineError(
            E_INVALID_STAGE,
            "decision strategy lists must be non-empty",
            scale=scale,
        )

    candidates: list[dict[str, Any]] = []
    for encoding_index, encoding_strategy in enumerate(encoding_strategies):
        if encoding_strategy not in decision["encode_cost_multiplier"]:
            raise PipelineError(
                E_INVALID_STAGE,
                "encoding strategy has no cost multiplier",
                strategy=encoding_strategy,
            )
        active_fields = _active_fields(config, encoding_strategy, sampled_config)
        cross_definitions, dropped_crosses = _cross_definitions(
            config, declaration, encoding_strategy, active_fields, sampled_config
        )
        for assembly_index, assembly_strategy in enumerate(assembly_strategies):
            if assembly_strategy not in decision["assembly_cost_per_sample"]:
                raise PipelineError(
                    E_INVALID_STAGE,
                    "assembly strategy has no cost entry",
                    strategy=assembly_strategy,
                )
            costs = _plan_cost(
                config,
                declaration,
                encoding_strategy,
                assembly_strategy,
                len(active_fields),
                len(cross_definitions),
            )
            loss_bound = float(decision["loss_upper_bound"][encoding_strategy]) + float(
                decision["assembly_loss_upper_bound"][assembly_strategy]
            )
            cost_score = float(score_weights.get("cost", 0.8)) * (costs["total"] / budget)
            loss_score = float(score_weights.get("loss", 0.2)) * (loss_bound / max_loss)
            score = round(cost_score + loss_score, 10)
            candidate = {
                "encoding_strategy": encoding_strategy,
                "assembly_strategy": assembly_strategy,
                "active_categorical_fields": active_fields,
                "dropped_categorical_fields": [
                    field for field in config.categorical_fields if field not in active_fields
                ],
                "cross_definitions": cross_definitions,
                "cross_count": len(cross_definitions),
                "dropped_crosses": dropped_crosses,
                "expected_loss_upper_bound": round(loss_bound, 10),
                "estimated_cost_units": costs["total"],
                "cost_detail": costs,
                "score": score,
                "score_detail": {
                    "cost_component": round(cost_score, 10),
                    "loss_component": round(loss_score, 10),
                },
                "eligible": True,
                "selected": False,
                "eliminated_reason": None,
                "_order": (encoding_index, assembly_index),
            }

            guard = bool(decision.get("small_scale_strategy_guard", True))
            if guard and scale == "small" and (
                encoding_strategy != "full_exact" or assembly_strategy != "exact_materialize"
            ):
                candidate["eligible"] = False
                candidate["eliminated_reason"] = "small_scale_guard"
                candidate["elimination_detail"] = (
                    "small scale uses only full exact encoding and exact materialization; "
                    "batched/sampling strategies are reserved for larger scales"
                )
            elif loss_bound > max_loss:
                candidate["eligible"] = False
                candidate["eliminated_reason"] = "loss_bound_too_high"
                candidate["elimination_detail"] = (
                    f"expected loss upper bound {loss_bound} exceeds {max_loss}"
                )
            elif costs["total"] > budget:
                candidate["eligible"] = False
                candidate["eliminated_reason"] = "over_budget"
                candidate["elimination_detail"] = (
                    f"estimated cost {costs['total']} exceeds budget {budget}"
                )
            candidates.append(candidate)

    eligible = [candidate for candidate in candidates if candidate["eligible"]]
    if not eligible:
        raise PipelineError(
            E_BUDGET_EXCEEDED,
            "no strategy combination fits the declared budget and quality constraints",
            scale=scale,
            budget_units=budget,
            candidates=[
                {
                    "encoding_strategy": item["encoding_strategy"],
                    "assembly_strategy": item["assembly_strategy"],
                    "estimated_cost_units": item["estimated_cost_units"],
                    "reason": item["eliminated_reason"],
                }
                for item in candidates
            ],
        )
    selected = min(eligible, key=lambda item: (item["score"], item["_order"]))
    for candidate in candidates:
        if candidate is selected:
            candidate["selected"] = True
            candidate["eliminated_reason"] = None
            candidate["selection_detail"] = (
                "selected: lowest weighted score among budget-feasible candidates"
            )
        elif candidate["eligible"]:
            candidate["eliminated_reason"] = "higher_weighted_score"
            candidate["elimination_detail"] = (
                f"score {candidate['score']} is worse than selected score {selected['score']}"
            )

    for candidate in candidates:
        candidate.pop("_order", None)
    selected = dict(selected)
    selected.pop("_order", None)
    batch_sizes = decision.get("batch_size", {})
    batch_size = int(batch_sizes.get(scale, declaration["sample_count"]))
    if selected["encoding_strategy"] == "full_exact":
        batch_size = int(declaration["sample_count"])
    selected["batch_size"] = max(1, batch_size)

    budget_detail = {
        "declared_budget_units": budget,
        "normalize_estimated_units": selected["cost_detail"]["normalize"],
        "encoding_estimated_units": selected["cost_detail"]["encode"],
        "assembly_estimated_units": selected["cost_detail"]["assemble"],
        "total_estimated_units": selected["cost_detail"]["total"],
        "remaining_units": round(budget - selected["cost_detail"]["total"], 6),
        "within_budget": selected["cost_detail"]["total"] <= budget,
    }
    if not budget_detail["within_budget"]:
        raise PipelineError(
            E_BUDGET_EXCEEDED,
            "selected plan unexpectedly exceeds budget",
            scale=scale,
            **budget_detail,
        )

    return {
        "schema_version": 1,
        "scale": scale,
        "formula_version": decision.get("formula_version"),
        "observations": {
            "sample_count": declaration["sample_count"],
            "feature_cardinality": declaration["feature_cardinality"],
            "cross_limit": declaration["cross_limit"],
            "budget_units": declaration["budget_units"],
            "numeric_slots": _numeric_slots(config),
            "categorical_field_count": len(config.categorical_fields),
            "available_cross_candidates": len(config.cross_candidates),
            "normalized_summary": normalized_summary,
        },
        "selected": selected,
        "selected_reason": (
            "Lowest weighted cost/loss score among eligible combinations; the modeled "
            "total remains within the declared budget."
        ),
        "score_formula": "score = score_weights.cost*(cost/budget) + score_weights.loss*(loss_bound/max_acceptable_loss)",
        "budget": budget_detail,
        "candidate_evaluations": candidates,
    }


def budget_ledger(
    config: Config,
    declaration: dict[str, Any],
    plan: dict[str, Any],
    *,
    normalized_count: int,
    assembled_count: int,
    discarded_count: int,
) -> dict[str, Any]:
    costs = _plan_cost(
        config,
        declaration,
        plan["encoding_strategy"],
        plan["assembly_strategy"],
        len(plan["active_categorical_fields"]),
        len(plan["cross_definitions"]),
        sample_count=normalized_count,
    )
    budget = float(declaration["budget_units"])
    total = costs["total"]
    if total > budget:
        raise PipelineError(
            E_BUDGET_EXCEEDED,
            "actual modeled consumption exceeds the declared budget",
            scale=declaration.get("scale"),
            declared_budget_units=budget,
            total_consumed_units=total,
            cost_detail=costs,
        )
    return {
        "schema_version": 1,
        "scale": declaration.get("scale"),
        "formula_version": config.decision.get("formula_version"),
        "declared_budget_units": budget,
        "consumption": {
            "normalize_units": costs["normalize"],
            "encoding_units": costs["encode"],
            "assembly_units": costs["assemble"],
            "total_units": total,
            "remaining_units": round(budget - total, 6),
        },
        "counts": {
            "normalized_samples": normalized_count,
            "assembled_samples": assembled_count,
            "discarded_samples": discarded_count,
        },
        "within_budget": True,
    }
