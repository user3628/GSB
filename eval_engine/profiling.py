"""Stage 3: fixed-point, language/task-type stratified capability profiles."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from .util import (
    WEIGHT_SCALE,
    digest_json,
    fixed_from_number,
    fixed_to_string,
    normalize_fixed_weights,
    rounded_ratio,
    weighted_fixed_mean,
)


def _layer_mean(records: list[dict[str, Any]]) -> int | None:
    if not records:
        return None
    return rounded_ratio(
        sum(record["score_micros"] for record in records), len(records)
    )


def _profile_model(
    model_id: str,
    model_records: list[dict[str, Any]],
    benchmark_ids: set[str],
    config: dict[str, Any],
    dimension_weights_all: dict[str, int],
) -> dict[str, Any]:
    profiling = config["profiling"]
    dimensions = profiling["dimensions"]
    records_by_layer: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in model_records:
        records_by_layer[(record["language"], record["question_type"])].append(record)

    dimension_results: dict[str, dict[str, Any]] = {}
    supported_dimension_parts: list[tuple[str, int, int]] = []
    for dimension_id, dimension_config in sorted(dimensions.items()):
        layer_results: list[dict[str, Any]] = []
        supported_config_weights: list[int] = []
        supported_means: list[int] = []
        insufficient_layer_reasons: list[str] = []
        total_layer_samples = 0
        configured_layers = sorted(dimension_config["layers"].items())
        for layer_key, configured_weight in configured_layers:
            language, question_type = layer_key.split(":", 1)
            layer_records = records_by_layer.get((language, question_type), [])
            count = len(layer_records)
            mean_micros = _layer_mean(layer_records)
            layer_supported = count >= profiling["min_samples_per_layer"]
            if not layer_supported:
                insufficient_layer_reasons.append(
                    f"layer_sample_count_below_minimum:{layer_key}"
                )
            layer_weight_fixed = fixed_from_number(configured_weight)
            layer_results.append(
                {
                    "layer": layer_key,
                    "language": language,
                    "question_type": question_type,
                    "configured_weight_micros": layer_weight_fixed,
                    "valid_samples": count,
                    "mean_score_micros": mean_micros,
                    "mean_score": fixed_to_string(mean_micros),
                    "supported": layer_supported,
                }
            )
            total_layer_samples += count
            if layer_supported and mean_micros is not None:
                supported_config_weights.append(layer_weight_fixed)
                supported_means.append(mean_micros)

        dimension_supported = (
            not insufficient_layer_reasons
            and total_layer_samples >= profiling["min_samples_per_dimension"]
        )
        reasons = list(insufficient_layer_reasons)
        if total_layer_samples < profiling["min_samples_per_dimension"]:
            reasons.append("dimension_sample_count_below_minimum")

        supported_score_micros = None
        if supported_means and sum(supported_config_weights) > 0:
            supported_weights = normalize_fixed_weights(supported_config_weights)
            supported_score_micros = weighted_fixed_mean(
                supported_means, supported_weights
            )
        score_micros = supported_score_micros if dimension_supported else None
        result = {
            "status": "ok" if dimension_supported else "insufficient_evidence",
            "score_micros": score_micros,
            "score": fixed_to_string(score_micros),
            "supported_score_micros": supported_score_micros,
            "configured_weight_micros": dimension_weights_all[dimension_id],
            "effective_weight_micros": 0,
            "valid_samples_in_layers": total_layer_samples,
            "configured_layer_count": len(configured_layers),
            "supported_layer_count": sum(1 for layer in layer_results if layer["supported"]),
            "reasons": reasons,
            "layers": layer_results,
        }
        dimension_results[dimension_id] = result
        if dimension_supported and score_micros is not None:
            supported_dimension_parts.append(
                (dimension_id, score_micros, dimension_weights_all[dimension_id])
            )

    if supported_dimension_parts:
        effective_weights = normalize_fixed_weights(
            [part[2] for part in supported_dimension_parts]
        )
        for (dimension_id, _, _), weight in zip(
            supported_dimension_parts, effective_weights
        ):
            dimension_results[dimension_id]["effective_weight_micros"] = weight
        total_score_micros = weighted_fixed_mean(
            [part[1] for part in supported_dimension_parts], effective_weights
        )
        evidence_status = (
            "sufficient"
            if len(supported_dimension_parts) == len(dimensions)
            else "partial"
        )
    else:
        total_score_micros = None
        evidence_status = "insufficient"

    coverage_notes: list[str] = []
    for dimension_id, result in sorted(dimension_results.items()):
        if result["status"] == "ok":
            coverage_notes.append(f"dimension_supported:{dimension_id}")
        else:
            coverage_notes.append(f"dimension_insufficient:{dimension_id}")
    return {
        "model_id": model_id,
        "total_score_micros": total_score_micros,
        "total_score": fixed_to_string(total_score_micros),
        "evidence_status": evidence_status,
        "valid_samples": len(model_records),
        "benchmark_count": len(benchmark_ids),
        "benchmarks": sorted(benchmark_ids),
        "dimensions": dimension_results,
        "dimension_effective_weight_sum_micros": sum(
            result["effective_weight_micros"]
            for result in dimension_results.values()
        ),
        "coverage_notes": coverage_notes,
    }


def run_stage3(
    stage2: dict[str, Any],
    config: dict[str, Any],
    dependency_fingerprint: str,
    *,
    on_unit: Callable[[], None] | None = None,
) -> dict[str, Any]:
    records_by_model: dict[str, list[dict[str, Any]]] = defaultdict(list)
    benchmarks_by_model: dict[str, set[str]] = defaultdict(set)
    for group in stage2["groups"]:
        if not group["valid_records"]:
            records_by_model.setdefault(group["model_id"], [])
            benchmarks_by_model.setdefault(group["model_id"], set())
            continue
        records_by_model[group["model_id"]].extend(group["valid_records"])
        benchmarks_by_model[group["model_id"]].add(group["benchmark_id"])

    dimensions = config["profiling"]["dimensions"]
    dimension_order = sorted(dimensions)
    dimension_weights = normalize_fixed_weights(
        [fixed_from_number(dimensions[key]["weight"]) for key in dimension_order]
    )
    dimension_weights_all = dict(zip(dimension_order, dimension_weights))
    if sum(dimension_weights_all.values()) != WEIGHT_SCALE:
        raise AssertionError("normalized dimension weights do not sum to the fixed scale")

    models: list[dict[str, Any]] = []
    for model_id in sorted(records_by_model):
        models.append(
            _profile_model(
                model_id,
                records_by_model[model_id],
                benchmarks_by_model[model_id],
                config,
                dimension_weights_all,
            )
        )
        if on_unit is not None:
            on_unit()

    return {
        "schema_version": 1,
        "stage": 3,
        "stage_name": "build_capability_profiles",
        "config_version": config["version"],
        "dependency_fingerprint": dependency_fingerprint,
        "upstream_stage2_digest": digest_json(stage2),
        "models": models,
        "stats": {
            "model_count": len(models),
            "evidence_sufficient_models": sum(
                model["evidence_status"] == "sufficient" for model in models
            ),
            "evidence_partial_models": sum(
                model["evidence_status"] == "partial" for model in models
            ),
            "evidence_insufficient_models": sum(
                model["evidence_status"] == "insufficient" for model in models
            ),
        },
    }
