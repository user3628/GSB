from __future__ import annotations

import math
from decimal import Decimal, localcontext
from typing import Any

import numpy as np


def grade_for_score(score: float, thresholds: list[float]) -> int:
    for grade, threshold in enumerate(thresholds, 1):
        if score < float(threshold):
            return grade - 1
    return 3


def _decimal_score(weights: list[float], precision: int) -> Decimal:
    with localcontext() as context:
        context.prec = precision
        total = Decimal(0)
        for weight in weights:
            total += (Decimal(1) + Decimal(str(weight))).ln()
        return +total


def _effective_digits(value: Decimal, bound: Decimal, maximum: int) -> int:
    if value == 0 and bound == 0:
        return maximum
    magnitude = max(abs(value), Decimal("1e-30"))
    relative = bound / magnitude
    if relative <= 0:
        return maximum
    if relative >= 1:
        return 0
    with localcontext() as context:
        context.prec = 40
        digits = int((relative.ln() / Decimal(10).ln()).copy_negate().to_integral_value(rounding="ROUND_FLOOR"))
    return max(0, min(maximum, digits))


def _source_contribution(contribution: dict[str, Any], source: str) -> float:
    return float(contribution.get("source_log_contributions", {}).get(source, 0.0))


def _pair_diagnostic(
    stage1_record: dict[str, Any],
    stage2_record: dict[str, Any],
    stage3_record: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    thresholds = [float(value) for value in config["grading"]["thresholds"]]
    precision = int(config["diagnostics"]["reference_precision"])
    tolerance = Decimal(str(config["diagnostics"]["crosscheck_tolerance"]))
    max_digits = int(config["diagnostics"]["max_significant_digits"])
    score = float(stage3_record["score"])
    grade = grade_for_score(score, thresholds)
    human = stage1_record.get("human_relevance")

    raw_weights: list[float] = []
    quantized_weights: list[float] = []
    per_term: list[dict[str, Any]] = []
    analytical_bound = Decimal(0)
    term_record_bound = Decimal(0)
    term_bound_conflicts: list[dict[str, Any]] = []
    for candidate in stage2_record.get("candidates", []):
        quantization = candidate["quantization"]
        scale = Decimal(str(quantization["scale_factor"]))
        step = Decimal(1) / scale
        candidate_bound = step / Decimal(2)
        analytical_bound += candidate_bound
        stored_bound = Decimal(str(quantization.get("absolute_error_bound", "NaN")))
        term_record_bound += stored_bound
        observed_weight_error = abs(
            Decimal(str(quantization["weight_quantized"])) - Decimal(str(candidate["weight_raw"]))
        )
        if observed_weight_error > candidate_bound + tolerance or stored_bound != candidate_bound:
            term_bound_conflicts.append(
                {
                    "term_id": candidate["term_id"],
                    "target_term": candidate["target_term"],
                    "observed_weight_error": str(observed_weight_error),
                    "analytical_weight_bound": str(candidate_bound),
                    "stored_weight_bound": str(stored_bound),
                }
            )
        raw_weights.append(float(candidate["weight_raw"]))
        quantized_weights.append(float(quantization["weight_quantized"]))
        per_term.append(
            {
                "term_id": candidate["term_id"],
                "query_term": candidate["query_term"],
                "target_term": candidate["target_term"],
                "weight_raw": candidate["weight_raw"],
                "weight_quantized": quantization["weight_quantized"],
                "scale_factor": quantization["scale_factor"],
                "step": str(step),
                "rounding": quantization["rounding"],
                "weight_error": str(observed_weight_error),
                "weight_error_bound": str(candidate_bound),
            }
        )

    reference_score = _decimal_score(raw_weights, precision)
    quantized_reference_score = _decimal_score(quantized_weights, precision)
    observed_quantization_error = abs(quantized_reference_score - reference_score)
    float_path_deviation = abs(Decimal(str(stage3_record["score_unclipped"])) - quantized_reference_score)
    crosscheck_ok = observed_quantization_error <= analytical_bound + tolerance
    bound_path_ok = abs(term_record_bound - analytical_bound) <= tolerance
    effective_digits = _effective_digits(reference_score, analytical_bound, max_digits)
    uncertainty_low = reference_score - analytical_bound
    uncertainty_high = reference_score + analytical_bound

    boundary_margin = Decimal(str(config["grading"]["boundary_warning_margin"]))
    near_threshold = False
    near_threshold_values: list[dict[str, Any]] = []
    for threshold in thresholds:
        distance = abs(score - threshold)
        if Decimal(str(distance)) <= analytical_bound + boundary_margin:
            near_threshold = True
            near_threshold_values.append(
                {
                    "threshold": threshold,
                    "distance": distance,
                    "distance_bound": str(analytical_bound + boundary_margin),
                }
            )

    human_offset = (grade - int(human)) if human is not None else None
    offset_exceeded = (
        human_offset is not None
        and abs(human_offset) > int(config["grading"]["max_human_offset"])
    )
    review_reasons: list[str] = []
    if not crosscheck_ok:
        review_reasons.append("高精度参考误差超过量化步长解析上界")
    if not bound_path_ok or term_bound_conflicts:
        review_reasons.append("词项记录误差界与缩放因子解析上界不一致")
    if float_path_deviation > analytical_bound + tolerance:
        review_reasons.append("浮点对数路径偏离高精度量化参考路径")
    if near_threshold:
        review_reasons.append("分数落在档位边界误差带内")
    if offset_exceeded:
        review_reasons.append("人工相关性等级与模型档位偏移超过阈值")
    empty_diagnostics = stage1_record.get("diagnostics", [])
    if any(item.get("code") == "E_EMPTY_TEXT" for item in empty_diagnostics):
        review_reasons.append("空查询或空文档样本，需人工确认")

    contributions = stage3_record.get("contributions", [])
    crosscheck_failure_sources: list[str] = []
    if not crosscheck_ok:
        crosscheck_failure_sources.append("observed_quantization_error_exceeds_scale_step_bound")
    if not bound_path_ok:
        crosscheck_failure_sources.append("term_record_bound_differs_from_scale_step_bound")
    if term_bound_conflicts:
        crosscheck_failure_sources.append("per_term_quantization_bound_conflict")
    if float_path_deviation > analytical_bound + tolerance:
        crosscheck_failure_sources.append("float_log_path_exceeds_quantization_bound")
    source_log_contributions = {
        "lexical": math.fsum(_source_contribution(item, "lexical") for item in contributions),
        "bridge": math.fsum(_source_contribution(item, "bridge") for item in contributions),
    }
    return {
        "pair_id": stage1_record["pair_id"],
        "shard": stage1_record["shard"],
        "query": stage1_record["query"],
        "doc": stage1_record["doc"],
        "lang_query": stage1_record["lang_query"],
        "lang_doc": stage1_record["lang_doc"],
        "score": score,
        "score_unclipped": stage3_record["score_unclipped"],
        "score_clip": stage3_record["score_clip"],
        "clipped": stage3_record["clipped"],
        "grade": grade,
        "grade_label": config["grading"]["labels"][grade],
        "human_relevance": human,
        "human_offset": human_offset,
        "human_offset_exceeded": offset_exceeded,
        "candidate_count": stage3_record["candidate_count"],
        "source_log_contributions": source_log_contributions,
        "error_report": {
            "reference_method": "decimal_ln1p_sum_v1",
            "reference_precision": precision,
            "reference_score": str(reference_score),
            "quantized_reference_score": str(quantized_reference_score),
            "stage3_float_score": stage3_record["score_unclipped"],
            "observed_quantization_error": str(observed_quantization_error),
            "analytical_error_bound": str(analytical_bound),
            "term_record_error_bound": str(term_record_bound),
            "uncertainty_interval": [str(uncertainty_low), str(uncertainty_high)],
            "effective_digits": effective_digits,
            "crosscheck_ok": crosscheck_ok,
            "bound_path_ok": bound_path_ok,
            "crosscheck_failure_sources": crosscheck_failure_sources,
            "float_path_deviation": str(float_path_deviation),
            "term_bound_conflicts": term_bound_conflicts,
            "per_term": per_term,
        },
        "boundary_checks": near_threshold_values,
        "diagnostics": empty_diagnostics,
        "review_required": bool(review_reasons),
        "review_reasons": review_reasons,
    }


def diagnose_shard(
    stage1_records: list[dict[str, Any]],
    stage2_records: list[dict[str, Any]],
    stage3_records: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    stage2_by_id = {item["pair_id"]: item for item in stage2_records}
    stage3_by_id = {item["pair_id"]: item for item in stage3_records}
    results = []
    for stage1_record in stage1_records:
        pair_id = stage1_record["pair_id"]
        results.append(
            _pair_diagnostic(
                stage1_record,
                stage2_by_id[pair_id],
                stage3_by_id[pair_id],
                config,
            )
        )
    return results


def _bound_regression(pair_diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    by_shard: dict[str, list[dict[str, Any]]] = {}
    for item in pair_diagnostics:
        by_shard.setdefault(item["shard"], []).append(item)
    rows: list[dict[str, Any]] = []
    cumulative_count = 0
    cumulative_bounds: list[float] = []
    for shard in sorted(by_shard):
        items = by_shard[shard]
        bounds = [float(item["error_report"]["analytical_error_bound"]) for item in items]
        cumulative_count += len(items)
        cumulative_bounds.extend(bounds)
        rows.append(
            {
                "shard": shard,
                "pair_count": len(items),
                "max_error_bound": max(bounds, default=0.0),
                "mean_error_bound": float(np.mean(bounds)) if bounds else 0.0,
                "cumulative_pair_count": cumulative_count,
                "cumulative_max_error_bound": max(cumulative_bounds, default=0.0),
                "cumulative_mean_error_bound": (
                    float(np.mean(cumulative_bounds)) if cumulative_bounds else 0.0
                ),
            }
        )
    sizes = [row["cumulative_pair_count"] for row in rows]
    maxima = [row["cumulative_max_error_bound"] for row in rows]
    means = [row["cumulative_mean_error_bound"] for row in rows]
    if len(sizes) >= 2 and len(set(sizes)) > 1:
        slope, intercept = np.polyfit(np.asarray(sizes, dtype=float), np.asarray(maxima, dtype=float), 1)
        slope_value = float(slope)
        intercept_value = float(intercept)
        mean_slope, mean_intercept = np.polyfit(
            np.asarray(sizes, dtype=float),
            np.asarray(means, dtype=float),
            1,
        )
        mean_slope_value = float(mean_slope)
        mean_intercept_value = float(mean_intercept)
    else:
        slope_value = 0.0
        intercept_value = maxima[-1] if maxima else 0.0
        mean_slope_value = 0.0
        mean_intercept_value = means[-1] if means else 0.0
    total = sizes[-1] if sizes else 0
    prediction_sizes = sorted({total, total * 2})
    predictions = [
        {
            "pair_count": size,
            "estimated_max_error_bound": intercept_value + slope_value * size,
            "estimated_mean_error_bound": mean_intercept_value + mean_slope_value * size,
        }
        for size in prediction_sizes
    ]
    return {
        "method": "least_squares_cumulative_shard_max_bound_v1",
        "by_shard": rows,
        "slope_per_pair": slope_value,
        "intercept": intercept_value,
        "mean_slope_per_pair": mean_slope_value,
        "mean_intercept": mean_intercept_value,
        "predictions": predictions,
    }


def aggregate_diagnostics(
    pair_diagnostics: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    labels = list(config["grading"]["labels"])
    thresholds = [float(value) for value in config["grading"]["thresholds"]]
    reference_distribution = [float(value) for value in config["grading"]["reference_distribution"]]
    max_distribution_deviation = float(config["grading"]["max_distribution_deviation"])

    grade_counts = [0, 0, 0, 0]
    for item in pair_diagnostics:
        grade_counts[int(item["grade"])] += 1
    total = len(pair_diagnostics)
    observed_distribution = [(count / total) if total else 0.0 for count in grade_counts]
    distribution_offsets = [
        {
            "grade": index,
            "label": labels[index],
            "count": grade_counts[index],
            "observed_proportion": observed_distribution[index],
            "reference_proportion": reference_distribution[index],
            "proportion_offset": observed_distribution[index] - reference_distribution[index],
            "count_offset": grade_counts[index] - reference_distribution[index] * total,
        }
        for index in range(4)
    ]
    distribution_review = any(
        abs(item["proportion_offset"]) > max_distribution_deviation for item in distribution_offsets
    )

    confusion = [[0, 0, 0, 0] for _ in range(4)]
    labeled = [item for item in pair_diagnostics if item["human_relevance"] is not None]
    for item in labeled:
        confusion[int(item["grade"])][int(item["human_relevance"])] += 1

    boundaries: list[dict[str, Any]] = []
    for human_level, threshold in enumerate(thresholds, 1):
        lower = [
            float(item["score"])
            for item in labeled
            if int(item["human_relevance"]) < human_level
        ]
        upper = [
            float(item["score"])
            for item in labeled
            if int(item["human_relevance"]) >= human_level
        ]
        if lower and upper:
            empirical = (max(lower) + min(upper)) / 2.0
            deviation = empirical - threshold
        else:
            empirical = None
            deviation = None
        boundaries.append(
            {
                "between_grades": [human_level - 1, human_level],
                "configured_threshold": threshold,
                "empirical_boundary": empirical,
                "deviation": deviation,
                "review_required": deviation is not None
                and abs(deviation) > float(config["grading"]["max_boundary_deviation"]),
            }
        )

    out_of_bounds = [
        {
            "pair_id": item["pair_id"],
            "shard": item["shard"],
            "reasons": item["review_reasons"],
            "score": item["score"],
            "grade": item["grade"],
            "human_relevance": item["human_relevance"],
        }
        for item in pair_diagnostics
        if item["review_required"]
    ]
    error_review = [
        item["pair_id"]
        for item in pair_diagnostics
        if not item["error_report"]["crosscheck_ok"]
        or not item["error_report"]["bound_path_ok"]
        or bool(item["error_report"]["term_bound_conflicts"])
    ]
    boundary_review = [item for item in boundaries if item["review_required"]]
    suggestions: list[str] = []
    if distribution_review:
        suggestions.append("检查档位阈值或参考分布；某些档位占比已超过允许偏移。")
    if boundary_review:
        suggestions.append("人工等级与阈值边界存在偏差，建议抽样复核并重新校准阈值。")
    if error_review:
        suggestions.append("量化误差界交叉核对失败，建议提高量化位宽并检查词项缩放因子。")
    if out_of_bounds:
        suggestions.append("优先复核越界清单中的高误差或边界样本。")

    return {
        "labels": labels,
        "thresholds": thresholds,
        "pair_count": total,
        "grade_counts": grade_counts,
        "observed_distribution": observed_distribution,
        "reference_distribution": reference_distribution,
        "distribution_offsets": distribution_offsets,
        "distribution_review_required": distribution_review,
        "labeled_pair_count": len(labeled),
        "confusion_matrix": {
            "axes": {"rows": "model_grade", "columns": "human_relevance"},
            "labels": labels,
            "counts": confusion,
        },
        "boundaries": boundaries,
        "boundary_review_required": bool(boundary_review),
        "out_of_bounds_samples": out_of_bounds,
        "error_crosscheck_review_pairs": error_review,
        "review_required": bool(out_of_bounds or distribution_review or boundary_review or error_review),
        "suggestions": suggestions,
        "error_propagation": _bound_regression(pair_diagnostics),
    }
