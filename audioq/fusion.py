"""Weighted score fusion, log contributions and reference diagnostics."""

from __future__ import annotations

import math
from decimal import Decimal, localcontext
from typing import Any, Mapping

from .audio import validate_metric_scores
from .canonical import finite_float, rounded_float
from .errors import AudioQError
from .models import EvaluationConfig, FusionConfig, METRIC_NAMES, ReferenceConfig


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0E+0"
    return format(value.normalize(), "E")


def _effective_digits(value: float, decimal_places: int) -> int:
    text = format(value, f".{decimal_places}f").replace(".", "").lstrip("0").rstrip("0")
    return max(1, len(text))


def _high_precision_fusion(
    available: list[str],
    scores: Mapping[str, float],
    normalised_weights: Mapping[str, float],
    fusion: FusionConfig,
) -> Decimal:
    with localcontext() as context:
        context.prec = fusion.high_precision
        epsilon = Decimal(str(fusion.epsilon))
        if fusion.domain == "log":
            terms = []
            for name in available:
                weight = Decimal(str(normalised_weights[name]))
                score = max(Decimal(str(scores[name])), epsilon)
                terms.append(weight * score.ln())
            return sum(terms, Decimal(0)).exp()
        return sum(
            (Decimal(str(normalised_weights[name])) * Decimal(str(scores[name])) for name in available),
            Decimal(0),
        )


def fuse_metrics(
    metrics_artifact: Mapping[str, Any],
    config: EvaluationConfig,
    reference: ReferenceConfig | Mapping[str, Any],
    *,
    shard_id: str | None = None,
) -> dict[str, Any]:
    scores_raw = metrics_artifact.get("scores", {})
    if not isinstance(scores_raw, Mapping):
        raise AudioQError("E_NUMERIC", "metrics scores must be an object", shard_id=shard_id, stage="fusion")
    try:
        scores = validate_metric_scores(scores_raw)
    except AudioQError as exc:
        if exc.shard_id is None:
            exc.shard_id = shard_id
        if exc.stage is None:
            exc.stage = "fusion"
        raise

    weights: dict[str, float] = {}
    for name in METRIC_NAMES:
        raw_weight = config.weights.get(name, 0.0)
        try:
            weight = finite_float(raw_weight, name=f"{name}_weight")
        except AudioQError as exc:
            exc.shard_id = shard_id
            exc.stage = "fusion"
            raise
        if weight < 0:
            raise AudioQError(
                "E_INVALID_WEIGHT",
                "weights must be non-negative",
                shard_id=shard_id,
                stage="fusion",
                observed={"metric": name, "weight": weight},
            )
        weights[name] = weight
    total_weight = sum(weights.values())
    if not math.isfinite(total_weight) or total_weight <= 0:
        raise AudioQError(
            "E_INVALID_WEIGHT",
            "weight sum is zero or non-finite",
            shard_id=shard_id,
            stage="fusion",
            observed={"weight_sum": total_weight},
        )

    available = [name for name in METRIC_NAMES if scores.get(name) is not None]
    skipped = [
        {
            "metric": name,
            "weight": rounded_float(weights[name] / total_weight, config.fusion.decimals),
            "reason": "metric_unavailable",
        }
        for name in METRIC_NAMES
        if name not in available
    ]
    available_weight_sum = sum(weights[name] for name in available)
    if not available or not math.isfinite(available_weight_sum) or available_weight_sum <= 0:
        raise AudioQError(
            "E_INVALID_WEIGHT",
            "all metrics are unavailable or the available weight sum is zero",
            shard_id=shard_id,
            stage="fusion",
            observed={
                "available_metrics": available,
                "available_weight_sum": available_weight_sum,
                "skipped": skipped,
            },
        )
    normalised_weights = {name: weights[name] / available_weight_sum for name in available}

    log_terms: dict[str, float] = {}
    weighted_terms: dict[str, float] = {}
    for name in available:
        score = float(scores[name])
        normalised_weight = normalised_weights[name]
        log_terms[name] = normalised_weight * math.log(max(score, config.fusion.epsilon))
        weighted_terms[name] = normalised_weight * score

    if config.fusion.domain == "log":
        fused_raw = math.exp(sum(log_terms.values()))
    else:
        fused_raw = sum(weighted_terms.values())
    if not math.isfinite(fused_raw):
        raise AudioQError(
            "E_NUMERIC",
            "fused score is NaN or infinite",
            shard_id=shard_id,
            stage="fusion",
            observed={"domain": config.fusion.domain, "raw": repr(fused_raw)},
        )
    fused = rounded_float(fused_raw, config.fusion.decimals, name="fused_score")

    high_precision = _high_precision_fusion(available, scores, normalised_weights, config.fusion)
    error_bound = abs(Decimal(str(fused)) - high_precision)
    if not high_precision.is_finite() or not error_bound.is_finite():
        raise AudioQError(
            "E_NUMERIC",
            "high precision fusion reference is non-finite",
            shard_id=shard_id,
            stage="fusion",
        )

    if isinstance(reference, ReferenceConfig):
        ref = reference
    else:
        if not isinstance(reference, Mapping):
            raise AudioQError("E_CONFIG", "reference interval must be an object", shard_id=shard_id, stage="fusion")
        try:
            low = float(reference.get("min_mos", reference.get("min", 0.0)))
            high = float(reference.get("max_mos", reference.get("max", 5.0)))
            target_raw = reference.get("target_mos", reference.get("target"))
            ref = ReferenceConfig(
                min_mos=low,
                max_mos=high,
                target_mos=(low + high) / 2.0 if target_raw is None else float(target_raw),
            )
        except (TypeError, ValueError) as exc:
            raise AudioQError(
                "E_CONFIG",
                "reference interval contains a non-numeric value",
                shard_id=shard_id,
                stage="fusion",
                observed={"reason": str(exc)},
            ) from exc

    below = fused < ref.min_mos
    above = fused > ref.max_mos
    out_of_range = bool(below or above)
    if below:
        deviation_signed = fused - ref.min_mos
        grade = "poor"
    elif above:
        deviation_signed = fused - ref.max_mos
        grade = "excellent"
    else:
        deviation_signed = 0.0
        grade = "excellent" if fused >= ref.target_mos else "good"

    contributions = {
        name: {
            "score": rounded_float(scores[name], config.fusion.decimals),
            "configured_weight": rounded_float(weights[name] / total_weight, config.fusion.decimals),
            "normalised_weight": rounded_float(normalised_weights[name], config.fusion.decimals),
            "log_contribution": rounded_float(log_terms[name], config.fusion.decimals),
            "weighted_contribution": rounded_float(weighted_terms[name], config.fusion.decimals),
        }
        for name in available
    }
    return {
        "status": "ok",
        "shard_id": shard_id,
        "domain": config.fusion.domain,
        "fused_score": fused,
        "grade": grade,
        "reference": {
            **ref.to_dict(),
            "out_of_range": out_of_range,
            "deviation": rounded_float(abs(deviation_signed), config.fusion.decimals),
            "deviation_signed": rounded_float(deviation_signed, config.fusion.decimals),
            "position": "below" if below else "above" if above else "inside",
        },
        "contributions": contributions,
        "log_contributions": {
            name: rounded_float(log_terms[name], config.fusion.decimals) for name in available
        },
        "weighting": {
            "configured_sum": rounded_float(total_weight, config.fusion.decimals),
            "available_sum_before_renormalisation": rounded_float(available_weight_sum, config.fusion.decimals),
            "renormalised": len(skipped) > 0 or abs(available_weight_sum - total_weight) > 1e-15,
            "normalised_available_weights": {
                name: rounded_float(normalised_weights[name], config.fusion.decimals) for name in available
            },
        },
        "skipped_metrics": skipped,
        "precision": {
            "decimal_places": config.fusion.decimals,
            "effective_digits": _effective_digits(fused, config.fusion.decimals),
            "high_precision_digits": config.fusion.high_precision,
            "high_precision_reference": format(high_precision, f".{config.fusion.high_precision - 2}f"),
            "error_bound": _decimal_text(error_bound),
            "comparison": "abs(SerializedFusedScore - local_decimal_reference)",
        },
        "errors": [],
    }
