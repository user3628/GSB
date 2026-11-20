from __future__ import annotations

import math
from typing import Any

from .errors import BusinessError, E_NUMERIC, numeric_error
from .util import is_finite_number


def _validate_candidate(pair_id: str, candidate: dict[str, Any]) -> tuple[float, float, int]:
    term = candidate.get("target_term")
    if candidate.get("numeric_issue"):
        raise numeric_error(
            pair_id,
            term,
            "stage3_log_domain_score",
            str(candidate["numeric_issue"]),
            candidate.get("weight_raw_text"),
        )
    quantization = candidate.get("quantization") or {}
    scale = quantization.get("scale_factor")
    q = quantization.get("q")
    weight = quantization.get("weight_quantized")
    raw_weight = candidate.get("weight_raw")
    if not is_finite_number(scale) or scale <= 0:
        raise numeric_error(pair_id, term, "stage3_log_domain_score", "zero or invalid quantization scale", scale)
    if not isinstance(q, int):
        raise numeric_error(pair_id, term, "stage3_log_domain_score", "missing quantized coefficient", q)
    if not is_finite_number(weight) or weight < 0:
        raise numeric_error(pair_id, term, "stage3_log_domain_score", "negative or non-finite quantized weight", weight)
    if not is_finite_number(raw_weight) or raw_weight < 0:
        raise numeric_error(pair_id, term, "stage3_log_domain_score", "negative or non-finite raw weight", raw_weight)
    if weight != q / scale:
        raise numeric_error(pair_id, term, "stage3_log_domain_score", "quantized coefficient/scale mismatch", weight)
    lexical_component = candidate.get("lexical_weight", 0.0)
    bridge_component = candidate.get("bridge_weight", 0.0)
    if not is_finite_number(lexical_component) or not is_finite_number(bridge_component):
        raise numeric_error(pair_id, term, "stage3_log_domain_score", "non-finite source component")
    if lexical_component < 0 or bridge_component < 0:
        raise numeric_error(pair_id, term, "stage3_log_domain_score", "negative source component")
    return float(weight), float(lexical_component), float(bridge_component)


def score_pair(
    stage2_record: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    pair_id = str(stage2_record["pair_id"])
    clip = float(config["scoring"]["score_clip"])
    if not is_finite_number(clip) or clip <= 0:
        raise BusinessError(E_NUMERIC, "invalid score clip", {"pair_id": pair_id, "stage": "stage3_log_domain_score"})

    contributions: list[dict[str, Any]] = []
    source_totals = {"lexical": 0.0, "bridge": 0.0}
    score_terms: list[float] = []
    error_bound = 0.0
    for candidate in stage2_record.get("candidates", []):
        weight, lexical_component, bridge_component = _validate_candidate(pair_id, candidate)
        log_contribution = math.log1p(weight)
        component_total = lexical_component + bridge_component
        if component_total < 0 or not math.isfinite(component_total):
            raise numeric_error(
                pair_id,
                candidate.get("target_term"),
                "stage3_log_domain_score",
                "invalid source component total",
                component_total,
            )
        if component_total == 0 and log_contribution != 0:
            raise numeric_error(
                pair_id,
                candidate.get("target_term"),
                "stage3_log_domain_score",
                "log contribution has no positive source",
                log_contribution,
            )
        if component_total > 0:
            lexical_share = lexical_component / component_total
            bridge_share = bridge_component / component_total
        else:
            lexical_share = 0.0
            bridge_share = 0.0
        lexical_log = log_contribution * lexical_share
        bridge_log = log_contribution * bridge_share
        scale = float(candidate["quantization"]["scale_factor"])
        candidate_bound = 0.5 / scale
        error_bound += candidate_bound
        score_terms.append(log_contribution)
        source_totals["lexical"] += lexical_log
        source_totals["bridge"] += bridge_log
        contributions.append(
            {
                "term_id": candidate["term_id"],
                "query_term": candidate["query_term"],
                "target_term": candidate["target_term"],
                "routes": candidate["routes"],
                "entry_ids": candidate["entry_ids"],
                "weight_raw": candidate["weight_raw"],
                "weight_quantized": weight,
                "log_contribution": log_contribution,
                "source_log_contributions": {
                    "lexical": lexical_log,
                    "bridge": bridge_log,
                },
                "document_factor": candidate["document_factor"],
                "quantization_scale_factor": scale,
                "quantization_error_bound": candidate_bound,
            }
        )

    score_unclipped = math.fsum(score_terms)
    source_totals["lexical"] = math.fsum(
        item["source_log_contributions"]["lexical"] for item in contributions
    )
    source_totals["bridge"] = math.fsum(
        item["source_log_contributions"]["bridge"] for item in contributions
    )
    score = min(score_unclipped, clip)
    if not math.isfinite(score) or not math.isfinite(score_unclipped):
        raise numeric_error(pair_id, None, "stage3_log_domain_score", "non-finite aggregate score", score)
    return {
        "pair_id": pair_id,
        "shard": stage2_record["shard"],
        "score": score,
        "score_unclipped": score_unclipped,
        "score_clip": clip,
        "clipped": score_unclipped > clip,
        "source_log_contributions": {
            "lexical": source_totals["lexical"],
            "bridge": source_totals["bridge"],
        },
        "quantization_error_bound": error_bound,
        "contributions": contributions,
        "candidate_count": len(contributions),
        "diagnostics": [],
    }


def score_shard(stage2_records: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    return [score_pair(record, config) for record in stage2_records]
