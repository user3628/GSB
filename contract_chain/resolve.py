"""Stage 4: conflict grouping, fixed-point log confidence and deterministic adjudication."""
from __future__ import annotations

import math
from fractions import Fraction

from .codec import stable_sorted
from .config import ResolveConfig
from .normalize import comparison_key

_LOG_PRECISION = 9
_LOG_SCALE = 10 ** _LOG_PRECISION


def _log_scaled(value: Fraction) -> int:
    """Fixed-point natural log; deterministic integer output, no float in stored scores."""
    return round(math.log(float(value)) * _LOG_SCALE)


def _month_index(crawl_month: str) -> int:
    year, month = crawl_month.split("-", 1)
    return int(year) * 12 + int(month)


def _confidence(entry: dict) -> Fraction:
    match = entry["region_binding"].get("match")
    if not match:
        return Fraction(0)
    return Fraction(match["confidence_scaled"], match["confidence_scale"])


def _freshness(crawl_month: str, reference_month: int, half_life: int) -> Fraction:
    age = max(0, reference_month - _month_index(crawl_month))
    # 2 ** (-age / half_life), expressed exactly as a rational before log quantization.
    return Fraction(2) ** Fraction(-age, max(1, half_life))


def _equal_terms(left, right, config: ResolveConfig, term_name: str) -> bool:
    if left["kind"] != right["kind"]:
        return False
    target_precision = config.compare_precision.get(
        "month" if term_name == "effective_month" else term_name, 0
    )
    if left["kind"] == "duration_interval":
        for endpoint_name in ("lower", "upper"):
            a = comparison_key(
                left[endpoint_name]["scaled"],
                left[endpoint_name]["precision"],
                target_precision,
            )
            b = comparison_key(
                right[endpoint_name]["scaled"],
                right[endpoint_name]["precision"],
                target_precision,
            )
            left_open = left[endpoint_name + "_open"]
            right_open = right[endpoint_name + "_open"]
            if a != b or left_open != right_open:
                return False
        return True
    a = comparison_key(
        left["point"]["scaled"], left["point"]["precision"], target_precision
    )
    b = comparison_key(
        right["point"]["scaled"], right["point"]["precision"], target_precision
    )
    return a == b


def score_ref(entry, reference_month, config: ResolveConfig) -> dict:
    prior = config.prior(entry["source"])
    lang = config.lang_weight(entry["lang"])
    confidence = _confidence(entry)
    confidence_for_log = confidence if confidence > 0 else Fraction(1, 10 ** 12)
    freshness = _freshness(entry["crawl_month"], reference_month, config.freshness_half_life_months)
    components = {
        "source_prior": str(prior),
        "language_weight": str(lang),
        "region_confidence": str(confidence),
        "freshness": str(freshness),
    }
    score_scaled = (
        _log_scaled(prior)
        + _log_scaled(lang)
        + _log_scaled(confidence_for_log)
        + _log_scaled(freshness)
    )
    return {"score_scaled": score_scaled, "score_scale": _LOG_SCALE, "components": components}


def _arbitration_key(entry, scoring, config: ResolveConfig):
    components = scoring["components"]
    lookup = {
        "source_prior_desc": -_log_scaled(Fraction(components["source_prior"])),
        "confidence_desc": -_log_scaled(Fraction(components["region_confidence"])
                                        if Fraction(components["region_confidence"]) > 0
                                        else Fraction(1, 10 ** 12)),
        "freshness_desc": -_log_scaled(Fraction(components["freshness"])),
        "language_weight_desc": -_log_scaled(Fraction(components["language_weight"])),
        "ref_id_asc": entry["ref_id"],
    }
    return tuple(lookup[rule] for rule in config.arbitration_order)


def run_resolve(parse_product: dict, normalize_product: dict, config: ResolveConfig) -> dict:
    """Stage-4 product: conflict groups, unique winners, demotions and reasons."""
    parse_entries = {entry["ref_id"]: entry for entry in parse_product["entries"]}
    norm_entries = {item["ref_id"]: item for item in normalize_product["results"]}
    eligible = [
        entry["ref_id"]
        for entry in parse_product["entries"]
        if entry["preferred_location"] is not None and "parse_error" not in entry
    ]
    reference_month = max(_month_index(parse_entries[ref]["crawl_month"]) for ref in eligible)

    groups = {}
    for ref_id in eligible:
        entry = parse_entries[ref_id]
        location = entry["preferred_location"]
        groups.setdefault(
            (entry["contract_id"], location["article"], location["clause"]), []
        ).append(ref_id)

    decisions = []
    demotions = []
    conflict_group_count = 0

    for (contract_id, article, clause), ref_ids in sorted(groups.items()):
        term_matrix = {ref_id: norm_entries[ref_id]["normalized_terms"] for ref_id in ref_ids}
        shared_terms = sorted(
            set.intersection(*(set(terms) for terms in term_matrix.values()))
        )
        conflicting_terms = []
        for term_name in shared_terms:
            distinct = []
            for ref_id in ref_ids:
                if not any(
                    _equal_terms(term_matrix[other][term_name], term_matrix[ref_id][term_name],
                                 config, term_name)
                    for other in distinct
                ):
                    distinct.append(ref_id)
            if len(distinct) > 1:
                conflicting_terms.append(term_name)

        scored = []
        for ref_id in ref_ids:
            scoring = score_ref(parse_entries[ref_id], reference_month, config)
            scored.append((ref_id, scoring))
        scored = stable_sorted(
            scored,
            key=lambda pair: (
                -pair[1]["score_scaled"],
                _arbitration_key(parse_entries[pair[0]], pair[1], config),
                pair[0],
            ),
        )
        winner_id = scored[0][0]
        is_conflict = bool(conflicting_terms)
        if is_conflict:
            conflict_group_count += 1
        decisions.append(
            {
                "group_id": "grp-%s-a%d-c%d" % (contract_id, article, clause),
                "contract_id": contract_id,
                "article": article,
                "clause": clause,
                "ref_ids": sorted(ref_ids),
                "conflict": is_conflict,
                "conflicting_terms": conflicting_terms,
                "winner_ref_id": winner_id,
                "scores": {
                    ref_id: {
                        "score_scaled": scoring["score_scaled"],
                        "score_scale": scoring["score_scale"],
                        "components": scoring["components"],
                        "arbitration_order": list(config.arbitration_order),
                    }
                    for ref_id, scoring in scored
                },
            }
        )
        if is_conflict:
            winner_score = dict(scored)[winner_id]["score_scaled"]
            for ref_id, scoring in scored[1:]:
                demotions.append(
                    {
                        "ref_id": ref_id,
                        "group_id": "grp-%s-a%d-c%d" % (contract_id, article, clause),
                        "reason": "conflict_loser",
                        "conflicting_terms": conflicting_terms,
                        "winner_ref_id": winner_id,
                        "winner_score_scaled": winner_score,
                        "loser_score_scaled": scoring["score_scaled"],
                        "arbitration_order": list(config.arbitration_order),
                    }
                )

    return {
        "stage": "resolve",
        "reference_month": reference_month,
        "groups": decisions,
        "demotions": stable_sorted(demotions, key=lambda item: item["ref_id"]),
        "stats": {
            "group_count": len(decisions),
            "conflict_group_count": conflict_group_count,
            "demoted_count": len(demotions),
        },
    }
