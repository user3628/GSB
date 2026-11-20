"""Stage 2: preference-pair quality scoring in the log domain."""
from __future__ import annotations

import math

from .errors import NumericError
from .hashing import canonical_json, stable_hash64, tokenize

STAGE = "quality"


def _round_sig(value: float, digits: int) -> float:
    if value == 0.0:
        return 0.0
    magnitude = math.floor(math.log10(abs(value)))
    factor = 10 ** (digits - 1 - magnitude)
    return round(value * factor) / factor


def length_penalty(norm_chosen: str, norm_rejected: str) -> float:
    """1 / (1 + abs(len_chosen - len_rejected)); in (0, 1]."""
    return 1.0 / (1.0 + abs(len(norm_chosen) - len(norm_rejected)))


def lexical_overlap(norm_chosen: str, norm_rejected: str) -> float:
    """Smoothed relative token containment of rejected inside chosen."""
    chosen_tokens = tokenize(norm_chosen)
    rejected_tokens = tokenize(norm_rejected)
    if not rejected_tokens:
        return 1.0
    chosen_bag: dict[str, int] = {}
    for token in chosen_tokens:
        chosen_bag[token] = chosen_bag.get(token, 0) + 1
    contained = 0
    for token in rejected_tokens:
        if chosen_bag.get(token, 0) > 0:
            chosen_bag[token] -= 1
            contained += 1
    return (contained + 1.0) / (len(rejected_tokens) + 1.0)


def source_prior(source: str, priors: dict[str, float]) -> float:
    if source in priors:
        return float(priors[source])
    return float(priors.get("unknown", 0.5))


def score_pair(record: dict, weights: dict[str, float], priors: dict[str, float],
               digits: int) -> dict:
    factors = {
        "length_penalty": length_penalty(record["norm_chosen"], record["norm_rejected"]),
        "lexical_overlap": lexical_overlap(record["norm_chosen"], record["norm_rejected"]),
        "source_prior": source_prior(record["source"], priors),
    }
    diagnostics: list[dict] = []
    log_contributions: dict[str, float] = {}
    for name, value in factors.items():
        if not math.isfinite(value):
            raise NumericError(
                f"factor {name} is NaN/Inf",
                details={"pair_id": record["pair_id"], "factor": name, "value": str(value)},
            )
        if value <= 0.0:
            raise NumericError(
                f"factor {name} is zero or negative",
                details={"pair_id": record["pair_id"], "factor": name, "value": value},
            )
        log_contributions[name] = _round_sig(
            float(weights.get(name, 0.0)) * math.log(value), digits)

    final_score = _round_sig(sum(log_contributions.values()), digits)
    if not math.isfinite(final_score):
        raise NumericError(
            "final score is NaN/Inf",
            details={"pair_id": record["pair_id"], "factor": "final_score",
                     "value": str(final_score)},
        )
    return {
        "pair_id": record["pair_id"],
        "group_id": record["group_id"],
        "factors": {name: _round_sig(float(value), digits) for name, value in factors.items()},
        "log_contributions": log_contributions,
        "quality_score": final_score,
        "diagnostics": diagnostics,
    }


def run(normalized: dict, cfg_raw: dict) -> dict:
    quality_cfg = cfg_raw["quality"]
    weights = quality_cfg["weights"]
    priors = quality_cfg["source_priors"]
    digits = int(quality_cfg.get("significant_digits", 8))

    results: list[dict] = []
    diagnostics: list[dict] = list(normalized.get("diagnostics", []))
    for record in normalized["records"]:
        try:
            results.append(score_pair(record, weights, priors, digits))
        except NumericError as exc:
            diagnostics.append({
                "code": exc.code,
                "pair_id": exc.details.get("pair_id"),
                "factor": exc.details.get("factor"),
                "message": str(exc),
            })
    results.sort(key=lambda item: item["pair_id"])
    diagnostics.sort(key=lambda item: (item.get("pair_id") or "", item.get("factor") or "",
                                       item["code"]))
    return {
        "stage": STAGE,
        "records": results,
        "diagnostics": diagnostics,
        "stats": {
            "scored_pairs": len(results),
            "numeric_errors": sum(1 for item in diagnostics if item["code"] == "E_NUMERIC"),
        },
    }


def artifact_digest(result: dict) -> str:
    payload = {
        "records": [
            {key: record[key] for key in ("pair_id", "group_id", "factors",
                                          "log_contributions", "quality_score")}
            for record in result["records"]
        ],
        "diagnostics": result["diagnostics"],
    }
    return stable_hash64(canonical_json(payload))
