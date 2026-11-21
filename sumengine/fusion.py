"""Log-domain candidate fusion and deterministic final adjudication."""

from __future__ import annotations

import math
from typing import Any


def _round(value: float, digits: int = 9) -> float:
    return round(float(value), digits)


def _length_fit(length: int, band: dict[str, int]) -> float:
    low = int(band["min_chars"])
    high = int(band["max_chars"])
    span = max(1, high - low)
    midpoint = (low + high) / 2
    if low <= length <= high:
        return max(0.85, 1.0 - 0.15 * abs(length - midpoint) / (span / 2))
    distance = low - length if length < low else length - high
    return max(0.01, 1.0 - distance / (span + 1.0))


def fuse_candidates(doc: dict[str, Any], candidate_bundle: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    fusion_cfg = config["fusion"]
    weights = {key: float(value) for key, value in fusion_cfg["weights"].items()}
    epsilon = float(fusion_cfg["epsilon"])
    priority = [str(route) for route in fusion_cfg["route_priority"]]
    priority_index = {route: index for index, route in enumerate(priority)}
    source_prior = float(fusion_cfg["source_priors"].get(doc.get("source"), 0.65))
    source_prior = min(1.0, max(epsilon, source_prior))
    band = candidate_bundle["candidates"][0]["target_band"] if candidate_bundle["candidates"] else None
    if band is None:
        low = int(config["targets"][doc["target_length"]]["min_chars"])
        high = int(config["targets"][doc["target_length"]]["max_chars"])
        band = {"min_chars": low, "max_chars": high}

    scored: list[dict[str, Any]] = []
    for candidate in candidate_bundle["candidates"]:
        coverage = min(1.0, max(epsilon, float(candidate["coverage"])))
        length_fit = min(1.0, max(epsilon, _length_fit(int(candidate["length_chars"]), band)))
        language = min(1.0, max(epsilon, float(candidate["language"]["consistency_score"])))
        features = {
            "coverage": coverage,
            "length_fit": length_fit,
            "language": language,
            "source_prior": source_prior,
        }
        log_terms = {
            feature: _round(weights[feature] * math.log(value), 12)
            for feature, value in features.items()
        }
        fusion_score = _round(sum(log_terms.values()), 9)
        unavailable_reasons = list(dict.fromkeys(candidate.get("structural_unavailable_reasons", [])))
        if unavailable_reasons:
            available = False
        else:
            available = True
        detail = {
            "candidate_id": candidate["candidate_id"],
            "route": candidate["route"],
            "text": candidate["text"],
            "length_chars": candidate["length_chars"],
            "target_band": dict(band),
            "available": available,
            "unavailable_reasons": unavailable_reasons,
            "eliminated_reason": None,
            "features": {key: _round(value) for key, value in features.items()},
            "log_terms": log_terms,
            "weights": dict(weights),
            "fusion_score": fusion_score,
            "coverage_minimum": candidate.get("coverage_minimum"),
            "coverage_ok": candidate.get("coverage_ok"),
            "language": candidate["language"],
        }
        scored.append(detail)

    def order_key(item: dict[str, Any]) -> tuple[Any, ...]:
        return (
            -float(item["fusion_score"]),
            priority_index.get(item["route"], len(priority)),
            item["candidate_id"],
        )

    available = sorted([item for item in scored if item["available"]], key=order_key)
    unavailable = sorted([item for item in scored if not item["available"]], key=order_key)
    winner: dict[str, Any] | None = available[0] if available else None
    if winner is not None:
        for item in scored:
            if item is winner:
                continue
            if not item["available"]:
                item["eliminated_reason"] = ";".join(item["unavailable_reasons"])
            elif item["fusion_score"] == winner["fusion_score"]:
                item["eliminated_reason"] = "TIE_BROKEN_BY_ROUTE_PRIORITY_OR_ID"
            else:
                item["eliminated_reason"] = "LOWER_FUSION_SCORE"
    else:
        for item in scored:
            item["eliminated_reason"] = ";".join(item["unavailable_reasons"]) or "NO_AVAILABLE_CANDIDATE"

    return {
        "doc_id": doc["doc_id"],
        "weights": dict(weights),
        "epsilon": epsilon,
        "source_channel": doc.get("source"),
        "source_prior": _round(source_prior),
        "candidates": scored,
        "ranking": [item["candidate_id"] for item in available] + [item["candidate_id"] for item in unavailable],
        "available_count": len(available),
        "unavailable_count": len(unavailable),
        "winner": (
            {
                "candidate_id": winner["candidate_id"],
                "route": winner["route"],
                "text": winner["text"],
                "length_chars": winner["length_chars"],
                "fusion_score": winner["fusion_score"],
            }
            if winner is not None
            else None
        ),
        "error": (
            None
            if winner is not None
            else {
                "code": "E_NO_AVAILABLE_CANDIDATE",
                "message": "All candidates were marked unavailable",
                "doc_id": doc["doc_id"],
            }
        ),
    }
