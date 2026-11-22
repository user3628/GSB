from __future__ import annotations

import math
from typing import Any, Iterable

import numpy as np

from .canonical import FIXED_SCALE, fixed_fp, fixed_str, round_float
from .config import Config


def _log_domain_mean(records: list[dict[str, Any]]) -> tuple[float, float | None]:
    """Sequentially accumulate sorted records with logaddexp (never raw-sum)."""
    log_sum = -math.inf
    count = 0
    for record in sorted(records, key=lambda item: item["sample_id"]):
        if record["status"] != "ok":
            continue
        score = max(float(record["consistency_score"]), 1e-12)
        log_sum = float(np.logaddexp(log_sum, math.log(score)))
        count += 1
    if count == 0:
        return 0.0, None
    return math.exp(log_sum - math.log(count)), log_sum


def _aggregate(records: list[dict[str, Any]]) -> dict[str, Any]:
    sample_count = len(records)
    judged = [record for record in records if record["status"] == "ok"]
    judged_count = len(judged)
    hallucination_count = sum(1 for record in judged if record["hallucination"] is True)
    pending_review_count = sum(1 for record in records if record["needs_review"])
    error_count = sum(1 for record in records if record["status"] != "ok")
    missing_annotation_count = sum(
        1
        for record in records
        if "MISSING_ANNOTATION" in record.get("review_reasons", [])
    )
    avg_consistency, log_sum = _log_domain_mean(records)
    hallucination_rate = hallucination_count / judged_count if judged_count else 0.0
    rate_fp = fixed_fp(hallucination_rate)
    avg_fp = fixed_fp(avg_consistency)
    return {
        "sample_count": sample_count,
        "judged_count": judged_count,
        "hallucination_count": hallucination_count,
        "hallucination_rate_fp": rate_fp,
        "hallucination_rate": fixed_str(rate_fp),
        "avg_consistency_fp": avg_fp,
        "avg_consistency": fixed_str(avg_fp),
        "pending_review_count": pending_review_count,
        "error_count": error_count,
        "missing_annotation_count": missing_annotation_count,
        "log_consistency_sum": round_float(log_sum) if log_sum is not None else None,
    }


def aggregate_records(
    stage2_payload: dict[str, Any], config: Config
) -> dict[str, Any]:
    records = sorted(stage2_payload["records"], key=lambda item: item["sample_id"])
    pairs = {(record["source"], record["lang"]) for record in records}
    pairs.update(
        (item["source"], item["lang"]) for item in config.values["declared_strata"]
    )

    strata: list[dict[str, Any]] = []
    for source, lang in sorted(pairs):
        subset = [record for record in records if record["source"] == source and record["lang"] == lang]
        item = {"source": source, "lang": lang}
        item.update(_aggregate(subset))
        strata.append(item)

    totals = _aggregate(records)
    return {
        "stage": "aggregate",
        "quantization_scale": FIXED_SCALE,
        "log_accumulation": "sequential np.logaddexp over sample_id-sorted consistency scores",
        "declared_strata": sorted(
            [{"source": item["source"], "lang": item["lang"]} for item in config.values["declared_strata"]],
            key=lambda item: (item["source"], item["lang"]),
        ),
        "strata": strata,
        "totals": totals,
    }


def aggregate_consistency_from_samples(
    records: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Public helper used by acceptance tests to independently accumulate a layer."""
    return _aggregate(list(records))
