from __future__ import annotations

from typing import Any

from .canonical import fixed_fp, fixed_str
from .config import Config


def _basis(record: dict[str, Any]) -> dict[str, Any]:
    sentences = []
    for item in record.get("sentence_support", []):
        best = item.get("best_evidence") or {}
        sentences.append(
            {
                "sentence_index": item["sentence_index"],
                "score": item["score"],
                "supported": item["supported"],
                "rule_mismatches": item["features"].get("rule_mismatches", []),
                "best_evidence_index": best.get("evidence_index"),
                "best_evidence_sentence_index": best.get("sentence_index"),
            }
        )
    return {
        "hallucination": record.get("hallucination"),
        "consistency_score": record.get("consistency_score", 0.0),
        "unsupported_ratio": record.get("unsupported_ratio", 0.0),
        "sentence_support": sentences,
    }


def _confusion_for_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    tp = tn = fp = fn = 0
    false_positive: list[dict[str, Any]] = []
    false_negative: list[dict[str, Any]] = []
    comparable = 0

    for record in records:
        manual = record["annotation"]["is_hallucination"]
        automatic = record["hallucination"]
        if manual is None or automatic is None or record["status"] != "ok":
            continue
        comparable += 1
        if automatic and manual:
            tp += 1
        elif not automatic and not manual:
            tn += 1
        elif automatic and not manual:
            fp += 1
            false_positive.append(
                {
                    "sample_id": record["sample_id"],
                    "automatic_hallucination": True,
                    "manual_hallucination": False,
                    "basis": _basis(record),
                }
            )
        else:
            fn += 1
            false_negative.append(
                {
                    "sample_id": record["sample_id"],
                    "automatic_hallucination": False,
                    "manual_hallucination": True,
                    "basis": _basis(record),
                }
            )

    accuracy = (tp + tn) / comparable if comparable else None
    accuracy_fp = fixed_fp(accuracy) if accuracy is not None else None
    return {
        "true_positive": tp,
        "true_negative": tn,
        "false_positive": fp,
        "false_negative": fn,
        "comparable_count": comparable,
        "accuracy": fixed_str(accuracy_fp) if accuracy_fp is not None else None,
        "accuracy_fp": accuracy_fp,
        "false_positive_samples": sorted(false_positive, key=lambda item: item["sample_id"]),
        "false_negative_samples": sorted(false_negative, key=lambda item: item["sample_id"]),
    }


def validate_annotations(
    stage2_payload: dict[str, Any], config: Config
) -> dict[str, Any]:
    records = sorted(stage2_payload["records"], key=lambda item: item["sample_id"])
    sources = sorted({record["source"] for record in records})
    source_reviews: list[dict[str, Any]] = []
    source_review_flags: dict[str, bool] = {}

    for source in sources:
        subset = [record for record in records if record["source"] == source]
        manual_labeled = [
            record for record in subset if record["annotation"]["is_hallucination"] is not None
        ]
        missing_ids = sorted(
            record["sample_id"]
            for record in subset
            if record["annotation"]["is_hallucination"] is None
        )
        coverage = len(manual_labeled) / len(subset) if subset else 0.0
        coverage_fp = fixed_fp(coverage)
        confusion = _confusion_for_records(subset)
        accuracy = None
        if confusion["accuracy_fp"] is not None:
            accuracy = confusion["accuracy_fp"] / 1_000_000
        accuracy_low = accuracy is not None and accuracy < config.consistency_min
        coverage_low = coverage < config.min_annotation_coverage
        reasons: list[str] = []
        recommendations: list[str] = []
        if accuracy_low:
            reasons.append("LOW_ANNOTATION_CONSISTENCY")
            recommendations.append(
                "自动判定与人工标注一致率 "
                f"{fixed_str(confusion['accuracy_fp'])} 低于下限 "
                f"{fixed_str(fixed_fp(config.consistency_min))}；请复核误判与漏判样本。"
            )
        if coverage_low:
            reasons.append("LOW_ANNOTATION_COVERAGE")
            recommendations.append(
                "人工标注覆盖率 "
                f"{fixed_str(coverage_fp)} 低于下限 "
                f"{fixed_str(fixed_fp(config.min_annotation_coverage))}；请补齐样本："
                + (", ".join(missing_ids) if missing_ids else "(无缺失样本)")
                + "。"
            )
        review_required = bool(reasons)
        source_review_flags[source] = review_required
        source_reviews.append(
            {
                "source": source,
                "sample_count": len(subset),
                "manual_labeled_count": len(manual_labeled),
                "missing_annotation_count": len(missing_ids),
                "missing_annotation_sample_ids": missing_ids,
                "coverage_fp": coverage_fp,
                "coverage": fixed_str(coverage_fp),
                "review_required": review_required,
                "review_reasons": sorted(reasons),
                "recommendations": recommendations,
                "confusion": confusion,
            }
        )

    review_map: dict[str, set[str]] = {
        record["sample_id"]: set(record.get("review_reasons", [])) for record in records
    }
    for record in records:
        if record["annotation"]["is_hallucination"] is None:
            review_map[record["sample_id"]].add("MISSING_ANNOTATION")
        if record["annotation"]["needs_review"] is True:
            review_map[record["sample_id"]].add("MANUAL_REVIEW_REQUESTED")
        if source_review_flags.get(record["source"], False):
            review_map[record["sample_id"]].add("SOURCE_REVIEW_REQUIRED")

    samples_requiring_review = [
        {"sample_id": sample_id, "reasons": sorted(reasons)}
        for sample_id, reasons in sorted(review_map.items())
        if reasons
    ]
    overall = _confusion_for_records(records)
    return {
        "stage": "validate",
        "consistency_min": config.consistency_min,
        "min_annotation_coverage": config.min_annotation_coverage,
        "overall_confusion": overall,
        "source_reviews": sorted(source_reviews, key=lambda item: item["source"]),
        "samples_requiring_review": samples_requiring_review,
    }
