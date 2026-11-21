from __future__ import annotations

from decimal import Decimal
from typing import Any

from .canonical import sha256_value


def _ratio(numerator: int, denominator: int, scale: int) -> str:
    if denominator == 0:
        return "0." + "0" * scale if scale else "0"
    return format((Decimal(numerator) / Decimal(denominator)).quantize(Decimal(1).scaleb(-scale)), f".{scale}f")


def _top_prediction(candidates: list[dict[str, Any]], element_types: set[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for element_type in sorted(element_types):
        valid = [
            candidate
            for candidate in candidates
            if candidate.get("element_type") == element_type
            and candidate.get("valid")
            and candidate.get("normalized_value") is not None
        ]
        if not valid:
            continue
        valid.sort(key=lambda item: (-float(item["confidence"]), item["candidate_id"]))
        result.append(valid[0])
    return result


def compute_metrics(
    extracted: list[dict[str, Any]],
    resolved: list[dict[str, Any]],
    gold_conflicts: list[dict[str, Any]],
    interop_compatibility: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    element_types = set(config["experiment"].get("element_types", []))
    scale = int(config["experiment"].get("metric_scale", 6))
    predicted: set[tuple[str, str, str]] = set()
    gold: set[tuple[str, str, str]] = set()
    by_type: dict[str, dict[str, int]] = {
        element_type: {"tp": 0, "fp": 0, "fn": 0, "predicted": 0, "gold": 0}
        for element_type in sorted(element_types)
    }

    for clause in extracted:
        clause_id = clause["clause_id"]
        for candidate in _top_prediction(clause.get("candidates", []), element_types):
            key = (clause_id, candidate["element_type"], candidate["normalized_value"])
            if key not in predicted:
                predicted.add(key)
                by_type[candidate["element_type"]]["predicted"] += 1
        for annotation in clause.get("annotations", []):
            value = annotation.get("normalized_value")
            if value is None:
                continue
            key = (clause_id, annotation["element_type"], str(value))
            if key not in gold:
                gold.add(key)
                by_type[annotation["element_type"]]["gold"] += 1

    true_positive = predicted.intersection(gold)
    for key in true_positive:
        by_type[key[1]]["tp"] += 1
    for key in predicted.difference(gold):
        by_type[key[1]]["fp"] += 1
    for key in gold.difference(predicted):
        by_type[key[1]]["fn"] += 1

    def summarize(counts: dict[str, int]) -> dict[str, Any]:
        tp, fp, fn = counts["tp"], counts["fp"], counts["fn"]
        precision = _ratio(tp, tp + fp, scale)
        recall = _ratio(tp, tp + fn, scale)
        f1 = _ratio(2 * tp, 2 * tp + fp + fn, scale)
        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "predicted": counts["predicted"],
            "gold": counts["gold"],
        }

    overall_counts = {
        "tp": len(true_positive),
        "fp": len(predicted.difference(gold)),
        "fn": len(gold.difference(predicted)),
        "predicted": len(predicted),
        "gold": len(gold),
    }
    per_type = {element_type: summarize(by_type[element_type]) for element_type in sorted(by_type)}
    element_metrics = summarize(overall_counts)
    element_metrics["by_type"] = per_type

    resolutions = {
        (item["contract_id"], item["element_type"]): item
        for item in resolved
        if item.get("type") == "resolved_element"
    }
    correct = 0
    checked = 0
    conflict_details: list[dict[str, Any]] = []
    for gold_conflict in gold_conflicts:
        checked += 1
        key = (gold_conflict["contract_id"], gold_conflict["element_type"])
        diagnostic = resolutions.get(key)
        passed = bool(
            diagnostic
            and diagnostic.get("conflict")
            and diagnostic.get("value") == gold_conflict.get("winner")
            and diagnostic.get("eliminated")
        )
        correct += int(passed)
        conflict_details.append(
            {
                "contract_id": key[0],
                "element_type": key[1],
                "expected_winner": gold_conflict.get("winner"),
                "actual_winner": None if diagnostic is None else diagnostic.get("value"),
                "diagnostic_conflict": bool(diagnostic and diagnostic.get("conflict")),
                "eliminated_count": 0 if diagnostic is None else len(diagnostic.get("eliminated", [])),
                "correct": passed,
            }
        )
    conflict_details.sort(key=lambda item: (item["contract_id"], item["element_type"]))

    total_interop = int(interop_compatibility.get("input_records", 0))
    successful_interop = int(interop_compatibility.get("successful_records", 0))
    interop_metrics = {
        "success": bool(interop_compatibility.get("success", False)),
        "successful_records": successful_interop,
        "total_records": total_interop,
        "success_rate": _ratio(successful_interop, total_interop, scale),
        "converted_records": int(interop_compatibility.get("converted_records", 0)),
        "first_export_sha256": interop_compatibility.get("first_export_sha256"),
        "second_export_sha256": interop_compatibility.get("second_export_sha256"),
    }
    return {
        "element_extraction": element_metrics,
        "conflict_resolution": {
            "correct": correct,
            "total": checked,
            "accuracy": _ratio(correct, checked, scale),
            "details": conflict_details,
        },
        "version_compatibility": interop_metrics,
    }


def experiment_identity(
    config: dict[str, Any],
    input_rows: list[dict[str, Any]],
    seed_rows: list[dict[str, Any]],
    dictionary_entries: list[dict[str, Any]],
    gold_conflicts: list[dict[str, Any]],
    v1_rows: list[dict[str, Any]],
) -> str:
    return sha256_value(
        {
            "config": config,
            "inputs": input_rows,
            "seeds": seed_rows,
            "dictionary": dictionary_entries,
            "gold_conflicts": gold_conflicts,
            "structure_v1": v1_rows,
        }
    )

