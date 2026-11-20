"""Candidate classification, per-source/per-class quality and reduction.

A candidate that is not consumed by a ground truth falls into exactly one of:

* ``duplicate_candidates``  - IoU >= threshold but another candidate won the GT
* ``background_candidates`` - IoU < threshold but score >= score threshold (FP)
* ``low_confidence_candidates`` - IoU and score both below their thresholds

Per-source aggregates report hits (matched), false positives (background),
duplicates, low-confidence items and the mean IoU over every candidate the
source emitted for the image.
"""
from __future__ import annotations

from typing import Any


def _round6(value: float) -> float:
    return round(float(value), 6)


def _new_class_row() -> dict[str, Any]:
    return {
        "ground_truth": 0,
        "matched": 0,
        "unmatched_ground_truth": 0,
        "duplicate": 0,
        "background": 0,
        "low_confidence": 0,
        "candidates": 0,
    }


def _new_source_row() -> dict[str, Any]:
    return {
        "hits": 0,
        "false_positives": 0,
        "duplicates": 0,
        "low_confidence": 0,
        "candidates": 0,
        "iou_sum": 0.0,
        "avg_iou": 0.0,
    }


def build_image_stats(summary: dict[str, Any]) -> dict[str, Any]:
    classes: dict[str, dict[str, Any]] = {}
    sources: dict[str, dict[str, Any]] = {}

    def class_row(category: str) -> dict[str, Any]:
        return classes.setdefault(category, _new_class_row())

    def source_row(source: str) -> dict[str, Any]:
        return sources.setdefault(source, _new_source_row())

    for gt in summary["unmatched_ground_truth"]:
        class_row(gt["category"])["unmatched_ground_truth"] += 1
    # Ground truth counts per class are derived from match + unmatched lists.
    gt_counts: dict[str, int] = {}
    for match in summary["matches"]:
        gt_counts[match["category"]] = gt_counts.get(match["category"], 0) + 1
    for gt in summary["unmatched_ground_truth"]:
        gt_counts[gt["category"]] = gt_counts.get(gt["category"], 0) + 1
    for category, count in gt_counts.items():
        class_row(category)["ground_truth"] += count

    def account(entry: dict[str, Any], bucket: str) -> None:
        class_row(entry["category"])["candidates"] += 1
        class_row(entry["category"])[bucket] += 1
        row = source_row(entry["source"])
        row["candidates"] += 1
        row["iou_sum"] += float(entry["iou"])
        if bucket == "matched":
            row["hits"] += 1
        elif bucket == "background":
            row["false_positives"] += 1
        elif bucket == "duplicate":
            row["duplicates"] += 1
        else:
            row["low_confidence"] += 1

    for match in summary["matches"]:
        account(match, "matched")
    for entry in summary["duplicate_candidates"]:
        account(entry, "duplicate")
    for entry in summary["background_candidates"]:
        account(entry, "background")
    for entry in summary["low_confidence_candidates"]:
        account(entry, "low_confidence")

    for row in sources.values():
        row["iou_sum"] = _round6(row["iou_sum"])
        row["avg_iou"] = (
            _round6(row["iou_sum"] / row["candidates"]) if row["candidates"] else 0.0
        )

    return {
        "image_id": summary["image_id"],
        "by_class": {key: classes[key] for key in sorted(classes)},
        "by_source": {key: sources[key] for key in sorted(sources)},
    }


def reduce_global(image_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """Reduce per-image summaries into one global statistics document.

    Summaries are sorted by ``image_id`` first so the output is byte-identical
    regardless of the order shards happened to finish in.
    """
    ordered = sorted(image_summaries, key=lambda item: item["image_id"])
    classes: dict[str, dict[str, Any]] = {}
    sources: dict[str, dict[str, Any]] = {}
    totals = {
        "images": 0,
        "ground_truth": 0,
        "candidates_evaluated": 0,
        "matched": 0,
        "unmatched_ground_truth": 0,
        "duplicate": 0,
        "background": 0,
        "low_confidence": 0,
    }

    for summary in ordered:
        image_stats = build_image_stats(summary)
        totals["images"] += 1
        totals["ground_truth"] += summary["num_ground_truth"]
        totals["candidates_evaluated"] += summary["num_candidates"]
        totals["matched"] += len(summary["matches"])
        totals["unmatched_ground_truth"] += len(summary["unmatched_ground_truth"])
        totals["duplicate"] += len(summary["duplicate_candidates"])
        totals["background"] += len(summary["background_candidates"])
        totals["low_confidence"] += len(summary["low_confidence_candidates"])

        for category, row in image_stats["by_class"].items():
            target = classes.setdefault(category, _new_class_row())
            for key, value in row.items():
                target[key] += value
        for source, row in image_stats["by_source"].items():
            target = sources.setdefault(source, _new_source_row())
            target["hits"] += row["hits"]
            target["false_positives"] += row["false_positives"]
            target["duplicates"] += row["duplicates"]
            target["low_confidence"] += row["low_confidence"]
            target["candidates"] += row["candidates"]
            target["iou_sum"] = round(target["iou_sum"] + row["iou_sum"], 6)

    for row in sources.values():
        row["avg_iou"] = (
            _round6(row["iou_sum"] / row["candidates"]) if row["candidates"] else 0.0
        )

    return {
        "totals": totals,
        "by_class": {key: classes[key] for key in sorted(classes)},
        "by_source": {key: sources[key] for key in sorted(sources)},
    }
