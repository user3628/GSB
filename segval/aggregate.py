"""Stage 3: deterministic aggregation from sorted sample-level match details."""

from __future__ import annotations

from typing import Any, Iterable

from .canonical import digest_obj, quantize


UNMATCHED_SOURCE = "__unmatched__"

COUNTER_KEYS = (
    "eligible_ground_truth",
    "eligible_predictions",
    "raw_ground_truth",
    "raw_predictions",
    "hits",
    "misses",
    "duplicates",
    "background_false_positives",
    "invalid_ground_truth",
    "invalid_predictions",
    "iou_sum",
    "matched_iou_count",
    "candidate_evaluations",
)


def _empty() -> dict[str, float | int]:
    return {key: 0 for key in COUNTER_KEYS}


def _add(acc: dict[str, float | int], detail: dict[str, Any]) -> None:
    counts = detail["counts"]
    acc["eligible_ground_truth"] += detail["eligible_ground_truth"]
    acc["eligible_predictions"] += detail["eligible_predictions"]
    acc["raw_ground_truth"] += detail["eligible_ground_truth"] + counts["invalid_ground_truth"]
    acc["raw_predictions"] += detail["eligible_predictions"] + counts["invalid_predictions"]
    acc["hits"] += counts["hits"]
    acc["misses"] += counts["misses"]
    acc["duplicates"] += counts["duplicates"]
    acc["background_false_positives"] += counts["background_false_positives"]
    acc["invalid_ground_truth"] += counts["invalid_ground_truth"]
    acc["invalid_predictions"] += counts["invalid_predictions"]
    acc["iou_sum"] += counts["iou_sum"]
    acc["matched_iou_count"] += counts["matched_iou_count"]
    acc["candidate_evaluations"] += detail["candidate_evaluations"]


def _source_acc_for(detail: dict[str, Any]) -> dict[str, dict[str, float | int]]:
    sources: dict[str, dict[str, float | int]] = {}

    def source_acc(name: str) -> dict[str, float | int]:
        if name not in sources:
            sources[name] = _empty()
        return sources[name]

    for hit in detail["hits"]:
        acc = source_acc(hit["source_operator"])
        acc["eligible_ground_truth"] += 1
        acc["raw_ground_truth"] += 1
        acc["eligible_predictions"] += 1
        acc["raw_predictions"] += 1
        acc["hits"] += 1
        acc["iou_sum"] += hit["iou"]
        acc["matched_iou_count"] += 1
    for prediction in detail["duplicate_predictions"]:
        acc = source_acc(prediction["source_operator"])
        acc["eligible_predictions"] += 1
        acc["raw_predictions"] += 1
        acc["duplicates"] += 1
    for prediction in detail["background_false_positives"]:
        acc = source_acc(prediction["source_operator"])
        acc["eligible_predictions"] += 1
        acc["raw_predictions"] += 1
        acc["background_false_positives"] += 1
    for prediction in detail["invalid_predictions"]:
        acc = source_acc(prediction["source_operator"])
        acc["raw_predictions"] += 1
        acc["invalid_predictions"] += 1
    if detail["missed_ground_truth"]:
        acc = source_acc(UNMATCHED_SOURCE)
        acc["eligible_ground_truth"] += len(detail["missed_ground_truth"])
        acc["raw_ground_truth"] += len(detail["missed_ground_truth"])
        acc["misses"] += len(detail["missed_ground_truth"])
    return sources


def _finalize(acc: dict[str, float | int], *, precision: int = 9) -> dict[str, Any]:
    gt = int(acc["eligible_ground_truth"])
    predictions = int(acc["eligible_predictions"])
    matches = int(acc["matched_iou_count"])
    metric_inputs = {
        "hit_rate": (acc["hits"], gt),
        "miss_rate": (acc["misses"], gt),
        "duplicate_rate": (acc["duplicates"], predictions),
        "background_false_positive_rate": (
            acc["background_false_positives"],
            predictions,
        ),
        "average_iou": (acc["iou_sum"], matches),
    }
    metrics: dict[str, Any] = {}
    na_metrics: list[str] = []
    for name, (numerator, denominator) in metric_inputs.items():
        if denominator == 0:
            metrics[name] = None
            metrics[f"{name}_status"] = "N/A"
            na_metrics.append(name)
        else:
            metrics[name] = quantize(float(numerator) / float(denominator), precision)
            metrics[f"{name}_status"] = "OK"
    result: dict[str, Any] = {
        "eligible_ground_truth": gt,
        "eligible_predictions": predictions,
        "raw_ground_truth": int(acc["raw_ground_truth"]),
        "raw_predictions": int(acc["raw_predictions"]),
        "hits": int(acc["hits"]),
        "misses": int(acc["misses"]),
        "duplicates": int(acc["duplicates"]),
        "background_false_positives": int(acc["background_false_positives"]),
        "invalid_ground_truth": int(acc["invalid_ground_truth"]),
        "invalid_predictions": int(acc["invalid_predictions"]),
        "iou_sum": quantize(float(acc["iou_sum"]), 12),
        "matched_iou_count": matches,
        "candidate_evaluations": int(acc["candidate_evaluations"]),
        "na_metrics": na_metrics,
        "na_metric_count": len(na_metrics),
    }
    result.update(metrics)
    return result


def _identity(shard: dict[str, Any]) -> dict[str, Any]:
    return {
        "shard_id": shard["shard_id"],
        "shard_index": shard["shard_index"],
    }


def aggregate_match_artifacts(match_artifacts: Iterable[dict[str, Any]]) -> dict[str, Any]:
    artifacts = list(match_artifacts)
    details = [detail for artifact in artifacts for detail in artifact["details"]]
    details.sort(key=lambda item: (item["shard_index"], item["image_id"], item["class_name"]))

    by_shard_class_acc: dict[tuple[int, str, str], dict[str, float | int]] = {}
    by_shard_class_source_acc: dict[tuple[int, str, str, str], dict[str, float | int]] = {}
    by_shard_acc: dict[tuple[int, str], dict[str, float | int]] = {}
    by_class_acc: dict[str, dict[str, float | int]] = {}
    totals = _empty()

    for detail in details:
        shard_index = detail["shard_index"]
        shard_id = detail["shard_id"]
        class_name = detail["class_name"]
        for key in (
            (shard_index, shard_id, class_name),
            (shard_index, shard_id),
        ):
            mapping = by_shard_class_acc if len(key) == 3 else by_shard_acc
            mapping.setdefault(key, _empty())
        by_class_acc.setdefault(class_name, _empty())
        _add(by_shard_class_acc[(shard_index, shard_id, class_name)], detail)
        _add(by_shard_acc[(shard_index, shard_id)], detail)
        _add(by_class_acc[class_name], detail)
        _add(totals, detail)

        for source, source_acc in _source_acc_for(detail).items():
            key = (shard_index, shard_id, class_name, source)
            by_shard_class_source_acc.setdefault(key, _empty())
            target = by_shard_class_source_acc[key]
            for counter in COUNTER_KEYS:
                target[counter] += source_acc[counter]

    by_shard_class: list[dict[str, Any]] = []
    for (shard_index, shard_id, class_name), acc in sorted(by_shard_class_acc.items()):
        row = {"shard_index": shard_index, "shard_id": shard_id, "class_name": class_name}
        row.update(_finalize(acc))
        by_shard_class.append(row)

    by_shard_class_source: list[dict[str, Any]] = []
    for (shard_index, shard_id, class_name, source), acc in sorted(
        by_shard_class_source_acc.items()
    ):
        row = {
            "shard_index": shard_index,
            "shard_id": shard_id,
            "class_name": class_name,
            "source_operator": source,
        }
        row.update(_finalize(acc))
        by_shard_class_source.append(row)

    by_shard: list[dict[str, Any]] = []
    for (shard_index, shard_id), acc in sorted(by_shard_acc.items()):
        row = {"shard_index": shard_index, "shard_id": shard_id}
        row.update(_finalize(acc))
        by_shard.append(row)

    by_class: list[dict[str, Any]] = []
    for class_name, acc in sorted(by_class_acc.items()):
        row = {"class_name": class_name}
        row.update(_finalize(acc))
        by_class.append(row)

    shard_digests = sorted(
        (
            {
                **_identity(artifact),
                "match_artifact_digest": artifact["artifact_digest"],
            }
            for artifact in artifacts
        ),
        key=lambda item: (item["shard_index"], item["shard_id"]),
    )
    result: dict[str, Any] = {
        "schema_version": 1,
        "stage": "aggregate",
        "source_shards": shard_digests,
        "detail_count": len(details),
        "totals": _finalize(totals),
        "by_shard": by_shard,
        "by_shard_class": by_shard_class,
        "by_shard_class_source": by_shard_class_source,
        "by_class": by_class,
        "consistency": {
            "detail_order": "shard_index,image_id,class_name",
            "detail_set_digest": digest_obj(
                [
                    {
                        "shard_index": detail["shard_index"],
                        "image_id": detail["image_id"],
                        "class_name": detail["class_name"],
                        "counts": detail["counts"],
                    }
                    for detail in details
                ]
            ),
            "detail_count": len(details),
        },
    }
    result["artifact_digest"] = digest_obj(result)
    return result

