"""Stage 2: deterministic per-class unique greedy IoU matching."""

from __future__ import annotations

from typing import Any, Callable

from .canonical import digest_obj, quantize
from .config import matching_config
from .polygons import polygon_iou


def _candidate_key(candidate: dict[str, Any], strategy: str) -> tuple[Any, ...]:
    if strategy == "greedy_global_iou_confidence_tiebreak":
        return (
            -candidate["iou"],
            -candidate["confidence"],
            candidate["ground_truth_id"],
            candidate["prediction_id"],
        )
    return (-candidate["iou"], candidate["ground_truth_id"], candidate["prediction_id"])


def match_shard(
    decoded_artifact: dict[str, Any],
    config: dict[str, Any],
    *,
    stop_requested: Callable[[], bool] | None = None,
    sample_boundary: Callable[[], None] | None = None,
) -> dict[str, Any] | None:
    match_options = matching_config(config)
    threshold = float(match_options["iou_threshold"])
    duplicate_confidence = float(match_options["duplicate_confidence_threshold"])
    details: list[dict[str, Any]] = []
    total_evaluations = 0

    to_points = lambda instance: [(float(x), float(y)) for x, y in instance["vertices_px"]]

    for sample in sorted(decoded_artifact["samples"], key=lambda item: item["image_id"]):
        if stop_requested is not None and stop_requested():
            return None
        class_names = sorted(sample["class_names"])
        for class_name in class_names:
            ground_truth = [
                item
                for item in sample["ground_truth"]
                if item["class_name"] == class_name and item["valid"]
            ]
            invalid_ground_truth = [
                item
                for item in sample["ground_truth"]
                if item["class_name"] == class_name and not item["valid"]
            ]
            predictions = [
                item
                for item in sample["predictions"]
                if item["class_name"] == class_name and item["valid"]
            ]
            invalid_predictions = [
                item
                for item in sample["predictions"]
                if item["class_name"] == class_name and not item["valid"]
            ]

            candidates: list[dict[str, Any]] = []
            candidate_lookup: dict[tuple[str, str], dict[str, Any]] = {}
            for ground_truth_instance in ground_truth:
                gt_points = to_points(ground_truth_instance)
                for prediction in predictions:
                    prediction_points = to_points(prediction)
                    iou = polygon_iou(
                        gt_points,
                        prediction_points,
                        ground_truth_instance["area"],
                        prediction["area"],
                    )
                    candidate = {
                        "ground_truth_id": ground_truth_instance["instance_id"],
                        "prediction_id": prediction["instance_id"],
                        "confidence": float(prediction.get("confidence", 0.0)),
                        "iou": iou,
                    }
                    candidates.append(candidate)
                    candidate_lookup[(candidate["ground_truth_id"], candidate["prediction_id"])] = candidate
            total_evaluations += len(candidates)
            candidates.sort(key=lambda item: _candidate_key(item, match_options["strategy"]))

            matched_ground_truth: set[str] = set()
            matched_predictions: set[str] = set()
            hits: list[dict[str, Any]] = []
            for candidate in candidates:
                if candidate["iou"] < threshold:
                    break
                gt_id = candidate["ground_truth_id"]
                prediction_id = candidate["prediction_id"]
                if gt_id in matched_ground_truth or prediction_id in matched_predictions:
                    continue
                prediction = next(item for item in predictions if item["instance_id"] == prediction_id)
                matched_ground_truth.add(gt_id)
                matched_predictions.add(prediction_id)
                hits.append(
                    {
                        "ground_truth_id": gt_id,
                        "prediction_id": prediction_id,
                        "iou": quantize(candidate["iou"], 12),
                        "confidence": prediction.get("confidence", 0.0),
                        "source_operator": prediction.get("source_operator", "unknown"),
                    }
                )

            missed_ground_truth = [
                {
                    "ground_truth_id": item["instance_id"],
                    "class_name": class_name,
                }
                for item in ground_truth
                if item["instance_id"] not in matched_ground_truth
            ]

            duplicates: list[dict[str, Any]] = []
            background_false_positives: list[dict[str, Any]] = []
            for prediction in predictions:
                prediction_id = prediction["instance_id"]
                if prediction_id in matched_predictions:
                    continue
                best_candidate = max(
                    (
                        candidate
                        for candidate in candidates
                        if candidate["prediction_id"] == prediction_id
                    ),
                    key=lambda item: (item["iou"], item["ground_truth_id"]),
                    default=None,
                )
                confidence = float(prediction.get("confidence", 0.0))
                record = {
                    "prediction_id": prediction_id,
                    "class_name": class_name,
                    "confidence": quantize(confidence, 9),
                    "source_operator": prediction.get("source_operator", "unknown"),
                    "best_iou": quantize(best_candidate["iou"], 12) if best_candidate else 0.0,
                    "best_ground_truth_id": best_candidate["ground_truth_id"] if best_candidate else None,
                }
                if confidence >= duplicate_confidence:
                    record["classification_reason"] = (
                        f"UNMATCHED_CONFIDENCE_GTE_{duplicate_confidence:g}"
                    )
                    duplicates.append(record)
                else:
                    record["classification_reason"] = (
                        f"UNMATCHED_CONFIDENCE_LT_{duplicate_confidence:g}"
                    )
                    background_false_positives.append(record)

            iou_sum = sum(item["iou"] for item in hits)
            detail = {
                "schema_version": 1,
                "shard_id": decoded_artifact["shard_id"],
                "shard_index": decoded_artifact["shard_index"],
                "image_id": sample["image_id"],
                "width": sample["width"],
                "height": sample["height"],
                "class_name": class_name,
                "matching": {
                    "iou_threshold": threshold,
                    "duplicate_confidence_threshold": duplicate_confidence,
                    "strategy": match_options["strategy"],
                },
                "eligible_ground_truth": len(ground_truth),
                "eligible_predictions": len(predictions),
                "invalid_ground_truth": [
                    {
                        "ground_truth_id": item["instance_id"],
                        "reason_codes": item["reason_codes"],
                    }
                    for item in invalid_ground_truth
                ],
                "invalid_predictions": [
                    {
                        "prediction_id": item["instance_id"],
                        "confidence": item.get("confidence", 0.0),
                        "source_operator": item.get("source_operator", "unknown"),
                        "reason_codes": item["reason_codes"],
                    }
                    for item in invalid_predictions
                ],
                "hits": hits,
                "missed_ground_truth": missed_ground_truth,
                "duplicate_predictions": duplicates,
                "background_false_positives": background_false_positives,
                "candidate_evaluations": len(candidates),
                "counts": {
                    "hits": len(hits),
                    "misses": len(missed_ground_truth),
                    "duplicates": len(duplicates),
                    "background_false_positives": len(background_false_positives),
                    "invalid_ground_truth": len(invalid_ground_truth),
                    "invalid_predictions": len(invalid_predictions),
                    "matched_iou_count": len(hits),
                    "iou_sum": quantize(iou_sum, 12),
                },
            }
            details.append(detail)
        if sample_boundary is not None:
            sample_boundary()

    details.sort(key=lambda item: (item["shard_index"], item["image_id"], item["class_name"]))
    artifact: dict[str, Any] = {
        "schema_version": 1,
        "stage": "match",
        "shard_id": decoded_artifact["shard_id"],
        "shard_index": decoded_artifact["shard_index"],
        "decode_artifact_digest": decoded_artifact["artifact_digest"],
        "shard_content_digest": decoded_artifact["shard_content_digest"],
        "class_set": decoded_artifact["class_set"],
        "matching": match_options,
        "details": details,
        "counts": {
            "sample_class_details": len(details),
            "candidate_evaluations": total_evaluations,
            "hits": sum(item["counts"]["hits"] for item in details),
            "misses": sum(item["counts"]["misses"] for item in details),
            "duplicates": sum(item["counts"]["duplicates"] for item in details),
            "background_false_positives": sum(
                item["counts"]["background_false_positives"] for item in details
            ),
            "invalid_ground_truth": sum(
                item["counts"]["invalid_ground_truth"] for item in details
            ),
            "invalid_predictions": sum(
                item["counts"]["invalid_predictions"] for item in details
            ),
        },
    }
    artifact["artifact_digest"] = digest_obj(artifact)
    return artifact

