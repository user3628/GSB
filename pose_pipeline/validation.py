"""Stage 4: quantisation against manifest truth and deterministic verification."""

from __future__ import annotations

import math
from collections import defaultdict
from functools import lru_cache
from typing import Any

from .config import PipelineConfig
from .constants import SCHEMA_VERSION, VALIDATION
from .models import PoseTruth, ShardSpec, coordinate_to_pixel
from .util import digest_json, finalize_artifact, round_float


def validate_shard(
    assembly_artifact: dict[str, Any],
    shard: ShardSpec,
    config: PipelineConfig,
    *,
    input_fingerprint: str,
) -> dict[str, Any]:
    predictions = list(assembly_artifact.get("instances", []))
    truth_instances = list(shard.instances)
    assignment = _match_instances(truth_instances, predictions, shard)
    matched_predictions: set[int] = {
        index for index in assignment.values() if index is not None
    }
    instance_reports: list[dict[str, Any]] = []
    total_visible = 0
    total_matched = 0
    total_missing = 0
    total_ignored = 0
    total_hits = 0
    all_errors: list[float] = []

    for truth_index, truth in enumerate(truth_instances):
        prediction_index = assignment.get(truth_index)
        prediction = predictions[prediction_index] if prediction_index is not None else None
        interval = truth.expected_confidence or shard.expected_confidence
        keypoint_reports: list[dict[str, Any]] = []
        observed_confidences: list[float] = []
        errors_px: list[float] = []
        errors_normalized: list[float] = []
        out_of_interval: list[str] = []
        error_outliers: list[str] = []
        for name in shard.keypoints:
            truth_point = truth.keypoints[name]
            predicted_point = (
                prediction.get("keypoints", {}).get(name) if prediction is not None else None
            )
            report: dict[str, Any] = {
                "keypoint": name,
                "truth": {
                    "x": truth_point.x,
                    "y": truth_point.y,
                    "visible": truth_point.visible,
                },
                "predicted": predicted_point,
            }
            if not truth_point.visible:
                total_ignored += 1
                report.update(
                    {
                        "status": "ignored_invisible",
                        "evaluated": False,
                        "hit": False,
                        "error_px": None,
                        "error_normalized": None,
                        "confidence_in_interval": None,
                    }
                )
                keypoint_reports.append(report)
                continue
            total_visible += 1
            if predicted_point is None:
                total_missing += 1
                report.update(
                    {
                        "status": "missing_candidate",
                        "evaluated": False,
                        "hit": False,
                        "error_px": None,
                        "error_normalized": None,
                        "confidence_in_interval": None,
                    }
                )
                keypoint_reports.append(report)
                continue
            truth_pixel_x = coordinate_to_pixel(truth_point.x, shard.width)
            truth_pixel_y = coordinate_to_pixel(truth_point.y, shard.height)
            dx = float(predicted_point["pixel_x"]) - truth_pixel_x
            dy = float(predicted_point["pixel_y"]) - truth_pixel_y
            error_px = math.hypot(dx, dy)
            error_normalized = math.hypot(
                float(predicted_point["normalized_x"]) - truth_point.x,
                float(predicted_point["normalized_y"]) - truth_point.y,
            )
            confidence = float(predicted_point["response"])
            confidence_in_interval = interval.contains(confidence)
            error_ok = error_px <= config.hit_error_px
            hit = confidence_in_interval and error_ok
            total_matched += 1
            if hit:
                total_hits += 1
            else:
                if not confidence_in_interval:
                    out_of_interval.append(name)
                if not error_ok:
                    error_outliers.append(name)
            observed_confidences.append(confidence)
            errors_px.append(error_px)
            errors_normalized.append(error_normalized)
            all_errors.append(error_px)
            report.update(
                {
                    "status": "matched",
                    "evaluated": True,
                    "hit": hit,
                    "error_px": round_float(error_px, config.round_digits),
                    "error_normalized": round_float(error_normalized, config.round_digits),
                    "confidence": round_float(confidence, config.round_digits),
                    "confidence_in_interval": confidence_in_interval,
                    "error_within_hit_threshold": error_ok,
                }
            )
            keypoint_reports.append(report)

        pose_confidence = (
            sum(observed_confidences) / len(observed_confidences)
            if observed_confidences
            else None
        )
        confidence_out = bool(
            pose_confidence is not None and not interval.contains(pose_confidence)
        )
        if confidence_out and not out_of_interval and observed_confidences:
            out_of_interval = [
                report["keypoint"]
                for report in keypoint_reports
                if report.get("status") == "matched"
            ]
        rmse_px = (
            math.sqrt(sum(value * value for value in errors_px) / len(errors_px))
            if errors_px
            else None
        )
        rmse_normalized = (
            math.sqrt(
                sum(value * value for value in errors_normalized)
                / len(errors_normalized)
            )
            if errors_normalized
            else None
        )
        instance_reports.append(
            {
                "truth_instance_id": truth.instance_id,
                "predicted_instance_id": (
                    prediction.get("instance_id") if prediction is not None else None
                ),
                "pose_confidence": (
                    round_float(pose_confidence, config.round_digits)
                    if pose_confidence is not None
                    else None
                ),
                "expected_confidence_interval": interval.to_mapping(),
                "confidence_out_of_interval": confidence_out,
                "out_of_interval_keypoints": sorted(out_of_interval),
                "error_outlier_keypoints": sorted(error_outliers),
                "evaluated_keypoint_count": len(errors_px),
                "missing_candidate_count": sum(
                    1
                    for report in keypoint_reports
                    if report["status"] == "missing_candidate"
                ),
                "rmse_px": (
                    round_float(rmse_px, config.round_digits)
                    if rmse_px is not None
                    else None
                ),
                "rmse_normalized": (
                    round_float(rmse_normalized, config.round_digits)
                    if rmse_normalized is not None
                    else None
                ),
                "keypoints": keypoint_reports,
            }
        )

    unmatched_predictions = [
        {
            "instance_id": prediction.get("instance_id"),
            "confidence": prediction.get("confidence"),
            "keypoints": prediction.get("keypoints", {}),
        }
        for index, prediction in enumerate(predictions)
        if index not in matched_predictions
    ]
    denominator = max(1, total_visible)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "stage": VALIDATION,
        "shard_id": shard.shard_id,
        "source_label": shard.source_label,
        "input_fingerprint": input_fingerprint,
        "config_version": config.stage_version(VALIDATION),
        "upstream_digest": assembly_artifact["artifact_digest"],
        "image": dict(assembly_artifact["image"]),
        "coordinate_convention": shard_image_convention(assembly_artifact),
        "hit_error_px": config.hit_error_px,
        "instances": instance_reports,
        "unmatched_predictions": unmatched_predictions,
        "geometry_violations": list(assembly_artifact.get("violations", [])),
        "excluded_instances": list(assembly_artifact.get("excluded_instances", [])),
        "counts": {
            "truth_instances": len(truth_instances),
            "predicted_instances": len(predictions),
            "matched_instances": len(matched_predictions),
            "visible_truth_keypoints": total_visible,
            "matched_keypoints": total_matched,
            "missing_candidate_keypoints": total_missing,
            "ignored_invisible_keypoints": total_ignored,
            "hits": total_hits,
            "hit_rate": round_float(total_hits / denominator, config.round_digits),
            "confidence_out_of_interval_instances": sum(
                1
                for item in instance_reports
                if item["confidence_out_of_interval"]
            ),
        },
        "rmse_px": (
            round_float(
                math.sqrt(sum(value * value for value in all_errors) / len(all_errors)),
                config.round_digits,
            )
            if all_errors
            else None
        ),
    }
    return finalize_artifact(payload)


def shard_image_convention(artifact: dict[str, Any]) -> str:
    return str(artifact.get("coordinate_convention", "normalized_topleft_xy"))


def build_final_report(
    summaries: list[dict[str, Any]],
    *,
    manifest_input_fingerprint: str,
    config: PipelineConfig,
    version_fingerprints: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    ordered = sorted(summaries, key=lambda item: str(item["shard_id"]))
    poses_export = {
        "schema_version": SCHEMA_VERSION,
        "coordinate_convention": "normalized_topleft_xy",
        "manifest_input_fingerprint": manifest_input_fingerprint,
        "shards": [
            {
                "shard_id": summary["shard_id"],
                "source_label": summary["source_label"],
                "instances": [
                    {
                        "truth_instance_id": instance["truth_instance_id"],
                        "predicted_instance_id": instance["predicted_instance_id"],
                        "rmse_px": instance["rmse_px"],
                        "rmse_normalized": instance["rmse_normalized"],
                        "pose_confidence": instance["pose_confidence"],
                        "expected_confidence_interval": instance["expected_confidence_interval"],
                        "confidence_out_of_interval": instance["confidence_out_of_interval"],
                        "out_of_interval_keypoints": instance["out_of_interval_keypoints"],
                        "keypoints": instance["keypoints"],
                    }
                    for instance in summary["instances"]
                ],
                "unmatched_predictions": summary["unmatched_predictions"],
            }
            for summary in ordered
        ],
    }
    poses_export = dict(poses_export)
    poses_export["export_digest"] = digest_json(poses_export)

    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_keypoint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_source_keypoint: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    totals = {
        "shards": len(ordered),
        "truth_instances": 0,
        "predicted_instances": 0,
        "matched_instances": 0,
        "visible_truth_keypoints": 0,
        "matched_keypoints": 0,
        "missing_candidate_keypoints": 0,
        "ignored_invisible_keypoints": 0,
        "hits": 0,
        "confidence_out_of_interval_instances": 0,
        "geometry_violations": 0,
        "excluded_instances": 0,
    }
    all_errors: list[float] = []
    shard_entries: list[dict[str, Any]] = []
    for summary in ordered:
        counts = summary["counts"]
        for key in totals:
            if key in counts:
                totals[key] += int(counts[key])
        totals["geometry_violations"] += len(summary.get("geometry_violations", []))
        totals["excluded_instances"] += len(summary.get("excluded_instances", []))
        source = str(summary["source_label"])
        for instance in summary["instances"]:
            for keypoint in instance["keypoints"]:
                if keypoint["status"] == "ignored_invisible":
                    continue
                record = dict(keypoint)
                record["source_label"] = source
                by_source[source].append(record)
                by_keypoint[keypoint["keypoint"]].append(record)
                by_source_keypoint[source][keypoint["keypoint"]].append(record)
                if keypoint["status"] == "matched":
                    all_errors.append(float(keypoint["error_px"]))
        shard_entries.append(
            {
                "shard_id": summary["shard_id"],
                "source_label": source,
                "summary_artifact_digest": summary["artifact_digest"],
                "counts": dict(counts),
                "rmse_px": summary["rmse_px"],
                "confidence_out_of_interval_instances": counts[
                    "confidence_out_of_interval_instances"
                ],
                "geometry_violations": len(summary.get("geometry_violations", [])),
                "excluded_instances": len(summary.get("excluded_instances", [])),
            }
        )
    totals["hit_rate"] = round_float(
        totals["hits"] / max(1, totals["visible_truth_keypoints"]), config.round_digits
    )
    totals["rmse_px"] = (
        round_float(
            math.sqrt(sum(value * value for value in all_errors) / len(all_errors)),
            config.round_digits,
        )
        if all_errors
        else None
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "pipeline": {
            "name": "deterministic-pose-pipeline",
            "version": "1.0.0",
            "coordinate_convention": "normalized_topleft_xy",
        },
        "manifest_input_fingerprint": manifest_input_fingerprint,
        "config": config.to_mapping(),
        "version_fingerprints": version_fingerprints,
        "shards": shard_entries,
        "totals": totals,
        "groups": {
            "by_source": _group_report(by_source, config),
            "by_keypoint": _group_report(by_keypoint, config),
            "by_source_and_keypoint": {
                source: _group_report(keypoint_groups, config)
                for source, keypoint_groups in sorted(by_source_keypoint.items())
            },
        },
        "poses_export_digest": poses_export["export_digest"],
    }
    report["report_digest"] = digest_json(report)
    return poses_export, report


def _match_instances(
    truths: list[PoseTruth],
    predictions: list[dict[str, Any]],
    shard: ShardSpec,
) -> dict[int, int | None]:
    if not truths:
        return {}
    if not predictions:
        return {index: None for index in range(len(truths))}
    costs: list[list[float]] = []
    for truth in truths:
        row: list[float] = []
        for prediction in predictions:
            visible = [point for point in truth.keypoints.values() if point.visible]
            squared_errors: list[float] = []
            for point in visible:
                predicted = prediction.get("keypoints", {}).get(point.name)
                if predicted is None:
                    continue
                dx = float(predicted["normalized_x"]) - point.x
                dy = float(predicted["normalized_y"]) - point.y
                squared_errors.append(dx * dx + dy * dy)
            if not visible:
                row.append(0.0)
                continue
            missing = len(visible) - len(squared_errors)
            mean_error = (
                sum(squared_errors) / len(squared_errors) if squared_errors else 1.0
            )
            row.append(mean_error + 2.0 * missing / len(visible))
        costs.append(row)

    prediction_count = len(predictions)
    skip_cost = 4.0

    @lru_cache(maxsize=None)
    def solve(truth_index: int, used_mask: int) -> tuple[float, tuple[int | None, ...]]:
        if truth_index >= len(truths):
            return 0.0, ()
        best_cost = skip_cost + solve(truth_index + 1, used_mask)[0]
        best_choice: int | None = None
        best_tail: tuple[int | None, ...] = solve(truth_index + 1, used_mask)[1]
        for prediction_index in range(prediction_count):
            bit = 1 << prediction_index
            if used_mask & bit:
                continue
            tail_cost, tail = solve(truth_index + 1, used_mask | bit)
            candidate_cost = costs[truth_index][prediction_index] + tail_cost
            if candidate_cost < best_cost - 1e-15 or (
                abs(candidate_cost - best_cost) <= 1e-15
                and (best_choice is None or prediction_index < best_choice)
            ):
                best_cost = candidate_cost
                best_choice = prediction_index
                best_tail = tail
        return best_cost, (best_choice,) + best_tail

    _, choices = solve(0, 0)
    return {index: choice for index, choice in enumerate(choices)}


def _group_report(
    groups: dict[str, Any], config: PipelineConfig
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for name in sorted(groups):
        records = groups[name]
        if isinstance(records, dict):
            output[name] = _group_report(records, config)
            continue
        errors = [
            float(record["error_px"])
            for record in records
            if record.get("status") == "matched"
        ]
        hits = sum(1 for record in records if bool(record.get("hit")))
        output[name] = {
            "visible_truth_keypoints": len(records),
            "matched_keypoints": sum(
                1 for record in records if record.get("status") == "matched"
            ),
            "missing_candidate_keypoints": sum(
                1 for record in records if record.get("status") == "missing_candidate"
            ),
            "hits": hits,
            "hit_rate": round_float(hits / max(1, len(records)), config.round_digits),
            "error_distribution": error_distribution(errors, config.round_digits),
        }
    return output


def error_distribution(values: list[float], digits: int = 10) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
            "p95": None,
            "rmse": None,
        }
    ordered = sorted(values)
    count = len(ordered)
    mean = sum(ordered) / count
    median = _quantile(ordered, 0.5)
    p95 = _quantile(ordered, 0.95)
    rmse = math.sqrt(sum(value * value for value in ordered) / count)
    return {
        "count": count,
        "min": round_float(ordered[0], digits),
        "max": round_float(ordered[-1], digits),
        "mean": round_float(mean, digits),
        "median": round_float(median, digits),
        "p95": round_float(p95, digits),
        "rmse": round_float(rmse, digits),
    }


def _quantile(ordered: list[float], quantile: float) -> float:
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction
