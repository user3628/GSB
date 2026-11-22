"""Stage 1: normalized polygon decoding, area, bbox, and validity diagnostics."""

from __future__ import annotations

import math
from typing import Any, Callable

from .canonical import digest_obj, quantize
from .config import decode_config
from .polygons import (
    clean_vertices,
    has_self_intersection,
    polygon_area,
    polygon_bbox,
)
from .shards import shard_content_digest


AREA_EPSILON = 1e-9


def _parse_normalized_polygon(raw: Any) -> tuple[list[tuple[float, float]], list[int]]:
    vertices: list[tuple[float, float]] = []
    nonfinite: list[int] = []
    if not isinstance(raw, list):
        return vertices, nonfinite
    for index, point in enumerate(raw):
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            nonfinite.append(index)
            continue
        try:
            x = float(point[0])
            y = float(point[1])
        except (TypeError, ValueError):
            nonfinite.append(index)
            continue
        if not math.isfinite(x) or not math.isfinite(y):
            nonfinite.append(index)
            continue
        vertices.append((x, y))
    return vertices, nonfinite


def _decode_instance(
    *,
    instance_id: str,
    class_name: str,
    source_operator: str,
    confidence: float | None,
    normalized: Any,
    width: int,
    height: int,
    policy: str,
    precision: int,
    kind: str,
) -> dict[str, Any]:
    normalized_vertices, nonfinite_indexes = _parse_normalized_polygon(normalized)
    pixel_vertices: list[tuple[float, float]] = []
    out_of_bounds_indexes: list[int] = []
    for index, (nx, ny) in enumerate(normalized_vertices):
        px = nx * width
        py = ny * height
        if px < 0.0 or px > float(width) or py < 0.0 or py > float(height):
            out_of_bounds_indexes.append(index)
        pixel_vertices.append((px, py))

    diagnostics: list[dict[str, Any]] = []
    reasons: list[str] = []
    if nonfinite_indexes:
        reasons.append("NONFINITE_COORDINATE")
        diagnostics.append(
            {
                "code": "NONFINITE_COORDINATE",
                "vertex_indexes": nonfinite_indexes,
                "action": "reject",
            }
        )

    if out_of_bounds_indexes:
        diagnostics.append(
            {
                "code": "OUT_OF_BOUNDS",
                "vertex_indexes": out_of_bounds_indexes,
                "action": "clamp" if policy == "clamp" else "reject",
            }
        )
        if policy == "reject":
            reasons.append("OUT_OF_BOUNDS")
        else:
            pixel_vertices = [
                (min(max(x, 0.0), float(width)), min(max(y, 0.0), float(height)))
                for x, y in pixel_vertices
            ]

    rounded_vertices = [
        (quantize(x, precision), quantize(y, precision)) for x, y in pixel_vertices
    ]
    closed_vertices = clean_vertices(rounded_vertices)

    if len(closed_vertices) < 3:
        reasons.append("TOO_FEW_VERTICES")
        diagnostics.append(
            {
                "code": "TOO_FEW_VERTICES",
                "vertex_count": len(closed_vertices),
                "minimum": 3,
            }
        )
        area_value = 0.0
    else:
        area_value = quantize(polygon_area(closed_vertices), precision)

    self_intersects = has_self_intersection(closed_vertices) if len(closed_vertices) >= 3 else False
    if self_intersects:
        reasons.append("SELF_INTERSECTION")
        diagnostics.append({"code": "SELF_INTERSECTION"})

    if len(closed_vertices) >= 3 and area_value <= AREA_EPSILON:
        reasons.append("ZERO_AREA")
        diagnostics.append({"code": "ZERO_AREA", "area": area_value})

    bbox = polygon_bbox(closed_vertices) if closed_vertices else None
    if bbox is not None:
        bbox = [quantize(value, precision) for value in bbox]

    ordered_reasons = [
        code
        for code in ("NONFINITE_COORDINATE", "OUT_OF_BOUNDS", "TOO_FEW_VERTICES", "SELF_INTERSECTION", "ZERO_AREA")
        if code in reasons
    ]
    result: dict[str, Any] = {
        "instance_id": instance_id,
        "class_name": class_name,
        "source_operator": source_operator,
        "vertices_px": [[x, y] for x, y in rounded_vertices],
        "area": area_value,
        "bbox": bbox,
        "valid": not ordered_reasons,
        "reason_codes": ordered_reasons,
        "diagnostics": diagnostics,
    }
    if confidence is not None:
        result["confidence"] = quantize(float(confidence), precision)
    if kind == "ground_truth":
        result["source_operator"] = "ground_truth"
    return result


def decode_shard(
    shard: dict[str, Any],
    config: dict[str, Any],
    *,
    stop_requested: Callable[[], bool] | None = None,
    sample_boundary: Callable[[], None] | None = None,
) -> dict[str, Any] | None:
    decode_options = decode_config(config)
    policy = decode_options["out_of_bounds_policy"]
    precision = decode_options["precision"]
    samples: list[dict[str, Any]] = []
    all_classes: set[str] = set()
    all_sources: set[str] = set()
    diagnostic_counts: dict[str, int] = {}
    gt_total = 0
    gt_valid = 0
    pred_total = 0
    pred_valid = 0

    for sample in sorted(shard["samples"], key=lambda item: item["image_id"]):
        if stop_requested is not None and stop_requested():
            return None
        width = int(sample["width"])
        height = int(sample["height"])
        image_id = sample["image_id"]
        ground_truth: list[dict[str, Any]] = []
        predictions: list[dict[str, Any]] = []
        sample_classes: set[str] = set()

        for class_name in sorted(sample["ground_truth"].keys()):
            polygons = sample["ground_truth"][class_name]
            sample_classes.add(class_name)
            all_classes.add(class_name)
            for instance_index, polygon in enumerate(polygons):
                instance_id = f"gt:{image_id}:{class_name}:{instance_index:04d}"
                decoded = _decode_instance(
                    instance_id=instance_id,
                    class_name=class_name,
                    source_operator="ground_truth",
                    confidence=None,
                    normalized=polygon,
                    width=width,
                    height=height,
                    policy=policy,
                    precision=precision,
                    kind="ground_truth",
                )
                ground_truth.append(decoded)
                gt_total += 1
                gt_valid += int(decoded["valid"])
                for diagnostic in decoded["diagnostics"]:
                    key = f"ground_truth:{diagnostic['code']}"
                    diagnostic_counts[key] = diagnostic_counts.get(key, 0) + 1

        used_prediction_ids: set[str] = set()
        for prediction_index, prediction in enumerate(sample["predictions"]):
            class_name = prediction["class_name"]
            source_operator = prediction.get("source_operator", "unknown")
            prediction_id = prediction.get("prediction_id") or f"pred:{image_id}:{prediction_index:04d}"
            if prediction_id in used_prediction_ids:
                prediction_id = f"{prediction_id}:dup{prediction_index:04d}"
            used_prediction_ids.add(prediction_id)
            sample_classes.add(class_name)
            all_classes.add(class_name)
            all_sources.add(source_operator)
            decoded = _decode_instance(
                instance_id=prediction_id,
                class_name=class_name,
                source_operator=source_operator,
                confidence=prediction["confidence"],
                normalized=prediction["polygon"],
                width=width,
                height=height,
                policy=policy,
                precision=precision,
                kind="prediction",
            )
            predictions.append(decoded)
            pred_total += 1
            pred_valid += int(decoded["valid"])
            for diagnostic in decoded["diagnostics"]:
                key = f"prediction:{diagnostic['code']}"
                diagnostic_counts[key] = diagnostic_counts.get(key, 0) + 1

        predictions.sort(key=lambda item: item["instance_id"])
        samples.append(
            {
                "image_id": image_id,
                "width": width,
                "height": height,
                "class_names": sorted(sample_classes),
                "ground_truth": ground_truth,
                "predictions": predictions,
                "counts": {
                    "ground_truth": len(ground_truth),
                    "ground_truth_valid": sum(1 for item in ground_truth if item["valid"]),
                    "predictions": len(predictions),
                    "predictions_valid": sum(1 for item in predictions if item["valid"]),
                },
            }
        )
        if sample_boundary is not None:
            sample_boundary()

    content_digest = shard_content_digest(shard)
    artifact: dict[str, Any] = {
        "schema_version": 1,
        "stage": "decode",
        "shard_id": shard["shard_id"],
        "shard_index": shard["shard_index"],
        "shard_content_digest": content_digest,
        "class_set": sorted(all_classes),
        "source_operators": sorted(all_sources),
        "samples": samples,
        "counts": {
            "samples": len(samples),
            "ground_truth": gt_total,
            "ground_truth_valid": gt_valid,
            "ground_truth_invalid": gt_total - gt_valid,
            "predictions": pred_total,
            "predictions_valid": pred_valid,
            "predictions_invalid": pred_total - pred_valid,
            "diagnostic_codes": dict(sorted(diagnostic_counts.items())),
        },
    }
    artifact["artifact_digest"] = digest_obj(artifact)
    return artifact

