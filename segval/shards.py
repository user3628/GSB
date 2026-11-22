"""Local shard loading, validation, import, and content fingerprints."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, canonical_bytes, digest_bytes, locked, read_json
from .errors import invalid_shard


def _require(condition: bool, message: str, details: dict[str, Any] | None = None) -> None:
    if not condition:
        raise invalid_shard(message, details)


def validate_shard(shard: Any) -> dict[str, Any]:
    _require(isinstance(shard, dict), "Shard must be a JSON object.")
    shard_id = shard.get("shard_id")
    _require(isinstance(shard_id, str) and bool(shard_id), "shard_id must be a non-empty string.")
    shard_index = shard.get("shard_index")
    _require(
        isinstance(shard_index, int) and not isinstance(shard_index, bool) and shard_index >= 0,
        "shard_index must be a non-negative integer.",
    )
    samples = shard.get("samples")
    _require(isinstance(samples, list) and samples, "samples must be a non-empty array.")

    seen_images: set[str] = set()
    for sample_index, sample in enumerate(samples):
        path = f"samples[{sample_index}]"
        _require(isinstance(sample, dict), f"{path} must be an object.")
        image_id = sample.get("image_id")
        _require(isinstance(image_id, str) and bool(image_id), f"{path}.image_id must be a non-empty string.")
        _require(image_id not in seen_images, f"Duplicate image_id {image_id!r}.", {"image_id": image_id})
        seen_images.add(image_id)
        width = sample.get("width")
        height = sample.get("height")
        _require(
            isinstance(width, int) and not isinstance(width, bool) and width > 0,
            f"{path}.width must be a positive integer.",
        )
        _require(
            isinstance(height, int) and not isinstance(height, bool) and height > 0,
            f"{path}.height must be a positive integer.",
        )
        gt = sample.get("ground_truth")
        _require(isinstance(gt, dict), f"{path}.ground_truth must be an object grouped by class.")
        for class_name, polygons in gt.items():
            _require(isinstance(class_name, str) and bool(class_name), f"{path}.ground_truth class names must be strings.")
            _require(isinstance(polygons, list), f"{path}.ground_truth[{class_name!r}] must be an array.")
            for polygon in polygons:
                _require(isinstance(polygon, list), f"{path}.ground_truth[{class_name!r}] polygons must be arrays.")
        predictions = sample.get("predictions")
        _require(isinstance(predictions, list), f"{path}.predictions must be an array.")
        seen_prediction_ids: set[str] = set()
        for prediction_index, prediction in enumerate(predictions):
            pred_path = f"{path}.predictions[{prediction_index}]"
            _require(isinstance(prediction, dict), f"{pred_path} must be an object.")
            class_name = prediction.get("class_name")
            _require(isinstance(class_name, str) and bool(class_name), f"{pred_path}.class_name must be a non-empty string.")
            confidence = prediction.get("confidence")
            _require(
                isinstance(confidence, (int, float))
                and not isinstance(confidence, bool)
                and math.isfinite(float(confidence)),
                f"{pred_path}.confidence must be numeric.",
            )
            _require(isinstance(prediction.get("polygon"), list), f"{pred_path}.polygon must be an array.")
            source = prediction.get("source_operator", "unknown")
            _require(isinstance(source, str) and bool(source), f"{pred_path}.source_operator must be a non-empty string.")
            prediction_id = prediction.get("prediction_id")
            if prediction_id is not None:
                _require(
                    isinstance(prediction_id, str) and bool(prediction_id),
                    f"{pred_path}.prediction_id must be a non-empty string when supplied.",
                )
                _require(
                    prediction_id not in seen_prediction_ids,
                    f"Duplicate prediction_id {prediction_id!r}.",
                    {"image_id": image_id, "prediction_id": prediction_id},
                )
                seen_prediction_ids.add(prediction_id)
    return shard


def shard_content_bytes(shard: dict[str, Any]) -> bytes:
    return canonical_bytes(shard)


def shard_content_digest(shard: dict[str, Any]) -> str:
    return digest_bytes(shard_content_bytes(shard))


def shard_path(shards_dir: Path, shard_index: int) -> Path:
    return shards_dir / f"shard-{shard_index:04d}.json"


def load_shard_file(path: Path) -> dict[str, Any]:
    return validate_shard(read_json(path))


def list_shards(workspace: Path) -> list[dict[str, Any]]:
    shards_dir = workspace / "shards"
    if not shards_dir.exists():
        return []
    shards = [load_shard_file(path) for path in sorted(shards_dir.glob("shard-*.json"))]
    shards.sort(key=lambda item: (item["shard_index"], item["shard_id"]))
    indexes: dict[int, str] = {}
    for shard in shards:
        previous = indexes.get(shard["shard_index"])
        if previous is not None and previous != shard["shard_id"]:
            raise invalid_shard(
                "Multiple shard ids share one shard_index.",
                {"shard_index": shard["shard_index"], "shard_ids": [previous, shard["shard_id"]]},
            )
        indexes[shard["shard_index"]] = shard["shard_id"]
    return shards


def import_shard(workspace: Path, shard: dict[str, Any]) -> dict[str, Any]:
    validate_shard(shard)
    shards_dir = workspace / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)
    with locked(workspace / ".checkpoint.lock"), locked(workspace / ".shard.lock"):
        existing = list_shards(workspace)
        for item in existing:
            if item["shard_index"] == shard["shard_index"] and item["shard_id"] != shard["shard_id"]:
                raise invalid_shard(
                    "A different shard already owns this shard_index.",
                    {
                        "shard_index": shard["shard_index"],
                        "existing_shard_id": item["shard_id"],
                        "incoming_shard_id": shard["shard_id"],
                    },
                )
            if item["shard_id"] == shard["shard_id"] and item["shard_index"] != shard["shard_index"]:
                raise invalid_shard(
                    "A shard with this id already exists at another shard_index.",
                    {
                        "shard_id": shard["shard_id"],
                        "existing_shard_index": item["shard_index"],
                        "incoming_shard_index": shard["shard_index"],
                    },
                )
        destination = shard_path(shards_dir, shard["shard_index"])
        atomic_write_json(destination, shard)
    return {
        "shard_id": shard["shard_id"],
        "shard_index": shard["shard_index"],
        "path": str(destination),
        "content_digest": shard_content_digest(shard),
        "sample_count": len(shard["samples"]),
        "prediction_count": sum(len(sample["predictions"]) for sample in shard["samples"]),
    }

