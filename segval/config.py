"""Workspace configuration and validation."""

from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any

from .canonical import digest_obj, read_json, atomic_write_json
from .errors import WorkbenchError, invalid_budget, invalid_iou_threshold


STAGE_NAMES = ("decode", "match", "aggregate")
ALL_STAGES = (*STAGE_NAMES, "schedule")


DEFAULT_CONFIG: dict[str, Any] = {
    "config_version": "1",
    "decode": {
        "out_of_bounds_policy": "clamp",
        "precision": 9,
    },
    "matching": {
        "iou_threshold": 0.5,
        "strategy": "greedy_global_iou",
        "duplicate_confidence_threshold": 0.5,
    },
    "aggregation": {
        "schema": "shard-class-source-v1",
    },
    "budget": {
        "max_evaluations": 200000,
        "per_worker_quota": 80000,
        "max_workers": 2,
    },
}


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise WorkbenchError("E_INVALID_CONFIG", "Configuration must be a JSON object.")
    merged = deep_merge(DEFAULT_CONFIG, config)

    decode = merged.get("decode")
    if not isinstance(decode, dict):
        raise WorkbenchError("E_INVALID_CONFIG", "decode must be an object.")
    policy = decode.get("out_of_bounds_policy")
    if policy not in {"clamp", "reject"}:
        raise WorkbenchError(
            "E_INVALID_OOB_POLICY",
            "decode.out_of_bounds_policy must be 'clamp' or 'reject'.",
            {"value": policy},
        )
    precision = decode.get("precision")
    if not isinstance(precision, int) or isinstance(precision, bool) or not 0 <= precision <= 12:
        raise WorkbenchError(
            "E_INVALID_CONFIG",
            "decode.precision must be an integer in [0, 12].",
            {"value": precision},
        )

    matching = merged.get("matching")
    if not isinstance(matching, dict):
        raise WorkbenchError("E_INVALID_CONFIG", "matching must be an object.")
    threshold = matching.get("iou_threshold")
    if not _finite_number(threshold) or not 0.0 <= float(threshold) <= 1.0:
        raise invalid_iou_threshold(threshold)
    duplicate_threshold = matching.get("duplicate_confidence_threshold")
    if not _finite_number(duplicate_threshold) or not 0.0 <= float(duplicate_threshold) <= 1.0:
        raise WorkbenchError(
            "E_INVALID_CONFIDENCE_THRESHOLD",
            "matching.duplicate_confidence_threshold must be in [0, 1].",
            {"value": duplicate_threshold},
        )
    strategy = matching.get("strategy")
    if strategy not in {"greedy_global_iou", "greedy_global_iou_confidence_tiebreak"}:
        raise WorkbenchError(
            "E_INVALID_MATCH_STRATEGY",
            "Unknown deterministic matching strategy.",
            {"value": strategy},
        )

    aggregation = merged.get("aggregation")
    if not isinstance(aggregation, dict) or aggregation.get("schema") != "shard-class-source-v1":
        raise WorkbenchError(
            "E_INVALID_CONFIG",
            "aggregation.schema must be 'shard-class-source-v1'.",
        )

    budget = merged.get("budget")
    if not isinstance(budget, dict):
        raise invalid_budget("budget must be an object.")
    for key in ("max_evaluations", "per_worker_quota"):
        value = budget.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise invalid_budget(
                f"budget.{key} must be a non-negative integer.",
                {"value": value},
            )
    max_workers = budget.get("max_workers")
    if not isinstance(max_workers, int) or isinstance(max_workers, bool) or not 1 <= max_workers <= 64:
        raise invalid_budget(
            "budget.max_workers must be an integer in [1, 64].",
            {"value": max_workers},
        )

    if not isinstance(merged.get("config_version"), str) or not merged["config_version"]:
        raise WorkbenchError("E_INVALID_CONFIG", "config_version must be a non-empty string.")
    return merged


def decode_config(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "out_of_bounds_policy": config["decode"]["out_of_bounds_policy"],
        "precision": config["decode"]["precision"],
    }


def matching_config(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "iou_threshold": float(config["matching"]["iou_threshold"]),
        "strategy": config["matching"]["strategy"],
        "duplicate_confidence_threshold": float(config["matching"]["duplicate_confidence_threshold"]),
    }


def aggregation_config(config: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(config["aggregation"])


def budget_config(config: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(config["budget"])


def stage_config(config: dict[str, Any], stage: str) -> dict[str, Any]:
    if stage == "decode":
        return decode_config(config)
    if stage == "match":
        return matching_config(config)
    if stage == "aggregate":
        return aggregation_config(config)
    if stage == "schedule":
        return budget_config(config)
    raise WorkbenchError("E_INVALID_STAGE", f"Unknown stage: {stage}", {"stage": stage}, 400)


def config_fingerprint(config: dict[str, Any]) -> str:
    return digest_obj({"schema": "segval-config-v1", "config": config})


def decode_stage_input(config: dict[str, Any], shard_content_digest: str) -> dict[str, Any]:
    return {
        "schema": "decode-input-v1",
        "shard_content_digest": shard_content_digest,
        "config": decode_config(config),
    }


def match_stage_input(
    config: dict[str, Any],
    decode_artifact_digest: str,
    class_set: list[str],
) -> dict[str, Any]:
    return {
        "schema": "match-input-v1",
        "decode_artifact_digest": decode_artifact_digest,
        "class_set": sorted(class_set),
        "config": matching_config(config),
    }


def aggregate_stage_input(config: dict[str, Any], match_artifact_digest: str) -> dict[str, Any]:
    return {
        "schema": "aggregate-input-v1",
        "match_artifact_digest": match_artifact_digest,
        "config": aggregation_config(config),
    }


def schedule_stage_input(
    config: dict[str, Any],
    accepted_shards: list[dict[str, Any]],
    unprocessed: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema": "schedule-input-v1",
        "budget": budget_config(config),
        "accepted": [
            {
                "shard_id": item["shard_id"],
                "shard_index": item["shard_index"],
                "estimate": item["estimated_evaluations"],
                "aggregate_digest": item.get("aggregate_digest"),
            }
            for item in accepted_shards
        ],
        "unprocessed": [
            {
                "shard_id": item["shard_id"],
                "shard_index": item["shard_index"],
                "code": item["code"],
            }
            for item in unprocessed
        ],
    }


def load_config(path: Path) -> dict[str, Any]:
    return validate_config(read_json(path))


def save_config(path: Path, config: dict[str, Any]) -> dict[str, Any]:
    validated = validate_config(config)
    atomic_write_json(path, validated)
    return validated

