"""Stage input metadata and canonical version fingerprints."""

from __future__ import annotations

from typing import Any

from .config import STAGE_NAMES, EncoderConfig, GlobalConfig, QuantizeConfig, ShardConfig
from .encoding import encode_prompt
from .util import stable_hash


STAGE_SCHEMA_VERSION = 1


def encode_meta(shard: ShardConfig, encoder: EncoderConfig) -> dict[str, Any]:
    _, normalized_prompt = encode_prompt(shard.prompt, encoder)
    return {
        "shard_id": shard.id,
        "normalized_prompt": normalized_prompt,
        "encoder": encoder.__dict__,
        "stage_schema_version": STAGE_SCHEMA_VERSION,
    }


def schedule_meta(
    shard: ShardConfig,
    *,
    encode_artifact_sha256: str,
) -> dict[str, Any]:
    return {
        "shard_id": shard.id,
        "encode_artifact_sha256": encode_artifact_sha256,
        "guidance_scale": shard.guidance_scale,
        "steps": shard.steps,
        "stage_schema_version": STAGE_SCHEMA_VERSION,
    }


def sample_meta(
    shard: ShardConfig,
    image: GlobalConfig,
    *,
    schedule_artifact_sha256: str,
    reference_sha256: str | None,
) -> dict[str, Any]:
    return {
        "shard_id": shard.id,
        "schedule_artifact_sha256": schedule_artifact_sha256,
        "seed": shard.seed,
        "reference_sha256": reference_sha256,
        "blend_strength": shard.blend_strength if reference_sha256 is not None else None,
        "shape": [image.image.height, image.image.width, image.image.channels],
        "sampler": {"name": "deterministic-sha256-xof", "version": 1},
        "stage_schema_version": STAGE_SCHEMA_VERSION,
    }


def quantize_meta(
    shard: ShardConfig,
    quantize: QuantizeConfig,
    *,
    sample_artifact_sha256: str,
) -> dict[str, Any]:
    return {
        "shard_id": shard.id,
        "sample_artifact_sha256": sample_artifact_sha256,
        "quantize": quantize.__dict__,
        "stage_schema_version": STAGE_SCHEMA_VERSION,
    }


def input_fingerprint(stage: str, meta: dict[str, Any]) -> str:
    if stage not in STAGE_NAMES:
        raise ValueError(f"unknown stage: {stage}")
    return stable_hash({"stage": stage, "schema_version": STAGE_SCHEMA_VERSION, "input": meta})


def changed_meta_keys(previous: Any, current: Any, prefix: str = "") -> list[str]:
    """List dotted keys whose values differ, bounded to the metadata payload."""

    if isinstance(previous, dict) and isinstance(current, dict):
        changed: list[str] = []
        for key in sorted(set(previous) | set(current)):
            child_prefix = f"{prefix}.{key}" if prefix else key
            if key not in previous or key not in current:
                changed.append(child_prefix)
            else:
                changed.extend(changed_meta_keys(previous[key], current[key], child_prefix))
        return changed
    if isinstance(previous, list) and isinstance(current, list):
        if previous == current:
            return []
        return [prefix or "list"]
    return [] if previous == current else [prefix or "value"]

