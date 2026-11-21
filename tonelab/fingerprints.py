"""Stage content fingerprints and earliest-invalidation planning."""

from __future__ import annotations

from typing import Any

from .config import Config
from .models import Project, Shard
from .util import canonical_json_bytes, sha256_bytes


STAGES = ("feature", "model", "quantize", "export")
DOWNSTREAM = {
    "feature": ("model", "quantize", "export"),
    "model": ("quantize", "export"),
    "quantize": ("export",),
    "export": (),
}


def shard_version_fingerprint(shard: Shard) -> str:
    payload = {
        "shard_id": shard.shard_id,
        "structure_version": shard.structure_version,
        "source_structure_version": shard.source_structure_version,
        "timbre_id": shard.timbre_id,
        "sample_count": shard.sample_count,
        "sample_rate": shard.sample_rate,
        "channels": shard.channels,
        "bit_depth": shard.bit_depth,
        "reference_f0_mean": shard.reference_f0_mean,
        "pcm_sha256": shard.pcm_sha256,
        "pitch_curve": shard.pitch_curve,
        "units": [unit.to_dict() for unit in shard.units],
    }
    return sha256_bytes(canonical_json_bytes(payload))


def structure_version_fingerprint(project: Project) -> str:
    return sha256_bytes(
        canonical_json_bytes(
            [
                {
                    "shard_id": shard.shard_id,
                    "structure_version": shard.structure_version,
                    "source_structure_version": shard.source_structure_version,
                }
                for shard in project.shards
            ]
        )
    )


def stage_inputs(stage: str, shard: Shard, config: Config, upstream: str | None) -> dict[str, Any]:
    if stage == "feature":
        return {
            "pcm_sha256": shard.pcm_sha256,
            "sample_count": shard.sample_count,
            "sample_rate": shard.sample_rate,
            "channels": shard.channels,
            "bit_depth": shard.bit_depth,
            "timbre_id": shard.timbre_id,
            "reference_f0_mean": shard.reference_f0_mean,
            "config": config.stage_config("feature"),
        }
    if stage == "model":
        return {
            "upstream": upstream,
            "pitch_curve": shard.pitch_curve,
            "units": [unit.to_dict() for unit in shard.units],
            "reference_f0_mean": shard.reference_f0_mean,
            "structure_version": shard.structure_version,
            "source_structure_version": shard.source_structure_version,
            "config": config.stage_config("model"),
        }
    if stage == "quantize":
        return {
            "upstream": upstream,
            "structure_version": shard.structure_version,
            "source_structure_version": shard.source_structure_version,
            "config": config.stage_config("quantize"),
        }
    if stage == "export":
        return {
            "upstream": upstream,
            "structure_version": shard.structure_version,
            "source_structure_version": shard.source_structure_version,
            "sample_rate": shard.sample_rate,
            "channels": shard.channels,
            "bit_depth": shard.bit_depth,
            "config": config.stage_config("export"),
        }
    raise ValueError(f"unknown stage: {stage}")


def stage_fingerprints(shard: Shard, config: Config) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    upstream: str | None = None
    for stage in STAGES:
        inputs = stage_inputs(stage, shard, config, upstream)
        digest = sha256_bytes(canonical_json_bytes(inputs))
        result[stage] = {
            "stage": stage,
            "fingerprint": digest,
            "upstream_fingerprint": upstream,
            "inputs": inputs,
        }
        upstream = digest
    return result


def classify_changes(old: dict[str, Any] | None, new: dict[str, Any], stage: str) -> list[str]:
    if old is None:
        return ["no-prior-stage-record"]
    old_inputs = old.get("inputs", {})
    new_inputs = new.get("inputs", {})
    reasons: list[str] = []
    for key in sorted(set(old_inputs) | set(new_inputs)):
        if old_inputs.get(key) != new_inputs.get(key):
            reasons.append(f"{stage}.{key}")
    if not reasons:
        reasons.append(f"{stage}.artifact-digest")
    return reasons


def plan_invalidation(
    old_fingerprints: dict[str, dict[str, Any]],
    new_fingerprints: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    earliest: str | None = None
    reasons: list[str] = []
    for stage in STAGES:
        old = old_fingerprints.get(stage)
        new = new_fingerprints[stage]
        if old is None or old.get("fingerprint") != new["fingerprint"]:
            earliest = stage
            reasons = classify_changes(old, new, stage)
            break
    invalid = [] if earliest is None else [earliest, *DOWNSTREAM[earliest]]
    return {
        "earliest_invalid_stage": earliest,
        "invalid_stages": invalid,
        "reasons": reasons,
        "fingerprints": {stage: new_fingerprints[stage]["fingerprint"] for stage in STAGES},
    }


def project_version_fingerprint(
    project: Project,
    shard_fingerprints: dict[str, dict[str, dict[str, Any]]],
    config: Config,
) -> str:
    payload = {
        "config_version": config.version,
        "structure_version_fingerprint": structure_version_fingerprint(project),
        "shards": {
            shard_id: {
                stage: shard_fingerprints[shard_id][stage]["fingerprint"]
                for stage in STAGES
            }
            for shard_id in sorted(shard_fingerprints)
        },
    }
    return sha256_bytes(canonical_json_bytes(payload))

