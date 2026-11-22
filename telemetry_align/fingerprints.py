"""Content fingerprints shared by status queries and the incremental engine."""

from __future__ import annotations

from typing import Any

from .canonical import digest_obj, fingerprint


STAGES = ("normalize", "resample", "fusion", "summary")
STAGE_RANK = {stage: index + 1 for index, stage in enumerate(STAGES)}
STAGE_DEPENDENCIES = {
    "normalize": [],
    "resample": ["normalize"],
    "fusion": ["normalize", "resample"],
    "summary": ["fusion"],
}


def grid_fingerprint(grid: dict[str, Any]) -> str:
    return fingerprint("grid-v1", grid)


def normalize_source_fingerprint(
    raw_digest: str, normalize_source_config: dict[str, Any]
) -> str:
    return fingerprint(
        "normalize-source-v1",
        {"raw_digest": raw_digest, "config": normalize_source_config},
    )


def normalize_stage_fingerprint(
    grid_definition: dict[str, Any], source_fingerprints: dict[str, str]
) -> str:
    return fingerprint(
        "normalize-stage-v1",
        {
            "grid": grid_definition,
            "sources": {
                source_id: source_fingerprints[source_id]
                for source_id in sorted(source_fingerprints)
            },
        },
    )


def resample_source_fingerprint(
    normalize_source_fp: str,
    grid_fp: str,
    resample_source_config: dict[str, Any],
) -> str:
    return fingerprint(
        "resample-source-v1",
        {
            "normalize": normalize_source_fp,
            "grid": grid_fp,
            "config": resample_source_config,
        },
    )


def resample_stage_fingerprint(
    grid_fp: str, source_fingerprints: dict[str, str]
) -> str:
    return fingerprint(
        "resample-stage-v1",
        {
            "grid": grid_fp,
            "sources": {
                source_id: source_fingerprints[source_id]
                for source_id in sorted(source_fingerprints)
            },
        },
    )


def fusion_stage_fingerprint(
    resample_fp: str, fusion_config: dict[str, Any]
) -> str:
    return fingerprint(
        "fusion-stage-v1",
        {"resample": resample_fp, "config": fusion_config},
    )


def summary_stage_fingerprint(
    fusion_fp: str, summary_config: dict[str, Any]
) -> str:
    return fingerprint(
        "summary-stage-v1",
        {"fusion": fusion_fp, "config": summary_config},
    )


def overall_fingerprint(
    stage_fingerprints: dict[str, str], semantic_config_fingerprint: str
) -> str:
    return fingerprint(
        "overall-v1",
        {
            "stages": {
                stage: stage_fingerprints[stage] for stage in STAGES
            },
            "config": semantic_config_fingerprint,
        },
    )


def points_digest(points: list[dict[str, Any]]) -> str:
    return digest_obj(points)

