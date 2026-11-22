"""Stage 4: deterministic, exportable alignment summary."""

from __future__ import annotations

from typing import Any

from .fingerprints import points_digest


def build_summary(
    *,
    config: Any,
    grid: dict[str, Any],
    fusion: dict[str, Any],
    fusion_artifact_digest: str,
    cleaning_reports: dict[str, dict[str, Any]],
    source_fingerprints: dict[str, Any],
    stage_fingerprints: dict[str, str],
) -> dict[str, Any]:
    include_points = bool(config.effective["export"]["include_points"])
    summary: dict[str, Any] = {
        "schema_version": 1,
        "title": config.effective["export"]["title"],
        "workspace_id": config.workspace_id,
        "config": {
            "source_schema_version": config.schema_version,
            "effective_schema_version": config.effective_schema_version,
            "semantic_fingerprint": config.semantic_fingerprint,
            "degraded": config.degraded,
            "tolerance_ms": config.effective["tolerance_ms"],
            "grid": config.effective["grid"],
            "interpolation": config.effective["interpolation"],
            "resample": config.effective["resample"],
            "fusion": config.effective["fusion"],
            "provenance": config.provenance,
        },
        "grid": {
            "fingerprint": grid["fingerprint"],
            "step_ms": grid["step_ms"],
            "start_ms": grid["start_ms"],
            "end_ms": grid["end_ms"],
            "count": grid["count"],
        },
        "labels": fusion["labels"],
        "stage_fingerprints": {
            stage: stage_fingerprints[stage]
            for stage in ("normalize", "resample", "fusion", "summary")
        },
        "source_artifacts": source_fingerprints,
        "fusion_result_digest": fusion_artifact_digest,
        "points_digest": points_digest(fusion["points"]),
        "counts": fusion["summary"],
        "cleaning_reports": {
            source_id: cleaning_reports[source_id]
            for source_id in sorted(cleaning_reports)
        },
        "conflicts": fusion["conflicts"],
        "gaps": fusion["gaps"],
    }
    if include_points:
        summary["alignment"] = fusion["points"]
    return summary

