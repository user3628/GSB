"""Per-stage version fingerprints and incremental invalidation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest, sha256_file
from .checkpoints import CheckpointScan, CheckpointStore
from .config import Settings
from .errors import WorkbenchError
from .manifest import ManifestView


def appearance_config_fingerprint(settings: Settings) -> str:
    return digest({"kind": "appearance-config-v1", "appearance_config": settings.appearance})


def scoring_config_fingerprint(settings: Settings, sequence: dict[str, Any]) -> str:
    tracking = settings.tracking
    return digest(
        {
            "kind": "candidate-scoring-config-v1",
            "search_window_expansion": sequence["search_window_expansion"],
            "appearance_config": settings.appearance,
            "grid_step_ratio": tracking["grid_step_ratio"],
            "size_deltas_pixels": tracking["size_deltas_pixels"],
            "top_k": tracking["top_k"],
            "gray_similarity_weight": tracking["gray_similarity_weight"],
            "gradient_similarity_weight": tracking["gradient_similarity_weight"],
            "spatial_similarity_weight": tracking["spatial_similarity_weight"],
            "center_prior_weight": tracking["center_prior_weight"],
            "scale_prior_weight": tracking["scale_prior_weight"],
            "center_prior_sigma_ratio": tracking["center_prior_sigma_ratio"],
            "scale_prior_sigma": tracking["scale_prior_sigma"],
            "numeric_epsilon": tracking["numeric_epsilon"],
        }
    )


def trajectory_config_fingerprint(settings: Settings, sequence: dict[str, Any]) -> str:
    tracking = settings.tracking
    return digest(
        {
            "kind": "trajectory-config-v1",
            "appearance_update_threshold": sequence["appearance_update_threshold"],
            "trace_confidence_floor": sequence["trace_confidence_floor"],
            "template_update_alpha": tracking["template_update_alpha"],
            "occlusion_patience": tracking["occlusion_patience"],
            "recovery_margin": tracking["recovery_margin"],
            "recovery_search_expansion_multiplier": tracking["recovery_search_expansion_multiplier"],
            "velocity_smoothing": tracking["velocity_smoothing"],
            "max_velocity_ratio": tracking["max_velocity_ratio"],
            "max_size_velocity_ratio": tracking["max_size_velocity_ratio"],
        }
    )


def current_frame_hashes(manifest: ManifestView, sequence: dict[str, Any]) -> list[str]:
    return [sha256_file(manifest.frame_path(sequence, index)) for index in range(len(sequence["frames"]))]


def sequence_version_fingerprint(
    settings: Settings,
    manifest: ManifestView,
    sequence: dict[str, Any],
    frame_hashes: list[str] | None = None,
) -> str:
    frame_hashes = frame_hashes or current_frame_hashes(manifest, sequence)
    return digest(
        {
            "kind": "sequence-version-v1",
            "sequence_id": sequence["sequence_id"],
            "manifest_sequence_digest": digest(sequence),
            "image_shape": sequence["image_shape"],
            "frame_rate": sequence["frame_rate"],
            "initial_box": sequence["initial_box"],
            "visible_frames": sequence["visible_frames"],
            "sample_count_upper_bound": sequence["sample_count_upper_bound"],
            "search_window_expansion": sequence["search_window_expansion"],
            "appearance_update_threshold": sequence["appearance_update_threshold"],
            "trace_confidence_floor": sequence["trace_confidence_floor"],
            "frames": [
                {
                    "frame_index": index,
                    "sha256": frame_hashes[index],
                    "reference_truth_box": sequence["frames"][index]["box_xywh_norm"],
                }
                for index in range(len(sequence["frames"]))
            ],
            "appearance_config": settings.appearance,
            "appearance_config_fingerprint": appearance_config_fingerprint(settings),
            "scoring_config_fingerprint": scoring_config_fingerprint(settings, sequence),
            "trajectory_config_fingerprint": trajectory_config_fingerprint(settings, sequence),
        }
    )


@dataclass
class PreparedSequence:
    scan: CheckpointScan
    frame_hashes: list[str]
    sequence_version_fingerprint: str
    scoring_config_fingerprint: str
    trajectory_config_fingerprint: str
    invalidation_events: list[dict[str, Any]]
    start_frame: int
    retained_through_frame: int


def _checkpoint_source(record: dict[str, Any]) -> dict[str, Any]:
    sources = record.get("sources")
    return sources if isinstance(sources, dict) else {}


def _apply_invalidation(
    settings: Settings,
    store: CheckpointStore,
    sequence: dict[str, Any],
    *,
    from_frame: int,
    stage: str,
    reason: str,
    current_fingerprint: str | None,
    stored_fingerprint: str | None,
    current_sequence_version: str,
) -> dict[str, Any]:
    sequence_id = str(sequence["sequence_id"])
    frame_count = len(sequence["frames"])
    token = f"{from_frame:04d}-{stage}-{reason}-{digest([sequence_id, from_frame, stage, reason, current_fingerprint, stored_fingerprint])[:16]}"
    moved = store.quarantine_from(sequence_id, from_frame, token=token)
    event = {
        "schema_version": 1,
        "event_type": "invalidation",
        "sequence_id": sequence_id,
        "stage": stage,
        "reason": reason,
        "earliest_affected_frame": from_frame,
        "affected_frame_interval": [from_frame, frame_count - 1],
        "retained_through_frame": from_frame - 1,
        "stored_fingerprint": stored_fingerprint,
        "current_fingerprint": current_fingerprint,
        "current_sequence_version_fingerprint": current_sequence_version,
        "quarantined_checkpoint_paths": moved,
        "event_token": token,
    }
    event["event_digest"] = digest(event)
    event_dir = settings.artifacts_dir / "invalidations" / sequence_id
    atomic_write_json(event_dir / f"{token}.json", event, pretty=True)
    return event


def prepare_sequence(
    settings: Settings,
    manifest: ManifestView,
    sequence: dict[str, Any],
    store: CheckpointStore,
) -> PreparedSequence:
    sequence_id = str(sequence["sequence_id"])
    frame_hashes = current_frame_hashes(manifest, sequence)
    version_fp = sequence_version_fingerprint(settings, manifest, sequence, frame_hashes)
    scoring_fp = scoring_config_fingerprint(settings, sequence)
    trajectory_fp = trajectory_config_fingerprint(settings, sequence)
    appearance_fp = appearance_config_fingerprint(settings)
    events: list[dict[str, Any]] = []
    scan = store.scan(sequence_id)
    start_frame = scan.count

    # A parse error may not carry an ordinal. Any extra file beyond the valid prefix is
    # quarantined so a later run cannot accidentally reuse it.
    if scan.total_checkpoint_files > scan.count:
        event = _apply_invalidation(
            settings,
            store,
            sequence,
            from_frame=start_frame,
            stage="checkpoint",
            reason="checkpoint_corrupt_incomplete_or_gap",
            current_fingerprint=version_fp,
            stored_fingerprint=None,
            current_sequence_version=version_fp,
        )
        events.append(event)
        scan = store.scan(sequence_id)
        start_frame = scan.count

    earliest: tuple[int, str, str, str | None, str | None] | None = None
    for frame_index, record in enumerate(scan.records):
        sources = _checkpoint_source(record)
        current_sha = frame_hashes[frame_index]
        classification: tuple[str, str, str | None, str | None] | None = None
        if sources.get("appearance_config_fingerprint") != appearance_fp:
            classification = (
                "appearance",
                "appearance_config_changed",
                sources.get("appearance_config_fingerprint"),
                appearance_fp,
            )
        elif sources.get("scoring_config_fingerprint") != scoring_fp:
            classification = (
                "candidates",
                "search_window_or_scoring_config_changed",
                sources.get("scoring_config_fingerprint"),
                scoring_fp,
            )
        elif sources.get("trajectory_config_fingerprint") != trajectory_fp:
            classification = (
                "trajectory",
                "update_threshold_or_confidence_floor_changed",
                sources.get("trajectory_config_fingerprint"),
                trajectory_fp,
            )
        elif sources.get("frame_sha256") != current_sha:
            classification = ("appearance", "frame_content_changed", sources.get("frame_sha256"), current_sha)
        elif sources.get("initial_box") != sequence["initial_box"]:
            classification = (
                "appearance",
                "initial_box_changed",
                str(sources.get("initial_box")),
                str(sequence["initial_box"]),
            )
        elif sources.get("reference_truth_box") != sequence["frames"][frame_index]["box_xywh_norm"]:
            classification = (
                "appearance",
                "reference_truth_changed",
                str(sources.get("reference_truth_box")),
                str(sequence["frames"][frame_index]["box_xywh_norm"]),
            )
        if classification is not None:
            earliest = (frame_index, *classification)
            break

    if earliest is not None:
        from_frame, stage, reason, stored_fp, current_fp = earliest
        event = _apply_invalidation(
            settings,
            store,
            sequence,
            from_frame=from_frame,
            stage=stage,
            reason=reason,
            current_fingerprint=current_fp,
            stored_fingerprint=stored_fp,
            current_sequence_version=version_fp,
        )
        events.append(event)
        scan = store.scan(sequence_id)
        start_frame = scan.count
    return PreparedSequence(
        scan=scan,
        frame_hashes=frame_hashes,
        sequence_version_fingerprint=version_fp,
        scoring_config_fingerprint=scoring_fp,
        trajectory_config_fingerprint=trajectory_fp,
        invalidation_events=events,
        start_frame=start_frame,
        retained_through_frame=start_frame - 1,
    )


def list_invalidation_events(settings: Settings, sequence_id: str | None = None) -> list[dict[str, Any]]:
    root = settings.artifacts_dir / "invalidations"
    if sequence_id is not None:
        root = root / sequence_id
    if not root.exists():
        return []
    events: list[dict[str, Any]] = []
    for path in sorted(root.glob("**/*.json")):
        try:
            events.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            continue
    events.sort(key=lambda e: (e.get("sequence_id", ""), e.get("earliest_affected_frame", -1), e.get("event_token", "")))
    return events


def stage_fingerprint_summary(
    settings: Settings,
    manifest: ManifestView,
    sequence: dict[str, Any],
    store: CheckpointStore,
    *,
    frame_hashes: list[str] | None = None,
) -> dict[str, Any]:
    sequence_id = str(sequence["sequence_id"])
    frame_hashes = frame_hashes or current_frame_hashes(manifest, sequence)
    scan = store.scan(sequence_id)
    appearance_records: list[dict[str, Any]] = []
    from .appearance import appearance_source_fingerprint, load_appearance_stage

    artifact = load_appearance_stage(settings, sequence_id)
    if artifact is not None:
        appearance_records = artifact.get("records", [])
    appearance_complete = len(appearance_records) == len(sequence["frames"]) and all(
        record.get("frame_sha256") == frame_hashes[i]
        and record.get("source_fingerprint")
        == appearance_source_fingerprint(settings, sequence, i, frame_hashes[i])
        for i, record in enumerate(appearance_records)
    )
    current_version = sequence_version_fingerprint(settings, manifest, sequence, frame_hashes)
    records = scan.records
    candidates_fp = digest([{"candidate": r.get("stage_fingerprints", {}).get("candidates")} for r in records])
    trajectory_fp = digest([{"trajectory": r.get("stage_fingerprints", {}).get("trajectory")} for r in records])
    return {
        "sequence_id": sequence_id,
        "sequence_version_fingerprint": current_version,
        "appearance_config_fingerprint": appearance_config_fingerprint(settings),
        "scoring_config_fingerprint": scoring_config_fingerprint(settings, sequence),
        "trajectory_config_fingerprint": trajectory_config_fingerprint(settings, sequence),
        "stages": {
            "appearance": {
                "ready": appearance_complete,
                "frames_ready": len(appearance_records),
                "fingerprint": artifact.get("appearance_fingerprint") if artifact else None,
            },
            "candidates": {
                "ready": scan.count == len(sequence["frames"]),
                "frames_ready": scan.count,
                "fingerprint": candidates_fp,
            },
            "trajectory": {
                "ready": scan.count == len(sequence["frames"]),
                "frames_ready": scan.count,
                "fingerprint": trajectory_fp,
            },
        },
        "checkpoint": {
            "valid_frames": scan.count,
            "next_frame_index": scan.next_frame_index,
            "chain_digest": scan.chain_digest,
            "issues": scan.issues,
        },
    }
