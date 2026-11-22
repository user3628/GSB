"""Repeatable end-to-end acceptance demonstration."""

from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path
from typing import Any

import yaml

from .config import load_config
from .generate import generate_dataset
from .pipeline import SeparationPipeline
from .util import atomic_write_json, digest_json, read_json
from .workspace import Workspace


def _build_reference_workspace(workspace: Workspace) -> Workspace:
    reference_root = workspace.state_root / "demo_reference"
    if reference_root.exists():
        shutil.rmtree(reference_root)
    reference = Workspace(workspace.project_root, state_root=reference_root)
    reference.import_dataset(workspace.samples_dir / "manifest.json", workspace.project_root / "configs" / "default.yaml")
    return reference


def run_demo(workspace: Workspace, *, output_path: str | Path | None = None, reset: bool = True) -> dict[str, Any]:
    if reset:
        workspace.reset_state()
    manifest_path = generate_dataset(workspace.samples_dir, seed=20261123)
    config_path = workspace.project_root / "configs" / "default.yaml"
    imported = workspace.import_dataset(manifest_path, config_path)
    config = load_config(config_path)
    pipeline = SeparationPipeline(workspace)

    stopped_build = pipeline.build_all(stop_after_units=11)
    workspace.clear_stop()
    resumed = pipeline.resume()
    resume_verification = pipeline.verify_slice("slice-001")
    resumed_digests = {
        item["slice_id"]: item.get("result_digest")
        for item in resumed["results"]
        if item.get("status") == "complete"
    }
    resume_checkpoint_status = pipeline.status(slice_id="slice-001")["checkpoints"]

    reference_workspace = _build_reference_workspace(workspace)
    reference_pipeline = SeparationPipeline(reference_workspace)
    reference_result = reference_pipeline.build_slice("slice-001")
    resume_matches_full = resumed_digests.get("slice-001") == reference_result["result_digest"]

    baseline_status = pipeline.status()
    baseline_digests = {
        item["slice_id"]: item.get("result_digest")
        for item in baseline_status["slices"]
        if item.get("complete")
    }
    baseline_details = {
        item["slice_id"]: {
            "result_digest": item.get("result_digest"),
            "mask_digest": item.get("mask_digest"),
            "round_changes_digest": item.get("round_changes_digest"),
            "source_sample_counts": next(
                (
                    result.get("source_sample_counts")
                    for result in resumed["results"]
                    if result.get("slice_id") == item["slice_id"]
                ),
                None,
            ),
        }
        for item in baseline_status["slices"]
    }

    source = workspace.effective_manifest().get("slice-001").sources[0]
    new_span = max(4.0, source.f0_max_hz - source.f0_min_hz)
    incremental = pipeline.incremental_band_update(
        "slice-001",
        source_id=0,
        band_start=2,
        band_end=5,
        f0_min_hz=source.f0_min_hz - new_span * 0.35,
        f0_max_hz=source.f0_max_hz + new_span * 0.35,
    )
    incremental_verification = pipeline.verify_slice("slice-001")

    raw_config = read_json(workspace.state_root / "workspace.json")["config_snapshot"]
    changed_config = copy.deepcopy(raw_config)
    changed_config["mask_iterations"] = int(changed_config["mask_iterations"]) + 1
    changed_config_path = workspace.state_root / "demo-config-more-iterations.yaml"
    changed_config_path.write_text(yaml.safe_dump(changed_config, sort_keys=True), encoding="utf-8")
    workspace.import_dataset(
        manifest_path=workspace.samples_dir / "manifest.json",
        config_path=changed_config_path,
        preserve_overrides=True,
    )
    invalidation_build = pipeline.build_slice("slice-003")
    invalidation_records = pipeline.invalidations(slice_id="slice-003")["slices"][0]["records"]
    latest_invalidation = invalidation_records[-1] if invalidation_records else None
    upstream_reused = {
        stage: invalidation_build["stage_metrics"][stage]["status"]
        for stage in ("stft", "features", "mask_init")
    }
    final_rebuild = pipeline.build_all()
    final_verifications = {
        slice_id: pipeline.verify_slice(slice_id)["fresh_full_match"]
        for slice_id in workspace.effective_manifest().slice_ids()
    }
    final_status = pipeline.status()
    final_result_digests = {
        item["slice_id"]: item.get("result_digest")
        for item in final_status["slices"]
        if item.get("complete")
    }
    checkpoint_status = pipeline.status()["checkpoints"]
    sequence_continuous = checkpoint_status["recoverable_sequence"] == checkpoint_status["latest_sequence"]
    no_duplicate_or_corrupt = not checkpoint_status["diagnostics"]

    deterministic_report = {
        "schema_version": 1,
        "seed": 20261123,
        "dataset": {
            "manifest_digest": imported["manifest_digest"],
            "slice_count": imported["slice_count"],
            "total_samples": imported["total_samples"],
            "band_count": config.band_count,
            "source_counts": [item.source_count for item in workspace.manifest().slices],
        },
        "baseline": baseline_digests,
        "baseline_details": baseline_details,
        "graceful_stop": {
            "stop_after_units": 11,
            "stopped_status": stopped_build["status"],
            "resume_status": resumed["status"],
            "resumed_slice_001_digest": resumed_digests.get("slice-001"),
            "fresh_full_slice_001_digest": reference_result["result_digest"],
            "matches_fresh_full": resume_matches_full,
            "fresh_full_verification": resume_verification["fresh_full_match"],
            "checkpoint_recoverable_sequence": resume_checkpoint_status["recoverable_sequence"],
            "checkpoint_latest_sequence": resume_checkpoint_status["latest_sequence"],
        },
        "incremental": {
            "slice_id": "slice-001",
            "source_id": 0,
            "affected_bands": incremental["affected_bands"],
            "rebuilt_blocks": incremental["rebuilt_blocks"],
            "full_equivalent": incremental["full_equivalent"],
            "result_digest": incremental["result_digest"],
            "mask_digest": incremental["mask_digest"],
            "round_changes_digest": incremental["round_changes_digest"],
            "fresh_full_verification": incremental_verification["fresh_full_match"],
            "upstream_reused": {
                "stft": incremental["stage_metrics"]["stft"]["status"],
                "features": incremental["stage_metrics"]["features"]["status"],
            },
        },
        "invalidation": {
            "trigger": "mask_iterations +1",
            "earliest_stage": latest_invalidation.get("earliest_stage") if latest_invalidation else None,
            "affected_bands": latest_invalidation.get("affected_bands") if latest_invalidation else None,
            "reason": latest_invalidation.get("reason") if latest_invalidation else None,
            "upstream_reused": upstream_reused,
            "rebuild_stage_status": invalidation_build["stage_metrics"],
        },
        "final_consistency": {
            "rebuild_status": final_rebuild["status"],
            "fresh_full_verification": final_verifications,
            "all_slices_current": all(final_verifications.values()),
            "result_digests": final_result_digests,
        },
        "checkpoints": {
            "recoverable_sequence": checkpoint_status["recoverable_sequence"],
            "latest_sequence": checkpoint_status["latest_sequence"],
            "continuous": sequence_continuous,
            "no_duplicate_or_corrupt": no_duplicate_or_corrupt,
        },
    }
    deterministic_report["demo_digest"] = digest_json(deterministic_report)
    report = {
        **deterministic_report,
        "ok": all(
            [
                resume_matches_full,
                resume_verification["fresh_full_match"],
                incremental["full_equivalent"],
                incremental_verification["fresh_full_match"],
                latest_invalidation is not None,
                latest_invalidation.get("earliest_stage") == "mask_refine" if latest_invalidation else False,
                upstream_reused == {"stft": "reused", "features": "reused", "mask_init": "reused"},
                sequence_continuous,
                no_duplicate_or_corrupt,
                all(final_verifications.values()),
            ]
        ),
        "outputs": baseline_details,
    }
    target = Path(output_path).resolve() if output_path else workspace.project_root / "artifacts" / "demo_report.json"
    atomic_write_json(target, report)
    return report

