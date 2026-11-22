"""Deterministic end-to-end demo and stop/resume equivalence verification."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest, hash_tree, sha256_bytes, sha256_file
from .config import Settings
from .generator import generate_demo
from .workbench import Workbench


STOP_POINTS = {
    "seq_uniform": 12,
    "seq_scaling": 9,
    "seq_direction": 20,
}


def _recreate(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _clear_tree(path: Path, *, recreate: bool = True) -> None:
    if path.exists():
        shutil.rmtree(path)
    if recreate:
        path.mkdir(parents=True, exist_ok=True)


def reset_workbench_state(settings: Settings) -> None:
    for path in (
        settings.checkpoints_dir,
        settings.snapshots_dir,
        settings.artifacts_dir,
        settings.invalidated_dir,
        settings.reports_dir,
        settings.diagnostics_dir,
        settings.stop_dir,
    ):
        _recreate(path)


def snapshot_tree(source: Path, destination: Path) -> dict[str, str]:
    """Copy a directory to a snapshot location, replacing the destination."""
    parent = destination.parent
    parent.mkdir(parents=True, exist_ok=True)
    temp = parent / f".{destination.name}.snapshot.{os.getpid()}.tmp"
    if temp.exists():
        shutil.rmtree(temp)
    shutil.copytree(source, temp)
    if destination.exists():
        shutil.rmtree(destination)
    os.replace(temp, destination)
    return hash_tree(destination)


def _bundle_digest_from_bytes(payload: bytes) -> str:
    import json

    value = json.loads(payload.decode("utf-8"))
    return str(value.get("summary_sha256"))


def run_demo(settings: Settings) -> dict[str, Any]:
    reset_workbench_state(settings)
    generation = generate_demo(settings, force=True)
    workbench = Workbench(settings)
    sequence_ids = [sequence["sequence_id"] for sequence in workbench.manifest.sequences]

    # Baseline: a from-zero complete run for every sequence.
    baseline_runs = []
    for sequence_id in sequence_ids:
        result = workbench.run_stage(sequence_id, "all")
        if result.get("status") != "complete":
            raise RuntimeError(f"baseline run for {sequence_id} did not complete: {result}")
        baseline_runs.append(result)
    baseline_report = workbench.build_report()
    baseline_bundle_bytes = (settings.reports_dir / "run_bundle.json").read_bytes()
    baseline_summary_bytes = (settings.reports_dir / "summary.json").read_bytes()

    full_root = settings.snapshots_dir / "full"
    full_checkpoint_hashes = snapshot_tree(settings.checkpoints_dir, full_root / "checkpoints")
    (full_root / "run_bundle.json").write_bytes(baseline_bundle_bytes)
    (full_root / "summary.json").write_bytes(baseline_summary_bytes)

    # Resumed run: stop after a deterministic per-sequence frame, then resume.
    _clear_tree(settings.checkpoints_dir)
    for sequence in workbench.manifest.sequences:
        _clear_tree(settings.sequence_artifact_dir(sequence["sequence_id"]))
    _clear_tree(settings.invalidated_dir)
    _clear_tree(settings.diagnostics_dir)
    _clear_tree(settings.reports_dir)
    _clear_tree(settings.stop_dir)

    stop_events: list[dict[str, Any]] = []
    resume_runs: list[dict[str, Any]] = []
    replayed_frames = 0
    for sequence_id in sequence_ids:
        stop_at = int(STOP_POINTS.get(sequence_id, 10))
        stopped = workbench.run_stage(sequence_id, "all", stop_after=stop_at)
        stop_events.append(
            {
                "sequence_id": sequence_id,
                "requested_stop_after_frames": stop_at,
                "status": stopped.get("status"),
                "valid_frames_at_stop": stopped.get("valid_frames"),
                "next_frame_index": stopped.get("next_frame_index"),
                "stop_reason": stopped.get("stop_reason"),
                "checkpoint_chain_digest_at_stop": stopped.get("checkpoint_chain_digest"),
            }
        )
        if stopped.get("status") != "stopped":
            raise RuntimeError(f"expected graceful stop for {sequence_id}, got {stopped}")
        resumed = workbench.resume_sequence(sequence_id)
        if resumed.get("status") != "complete":
            raise RuntimeError(f"resume for {sequence_id} did not complete: {resumed}")
        processed = resumed.get("processed_frames") or []
        expected_start = stopped.get("valid_frames", -1)
        if processed and int(processed[0]) != int(expected_start):
            raise RuntimeError(f"resume for {sequence_id} restarted at {processed[0]}, expected {expected_start}")
        if processed and len(set(processed)) != len(processed):
            raise RuntimeError(f"resume for {sequence_id} recomputed a frame")
        replayed_frames += max(0, int(expected_start) - int(processed[0])) if processed else 0
        resume_runs.append(resumed)

    resumed_report = workbench.build_report()
    resumed_bundle_bytes = (settings.reports_dir / "run_bundle.json").read_bytes()
    resumed_summary_bytes = (settings.reports_dir / "summary.json").read_bytes()
    resume_root = settings.snapshots_dir / "resume"
    resumed_checkpoint_hashes = snapshot_tree(settings.checkpoints_dir, resume_root / "checkpoints")
    (resume_root / "run_bundle.json").write_bytes(resumed_bundle_bytes)
    (resume_root / "summary.json").write_bytes(resumed_summary_bytes)

    bundle_equal = baseline_bundle_bytes == resumed_bundle_bytes
    summary_equal = baseline_summary_bytes == resumed_summary_bytes
    checkpoints_equal = full_checkpoint_hashes == resumed_checkpoint_hashes
    final_bundle = __import__("json").loads(resumed_bundle_bytes.decode("utf-8"))
    sequence_summaries = []
    for bundle in final_bundle["sequences"]:
        sequence_summaries.append(
            {
                "sequence_id": bundle["sequence_id"],
                "frames": len(bundle["tracking_frames"]),
                "mean_iou": bundle["mean_iou"],
                "max_center_error_normalized": bundle["max_center_error_normalized"],
                "occlusion_intervals": bundle["occlusion_intervals"],
                "recovered_occlusion_count": sum(1 for item in bundle["occlusion_intervals"] if item.get("recovered")),
                "template_update_count": len(bundle["template_updates"]),
                "checkpoint_chain_digest": bundle["checkpoint_chain_digest"],
                "bundle_digest": bundle["bundle_digest"],
                "final_box_xywh_norm": bundle["final_box_xywh_norm"],
            }
        )
    demo = {
        "schema_version": 1,
        "kind": "tracklab.demo_summary",
        "seed": settings.generator["seed"],
        "manifest_sha256": generation["manifest_sha256"],
        "manifest_fingerprint": generation["manifest_fingerprint"],
        "baseline_summary_sha256": _bundle_digest_from_bytes(baseline_bundle_bytes),
        "resumed_summary_sha256": _bundle_digest_from_bytes(resumed_bundle_bytes),
        "byte_identical": bundle_equal and summary_equal,
        "run_bundle_byte_identical": bundle_equal,
        "report_summary_byte_identical": summary_equal,
        "checkpoint_trees_identical": checkpoints_equal,
        "checkpoint_file_count": len(resumed_checkpoint_hashes),
        "full_checkpoint_tree_digest": digest(full_checkpoint_hashes),
        "resumed_checkpoint_tree_digest": digest(resumed_checkpoint_hashes),
        "replayed_frames": replayed_frames,
        "stop_events": stop_events,
        "sequence_count": len(sequence_summaries),
        "total_frames": sum(item["frames"] for item in sequence_summaries),
        "sequences": sequence_summaries,
        "baseline_run": {
            "summary_path": baseline_report["summary_path"],
            "summary_sha256": baseline_report["summary_sha256"],
        },
        "resumed_run": {
            "summary_path": resumed_report["summary_path"],
            "summary_sha256": resumed_report["summary_sha256"],
        },
    }
    demo["verification_passed"] = bool(
        demo["byte_identical"]
        and demo["checkpoint_trees_identical"]
        and demo["replayed_frames"] == 0
        and all(event["status"] == "stopped" for event in stop_events)
        and any(item["recovered_occlusion_count"] > 0 for item in sequence_summaries)
    )
    demo["demo_summary_sha256"] = digest(demo)
    atomic_write_json(settings.reports_dir / "demo_summary.json", demo, pretty=True)
    return demo
