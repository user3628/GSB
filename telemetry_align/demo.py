"""Repeatable end-to-end demonstration including incremental and stop/resume paths."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, sha256_bytes, sha256_file
from .workspace import AlignmentWorkspace


def _artifact_bytes(workspace: AlignmentWorkspace, stage: str) -> bytes:
    head = workspace.store.head()
    if head is None:
        raise RuntimeError("demo workspace has no checkpoint")
    if stage == "normalize":
        entry = head["catalog"]["normalize"]["grid"]
    else:
        entry = head["catalog"][stage]
    return (workspace.root / entry["artifact_path"]).read_bytes()


def run_demo(output_root: Path | str = "demo") -> dict[str, Any]:
    base = Path(output_root).resolve()
    base.mkdir(parents=True, exist_ok=True)

    workspace = AlignmentWorkspace(base / "workspace")
    workspace.init_demo(reset=True)
    initial_run = workspace.align()
    initial_summary = workspace.summary()
    initial_grid_bytes = _artifact_bytes(workspace, "normalize")
    initial_fusion_bytes = _artifact_bytes(workspace, "fusion")
    initial_summary_bytes = _artifact_bytes(workspace, "summary")
    initial_checkpoints = workspace.status()["checkpoints"]
    initial_counts = initial_summary["counts"]
    initial_overall = initial_run["overall_fingerprint"]

    v1_workspace = AlignmentWorkspace(base / "v1_workspace")
    v1_workspace.init_demo(reset=True)
    v1_run = v1_workspace.align(config_name="v1")
    v1_summary = v1_workspace.summary()
    upgrade = v1_workspace.upgrade_v1_config()
    v1_workspace.import_inputs(
        configs={"v2_upgraded": {"path": upgrade["path"]}},
        active_config="v2_upgraded",
    )
    upgraded_run = v1_workspace.align(config_name="v2_upgraded")

    mutation_result = workspace.mutate_source_and_rebuild(sensor_id="sensor_a")
    incremental_run = mutation_result["alignment"]
    incremental_summary = workspace.summary()

    recovery = AlignmentWorkspace(base / "recovery_workspace")
    recovery.init_demo(reset=True)
    recovery.request_stop("resample")
    stopped_run = recovery.align()
    stopped_status = recovery.status()
    resumed_run = recovery.align(resume=True)
    recovered_summary = recovery.summary()
    recovered_grid_bytes = _artifact_bytes(recovery, "normalize")
    recovered_fusion_bytes = _artifact_bytes(recovery, "fusion")
    recovered_summary_bytes = _artifact_bytes(recovery, "summary")
    recovery_checkpoints = recovery.status()["checkpoints"]

    report = {
        "schema_version": 1,
        "seed": 20261122,
        "fixed_summary_digest": sha256_bytes(initial_summary_bytes),
        "initial": {
            "status": initial_run["status"],
            "overall_fingerprint": initial_overall,
            "stage_fingerprints": initial_run["fingerprints"],
            "checkpoints": initial_checkpoints,
            "counts": initial_counts,
            "summary_digest": sha256_bytes(initial_summary_bytes),
            "grid_artifact_digest": sha256_bytes(initial_grid_bytes),
            "fusion_artifact_digest": sha256_bytes(initial_fusion_bytes),
        },
        "v1_compatibility": {
            "v1_run_status": v1_run["status"],
            "upgrade_equivalent": upgrade["equivalent"],
            "semantic_fingerprint_before": upgrade[
                "semantic_fingerprint_before"
            ],
            "semantic_fingerprint_after": upgrade["semantic_fingerprint_after"],
            "upgraded_run_status": upgraded_run["status"],
            "grid_matches_native_v2": (
                v1_summary["grid"]["fingerprint"]
                == initial_summary["grid"]["fingerprint"]
            ),
            "fusion_matches_native_v2": (
                v1_summary["fusion_result_digest"]
                == initial_summary["fusion_result_digest"]
            ),
            "upgraded_config_path": str(
                (v1_workspace.root / upgrade["path"]).relative_to(base)
            ),
        },
        "incremental": {
            "mutation": mutation_result["mutation"],
            "status": incremental_run["status"],
            "earliest_invalid_stage": incremental_run["invalidations"][
                "earliest_stage"
            ],
            "reasons": incremental_run["invalidations"]["reasons"],
            "executed_stages": incremental_run["executed_stages"],
            "reused_artifacts": incremental_run["reused"],
            "rebuilt_artifacts": incremental_run["rebuilt"],
            "checkpoint_sequence": incremental_run["checkpoint_sequence"],
            "summary_digest_before": sha256_bytes(initial_summary_bytes),
            "summary_digest_after": sha256_bytes(
                (workspace.root / "out" / "alignment_summary.json").read_bytes()
            ),
        },
        "stop_resume": {
            "stopped_status": stopped_run["status"],
            "stopped_executed_stages": stopped_run["executed_stages"],
            "stopped_head_stage": stopped_status["checkpoints"]["head_stage"],
            "resumed_status": resumed_run["status"],
            "resumed_executed_stages": resumed_run["executed_stages"],
            "checkpoint_sequence": recovery_checkpoints["sequence"],
            "checkpoint_sequence_contiguous": recovery_checkpoints[
                "sequence"
            ]
            == list(range(1, len(recovery_checkpoints["sequence"]) + 1)),
            "grid_bytes_equal": recovered_grid_bytes == initial_grid_bytes,
            "fusion_bytes_equal": recovered_fusion_bytes == initial_fusion_bytes,
            "summary_bytes_equal": recovered_summary_bytes
            == initial_summary_bytes,
            "fusion_fingerprint_equal": recovered_summary[
                "fusion_result_digest"
            ]
            == initial_summary["fusion_result_digest"],
        },
        "workspace_paths": {
            "main": str((base / "workspace").relative_to(base)),
            "v1": str((base / "v1_workspace").relative_to(base)),
            "recovery": str((base / "recovery_workspace").relative_to(base)),
        },
    }
    report_path = base / "reports" / "demo_report.json"
    atomic_write_json(report_path, report)
    return {**report, "report_path": str(report_path)}

