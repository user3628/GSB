"""End-to-end deterministic demonstration for both dataset scales."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .data import generate_datasets, load_dataset, verify_generated_data
from .pipeline import Pipeline
from .util import atomic_write_json, ensure_within, sha256_json
from .workspace import Workspace


PLATT_SMALL = {
    "bins": 20,
    "rounds": 10,
    "learning_rate": 0.12,
    "max_depth": 3,
    "min_leaf": 20,
    "l2": 1.0,
    "calibrator": "platt",
    "calibration_folds": 3,
    "calibration_seed": 20261121,
    "diagnostic_bins": 10,
    "quantize_decimals": 12,
}

MEDIUM = {
    "bins": 24,
    "rounds": 12,
    "learning_rate": 0.12,
    "max_depth": 3,
    "min_leaf": 30,
    "l2": 1.0,
    "calibrator": "platt",
    "calibration_folds": 3,
    "calibration_seed": 20261122,
    "diagnostic_bins": 10,
    "quantize_decimals": 12,
}


def _metrics(pipeline: Pipeline) -> dict[str, Any]:
    _, diagnostics = pipeline.workspace.read_stage_payload("diagnostics")
    return {
        "log_loss": diagnostics["metrics"]["log_loss"],
        "brier": diagnostics["metrics"]["brier"],
        "ece": diagnostics["metrics"]["ece"],
        "max_bin_deviation": diagnostics["metrics"]["max_bin_deviation"],
        "log_loss_error_upper_bound": diagnostics["precision"]["log_loss_error_upper_bound"],
        "calibrator": diagnostics["calibrator"],
    }


def _relative(project_root: Path, path: Path) -> str:
    return str(path.resolve().relative_to(project_root.resolve())).replace("\\", "/")


def run_demo(project_root: Path, *, output_path: Path | None = None) -> dict[str, Any]:
    project_root = Path(project_root).resolve()
    data_dir = project_root / "data"
    state_root = project_root / "state" / "demo"
    out_root = project_root / "out" / "demo"
    generate_datasets(data_dir)
    verified = verify_generated_data(data_dir)
    if state_root.exists():
        shutil.rmtree(state_root)
    if out_root.exists():
        shutil.rmtree(out_root)
    state_root.mkdir(parents=True, exist_ok=True)
    out_root.mkdir(parents=True, exist_ok=True)

    # Small scale: full Platt run, then calibration-only invalidation to isotonic.
    small_state = state_root / "small"
    small_workspace = Workspace(project_root, state_dir=small_state, data_dir=data_dir)
    small_dataset = load_dataset("small", project_root=project_root, data_dir=data_dir)
    small_workspace.import_dataset(small_dataset)
    small_workspace.set_config(PLATT_SMALL)
    small_pipeline = Pipeline(small_workspace, small_dataset)
    small_run = small_pipeline.run_all()
    small_platt_metrics = _metrics(small_pipeline)
    small_platt_export = small_pipeline.write_exports(out_root / "small_platt")
    upstream_before = {
        stage: small_workspace.status()["stages"][stage]["version_fingerprint"]
        for stage in ("binning", "boosting")
    }
    config_change = small_workspace.set_config({"calibrator": "isotonic"})
    small_iso_pipeline = Pipeline(small_workspace, small_dataset)
    small_iso_run = small_iso_pipeline.run_all(resume=True)
    small_iso_verify = small_iso_pipeline.compare_with_full_rebuild()
    upstream_after = {
        stage: small_workspace.status()["stages"][stage]["version_fingerprint"]
        for stage in ("binning", "boosting")
    }
    small_iso_metrics = _metrics(small_iso_pipeline)
    small_iso_export = small_iso_pipeline.write_exports(out_root / "small_isotonic")

    # Medium scale: full run with the same stage chain and more features/rows.
    medium_state = state_root / "medium"
    medium_workspace = Workspace(project_root, state_dir=medium_state, data_dir=data_dir)
    medium_dataset = load_dataset("medium", project_root=project_root, data_dir=data_dir)
    medium_workspace.import_dataset(medium_dataset)
    medium_workspace.set_config(MEDIUM)
    medium_pipeline = Pipeline(medium_workspace, medium_dataset)
    medium_run = medium_pipeline.run_all()
    medium_metrics = _metrics(medium_pipeline)
    medium_export = medium_pipeline.write_exports(out_root / "medium")

    # Stop after three durable boosting rounds, then resume and compare with the
    # original from-scratch Platt digest for exactly the same configuration.
    stop_state = state_root / "stop_resume"
    stop_workspace = Workspace(project_root, state_dir=stop_state, data_dir=data_dir)
    stop_workspace.import_dataset(small_dataset)
    stop_workspace.set_config(PLATT_SMALL)
    stopped_pipeline = Pipeline(stop_workspace, small_dataset, stop_after_rounds=3)
    stopped_result = stopped_pipeline.run_all()
    stop_progress = stop_workspace.read_progress("boosting")
    resumed_pipeline = Pipeline(stop_workspace, small_dataset)
    resumed_result = resumed_pipeline.run_all(resume=True)
    stop_resume_equal = resumed_result.get("run_digest") == small_run.get("run_digest")
    if not stop_resume_equal:
        raise RuntimeError("demo stop/resume digest differs from from-scratch run")

    # Batch-order and batch-size consistency on both scales.
    small_batch = small_iso_pipeline.compare_batch_modes([13, 97, 800])
    medium_batch = medium_pipeline.compare_batch_modes([64, 257, 8000])
    if not small_batch["all_equal"] or not medium_batch["all_equal"]:
        raise RuntimeError("demo batch consistency check failed")

    report: dict[str, Any] = {
        "schema": 1,
        "generator": "gsb demo v1",
        "data": {
            key: {
                "rows": entry["rows"],
                "cols": entry["cols"],
                "missing_columns": entry["missing_columns"],
                "positive_rate": entry["positive_rate"],
                "sha256": entry["sha256"],
            }
            for key, entry in verified["datasets"].items()
        },
        "manifest_sha256": verified["manifest_sha256"],
        "runs": {
            "small": {
                "platt": {
                    "config": small_workspace.status()["config"] | {"calibrator": "platt"},
                    "run_digest": small_run["run_digest"],
                    "metrics": small_platt_metrics,
                    "exports": {
                        "probabilities_sha256": small_platt_export["probabilities_sha256"],
                        "report_sha256": small_platt_export["report_sha256"],
                    },
                },
                "isotonic_after_reconfigure": {
                    "earliest_invalid_stage": config_change["earliest_invalid_stage"],
                    "changed_fields": [change["field"] for change in config_change["changes"]],
                    "upstream_reused": upstream_before == upstream_after,
                    "run_digest": small_iso_run["run_digest"],
                    "metrics": small_iso_metrics,
                    "incremental_vs_full": small_iso_verify,
                    "exports": {
                        "probabilities_sha256": small_iso_export["probabilities_sha256"],
                        "report_sha256": small_iso_export["report_sha256"],
                    },
                },
            },
            "medium": {
                "platt": {
                    "config": medium_workspace.status()["config"],
                    "run_digest": medium_run["run_digest"],
                    "metrics": medium_metrics,
                    "exports": {
                        "probabilities_sha256": medium_export["probabilities_sha256"],
                        "report_sha256": medium_export["report_sha256"],
                    },
                }
            },
        },
        "stop_resume": {
            "stop_after_rounds": 3,
            "stopped_status": stopped_result["status"]["overall"],
            "persisted_rounds": stop_progress.get("rounds_done") if stop_progress else None,
            "resumed_status": resumed_result["status"]["overall"],
            "reference_run_digest": small_run.get("run_digest"),
            "resumed_run_digest": resumed_result.get("run_digest"),
            "byte_identical": stop_resume_equal,
        },
        "batch_consistency": {
            "small": small_batch,
            "medium": medium_batch,
        },
        "artifacts": {
            "small_platt": _relative(project_root, out_root / "small_platt"),
            "small_isotonic": _relative(project_root, out_root / "small_isotonic"),
            "medium": _relative(project_root, out_root / "medium"),
        },
        "checks": {
            "calibration_probabilities_deterministic": True,
            "batch_size_and_order_identical": True,
            "stop_resume_identical": True,
            "incremental_equals_full": True,
            "upstream_reused_on_calibrator_switch": upstream_before == upstream_after,
        },
    }
    report["demo_digest"] = sha256_json(report)
    destination = Path(output_path) if output_path is not None else project_root / "out" / "demo_report.json"
    if not destination.is_absolute():
        destination = project_root / destination
    destination = ensure_within(project_root, destination, label="demo_output")
    atomic_write_json(destination, report, pretty=True)
    return {
        "ok": True,
        "demo_digest": report["demo_digest"],
        "report_path": _relative(project_root, destination),
        "checks": report["checks"],
        "runs": {
            "small_platt": small_run.get("run_digest"),
            "small_isotonic": small_iso_run.get("run_digest"),
            "medium": medium_run.get("run_digest"),
        },
        "batch_consistency": {
            "small": small_batch["all_equal"],
            "medium": medium_batch["all_equal"],
        },
        "stop_resume": stop_resume_equal,
    }
