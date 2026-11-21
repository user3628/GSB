"""Repeatable end-to-end demonstration and acceptance harness."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    digest_object,
    file_sha256,
)
from .checkpoints import STAGE_ORDER
from .config import normalize_config
from .data import load_dataset
from .error_demos import collect_error_demonstrations
from .errors import GracefulStop
from .generator import BASE_SEED, generate_dataset
from .stop import StopController
from .workbench import Workbench


def _stage_bytes(result: dict[str, Any]) -> dict[str, bytes]:
    return {
        stage: canonical_json_bytes(result["payloads"][stage], strict=True)
        for stage in STAGE_ORDER
    }


def _record_map(result: dict[str, Any]) -> dict[str, dict[str, int]]:
    return {
        stage: {
            "sequence": int(result["records"][stage]["sequence"]),
            "stage_revision": int(result["records"][stage]["stage_revision"]),
        }
        for stage in STAGE_ORDER
    }


def _data_hashes(data_dir: Path) -> dict[str, str]:
    return {
        str(path.relative_to(data_dir)): file_sha256(path)
        for path in sorted(data_dir.rglob("*"))
        if path.is_file()
    }


def _fresh_workdir(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_demo(
    *,
    data_dir: str | Path = "data",
    output_dir: str | Path = "runs/demo",
    seed: int = BASE_SEED,
    regenerate: bool = True,
) -> dict[str, Any]:
    data_root = Path(data_dir)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    if regenerate:
        generate_dataset(data_root, seed=seed)
    datasets = [load_dataset(data_root, name) for name in ("small", "large")]
    generator_reproducible: bool | None = None
    if regenerate:
        original_hashes = _data_hashes(data_root)
        with tempfile.TemporaryDirectory(prefix="kde-generator-replay-") as temporary:
            replay = Path(temporary)
            generate_dataset(replay, seed=seed)
            generator_reproducible = _data_hashes(replay) == original_hashes

    config = normalize_config(None)
    small_dataset, large_dataset = datasets
    small_workdir = _fresh_workdir(output / "small-baseline")
    small = Workbench(small_dataset, config, small_workdir).run(stop=StopController())
    small_bytes = _stage_bytes(small)

    batch_workdir = _fresh_workdir(output / "small-batched-reverse")
    batched = Workbench(small_dataset, config, batch_workdir).run(
        stop=StopController(), batch_size=173, batch_order="reverse"
    )
    batch_bytes = _stage_bytes(batched)
    batch_invariance = {
        "batch_size": 173,
        "batch_order": "reverse",
        "summary_byte_equal": canonical_json_bytes(small["summary"], strict=True)
        == canonical_json_bytes(batched["summary"], strict=True),
        "density_byte_equal": small_bytes["density"] == batch_bytes["density"],
        "metrics_byte_equal": small_bytes["metrics"] == batch_bytes["metrics"],
        "drift_byte_equal": small_bytes["drift"] == batch_bytes["drift"],
        "output_digest": batched["summary"]["output_digest"],
    }

    large_workdir = _fresh_workdir(output / "large-baseline")
    large = Workbench(large_dataset, config, large_workdir).run(stop=StopController())

    strict_config = normalize_config({"drift": {"alpha": 0.01}})
    incremental_workbench = Workbench(small_dataset, strict_config, small_workdir)
    invalidation_before = incremental_workbench.analyze()
    incremental = incremental_workbench.run(stop=StopController())
    incremental_changed = bool(
        small["summary"]["drift_count"] != incremental["summary"]["drift_count"]
    )
    incremental_result = {
        "changed_config_path": "drift.alpha",
        "old_alpha": config["drift"]["alpha"],
        "new_alpha": strict_config["drift"]["alpha"],
        "earliest_invalid_stage": invalidation_before["earliest_invalid_stage"],
        "affected_stages": invalidation_before["affected_stages"],
        "reusable_stages": invalidation_before["reusable_stages"],
        "old_config_version": small["config_version"],
        "new_config_version": incremental["config_version"],
        "config_version_changed": small["config_version"] != incremental["config_version"],
        "old_sequence": small["outer_sequence"],
        "new_sequence": incremental["outer_sequence"],
        "old_stage_records": _record_map(small),
        "new_stage_records": _record_map(incremental),
        "old_drift_columns": small["summary"]["drift_columns"],
        "new_drift_columns": incremental["summary"]["drift_columns"],
        "decision_changed": incremental_changed,
    }

    resume_workdir = _fresh_workdir(output / "small-interrupted")
    interrupted_workbench = Workbench(small_dataset, config, resume_workdir)
    stop_after = len(small_dataset.column_names) + 1
    stopped: dict[str, Any]
    try:
        interrupted_workbench.run(stop=StopController(stop_after_units=stop_after))
    except GracefulStop as exc:
        stopped = exc.to_dict()
    else:  # pragma: no cover - deterministic dataset should stop here
        stopped = {"status": "did_not_stop"}
    intermediate = interrupted_workbench.analyze()
    resumed = interrupted_workbench.run(stop=StopController())
    resumed_bytes = _stage_bytes(resumed)
    resume_result = {
        "stop_after_units": stop_after,
        "stopped": stopped,
        "sequence_after_stop": intermediate["latest_sequence"],
        "earliest_stage_after_stop": intermediate["earliest_invalid_stage"],
        "sequence_after_resume": resumed["outer_sequence"],
        "stage_records_after_resume": _record_map(resumed),
        "density_byte_equal_to_full_run": resumed_bytes["density"] == small_bytes["density"],
        "metrics_byte_equal_to_full_run": resumed_bytes["metrics"] == small_bytes["metrics"],
        "drift_byte_equal_to_full_run": resumed_bytes["drift"] == small_bytes["drift"],
        "summary_byte_equal_to_full_run": canonical_json_bytes(
            resumed["summary"], strict=True
        )
        == canonical_json_bytes(small["summary"], strict=True),
        "sequence_continuous": [
            resumed["records"][stage]["sequence"] for stage in STAGE_ORDER
        ]
        == [1, 2, 3, 4],
    }

    export_workbench = Workbench(small_dataset, config, batch_workdir)
    density_export = export_workbench.export_bytes("density")
    report_export = export_workbench.export_bytes("report")
    density_path = output / "density_curves.json"
    report_path = output / "drift_report.json"
    atomic_write_bytes(density_path, density_export)
    atomic_write_bytes(report_path, report_export)

    error_demonstrations = collect_error_demonstrations(small_dataset)
    checks = {
        "generator_reproducible": generator_reproducible is not False,
        "small_completed": small["status"] == "completed",
        "large_completed": large["status"] == "completed",
        "batch_summary_equal": batch_invariance["summary_byte_equal"],
        "batch_density_equal": batch_invariance["density_byte_equal"],
        "batch_metrics_equal": batch_invariance["metrics_byte_equal"],
        "batch_drift_equal": batch_invariance["drift_byte_equal"],
        "incremental_earliest_is_drift": invalidation_before["earliest_invalid_stage"]
        == "drift",
        "incremental_upstream_reused": invalidation_before["reusable_stages"]
        == ["bandwidth", "density", "metrics"],
        "incremental_decision_changed": incremental_changed,
        "incremental_sequence_advanced": incremental["outer_sequence"]
        == small["outer_sequence"] + 1,
        "resume_after_density_chunk": stopped.get("stage") == "density",
        "resume_sequence_continuous": resume_result["sequence_continuous"],
        "resume_density_equal": resume_result["density_byte_equal_to_full_run"],
        "resume_metrics_equal": resume_result["metrics_byte_equal_to_full_run"],
        "resume_drift_equal": resume_result["drift_byte_equal_to_full_run"],
        "error_codes_complete": {item["raised"] for item in error_demonstrations}
        == {
            "E_NUMERIC",
            "E_COLUMN_MISMATCH",
            "E_INVALID_BATCH",
            "E_STALE_CHECKPOINT",
            "E_INVALID_STAGE",
        },
    }

    result = {
        "schema_version": 1,
        "seed": seed,
        "generator_reproducible": generator_reproducible,
        "data_files": _data_hashes(data_root),
        "small": {
            "summary": small["summary"],
            "stage_records": _record_map(small),
        },
        "large": {
            "summary": large["summary"],
            "stage_records": _record_map(large),
        },
        "batch_invariance": batch_invariance,
        "incremental_rebuild": incremental_result,
        "interrupt_resume": resume_result,
        "error_codes": error_demonstrations,
        "exports": {
            "density": {
                "path": str(density_path),
                "bytes": len(density_export),
                "sha256": file_sha256(density_path),
            },
            "report": {
                "path": str(report_path),
                "bytes": len(report_export),
                "sha256": file_sha256(report_path),
            },
        },
        "checks": checks,
        "all_checks_passed": all(checks.values()),
        "demo_digest": "",
    }
    result["demo_digest"] = digest_object({key: value for key, value in result.items() if key != "demo_digest"})
    atomic_write_json(output / "demo_results.json", result)
    return result
