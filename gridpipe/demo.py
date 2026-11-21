"""Reproducible end-to-end demonstration and anomaly probes."""

from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
from PIL import Image, ImageDraw

from .canonical import atomic_write_bytes, atomic_write_json, digest_file, dump_bytes, sha256_bytes
from .chart_structure import extract_chart
from .checkpoints import CheckpointStore
from .config import default_configs
from .detection import detect_regions
from .errors import (
    E_CHART_AXIS,
    E_INVALID_REGION,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_TABLE_STRUCTURE,
    PipelineError,
)
from .export import write_exports
from .generator import generate_samples
from .imaging import load_rgb_and_gray
from .pipeline import Pipeline
from .table_structure import restore_table


def _probe_error(function: Any) -> Dict[str, Any]:
    try:
        function()
    except PipelineError as exc:
        return {"ok": True, "code": exc.code, "error": exc.to_dict()}
    except Exception as exc:  # pragma: no cover - diagnostic path
        return {
            "ok": False,
            "code": "E_UNEXPECTED",
            "message": str(exc),
            "exception_type": type(exc).__name__,
        }
    return {"ok": False, "code": None, "message": "expected PipelineError was not raised"}


def run_demo(root: str | Path, *, seed: int = 20261122) -> Dict[str, Any]:
    root_path = Path(root).resolve()
    data_dir = root_path / "data"
    work_root = root_path / "work"
    output_root = root_path / "output"
    for directory in (data_dir, work_root, output_root):
        directory.mkdir(parents=True, exist_ok=True)
    generation = generate_samples(data_dir, seed=seed)
    manifest = data_dir / "manifest.json"

    for name in ("demo-full", "demo-serial", "demo-resume", "demo-errors"):
        path = work_root / name
        if path.exists():
            shutil.rmtree(path)
    for name in ("demo-full", "demo-serial", "demo-resume"):
        path = output_root / name
        if path.exists():
            shutil.rmtree(path)

    full_pipeline = Pipeline(
        root_path, manifest, work_dir=work_root / "demo-full", output_dir=output_root / "demo-full"
    )
    full_result = full_pipeline.run(execution_workers=2)
    full_export = write_exports(full_pipeline)

    serial_pipeline = Pipeline(
        root_path,
        manifest,
        work_dir=work_root / "demo-serial",
        output_dir=output_root / "demo-serial",
    )
    serial_result = serial_pipeline.run(execution_workers=1)
    serial_export = write_exports(serial_pipeline)

    resume_pipeline = Pipeline(
        root_path,
        manifest,
        work_dir=work_root / "demo-resume",
        output_dir=output_root / "demo-resume",
    )
    stopped_result = resume_pipeline.run(stop_after=2, execution_workers=2)
    stopped_sequence = stopped_result["checkpoint_sequence"]
    resumed_pipeline = Pipeline(
        root_path,
        manifest,
        work_dir=work_root / "demo-resume",
        output_dir=output_root / "demo-resume",
    )
    resumed_result = resumed_pipeline.run(resume=True, execution_workers=2)
    resume_export = write_exports(resumed_pipeline)

    full_bytes = (output_root / "demo-full" / "export.json").read_bytes()
    serial_bytes = (output_root / "demo-serial" / "export.json").read_bytes()
    resume_bytes = (output_root / "demo-resume" / "export.json").read_bytes()
    comparison_files = ["export.json", "tables.json", "charts.json", "validation.json", "budget.json"]
    all_file_equality = {
        name: (
            (output_root / "demo-full" / name).read_bytes()
            == (output_root / "demo-serial" / name).read_bytes()
            == (output_root / "demo-resume" / name).read_bytes()
        )
        for name in comparison_files
    }

    configs = default_configs()
    shard = full_pipeline.shards[0]
    rgb, gray = load_rgb_and_gray(shard["_image_path"])
    detection = detect_regions(gray, shard_id=str(shard["id"]), config=configs["detection"])
    blank = np.full((120, 160), 255, dtype=np.uint8)
    invalid_region_probe = _probe_error(
        lambda: detect_regions(blank, shard_id="probe-invalid-region", config=configs["detection"])
    )

    declared = copy.deepcopy(shard["truth"]["table"])
    declared["rows"] = int(declared["rows"]) + 1
    table_probe = _probe_error(
        lambda: restore_table(
            rgb,
            gray,
            region=detection["table"]["pixel_rect"],
            declared=declared,
            config=configs["table"],
            shard_id=str(shard["id"]),
        )
    )

    chart_result = extract_chart(
        rgb,
        gray,
        region=detection["chart"]["pixel_rect"],
        config=configs["chart"],
        shard_id=str(shard["id"]),
    )
    chart_region = detection["chart"]["pixel_rect"]
    plot_rect = chart_result["plot_rect_pixel"]
    tick_y = chart_region[1] + chart_result["ticks"]["y"][1]["pixel"]
    fault_dir = work_root / "demo-errors"
    fault_dir.mkdir(parents=True, exist_ok=True)
    fault_image_path = fault_dir / "chart_axis_fault.png"
    with Image.open(shard["_image_path"]) as opened:
        fault_image = opened.convert("RGB")
    fault_draw = ImageDraw.Draw(fault_image)
    fault_draw.rectangle(
        [chart_region[0] + 3, tick_y - 9, plot_rect[0] - 8, tick_y + 10],
        fill=(248, 249, 250),
    )
    fault_image.save(fault_image_path, format="PNG", optimize=False, compress_level=6)
    fault_rgb, fault_gray = load_rgb_and_gray(fault_image_path)

    def chart_axis_probe() -> None:
        extract_chart(
            fault_rgb,
            fault_gray,
            region=chart_region,
            config=configs["chart"],
            shard_id=str(shard["id"]),
        )

    chart_probe = _probe_error(chart_axis_probe)
    chart_partial_preserved = bool(
        chart_probe.get("error", {}).get("partial", {}).get("series")
    )

    stale_dir = fault_dir / "stale"
    if stale_dir.exists():
        shutil.rmtree(stale_dir)
    stale_store = CheckpointStore(stale_dir, "probe-run")
    stale_probe = _probe_error(
        lambda: stale_store.publish(
            shard_id="shard-0001",
            shard_index=1,
            stage="detection",
            input_fingerprint="probe-fingerprint",
            config_version="probe-config",
            artifact_bytes=dump_bytes({"probe": True}),
            sequence=2,
        )
    )

    invalid_stage_dir = fault_dir / "invalid-stage"
    if invalid_stage_dir.exists():
        shutil.rmtree(invalid_stage_dir)
    invalid_stage_pipeline = Pipeline(
        root_path,
        manifest,
        work_dir=invalid_stage_dir,
        output_dir=fault_dir / "invalid-stage-output",
    )
    invalid_stage_probe = _probe_error(
        lambda: invalid_stage_pipeline.run(stage="chart", execution_workers=1)
    )

    checks = {
        "full_run_completed": full_result["status"] == "completed",
        "serial_run_completed": serial_result["status"] == "completed",
        "stop_was_observed": stopped_result["status"] == "stopped" and stopped_sequence < resumed_result["checkpoint_sequence"],
        "resume_completed": resumed_result["status"] == "completed",
        "parallel_equals_serial_export": full_bytes == serial_bytes,
        "stop_resume_equals_full_export": full_bytes == resume_bytes,
        "all_derived_files_equal": all(all_file_equality.values()),
        "invalid_region_code": invalid_region_probe.get("code") == E_INVALID_REGION,
        "table_structure_code": table_probe.get("code") == E_TABLE_STRUCTURE,
        "chart_axis_code": chart_probe.get("code") == E_CHART_AXIS,
        "chart_partial_series_preserved": chart_partial_preserved,
        "stale_checkpoint_code": stale_probe.get("code") == E_STALE_CHECKPOINT,
        "invalid_stage_code": invalid_stage_probe.get("code") == E_INVALID_STAGE,
        "budget_within_limit": bool(full_pipeline.schedule["within_total_budget"]),
        "unprocessed_reason_present": all(
            bool(item.get("reason")) for item in full_pipeline.schedule["unprocessed"]
        ),
    }
    passed = all(checks.values())
    export_sha = sha256_bytes(full_bytes)
    verification = {
        "schema_version": 1,
        "passed": passed,
        "checks": checks,
        "seed": seed,
        "generation": generation,
        "run": {
            "run_id": full_pipeline.run_id,
            "status": full_result["status"],
            "checkpoint_sequence": full_result["checkpoint_sequence"],
            "stopped_sequence": stopped_sequence,
            "resumed_sequence": resumed_result["checkpoint_sequence"],
            "stats_full": full_result["stats"],
            "stats_resumed": resumed_result["stats"],
        },
        "digests": {
            "export_sha256": export_sha,
            "serial_export_sha256": sha256_bytes(serial_bytes),
            "resume_export_sha256": sha256_bytes(resume_bytes),
            "files": full_export["files"],
        },
        "validation_summary": full_pipeline.export_data()["summary"],
        "budget": full_pipeline.budget_report(),
        "invalidation": full_pipeline.invalidation,
        "error_probes": {
            "invalid_region": invalid_region_probe,
            "table_structure": table_probe,
            "chart_axis": chart_probe,
            "stale_checkpoint": stale_probe,
            "invalid_stage": invalid_stage_probe,
        },
    }
    atomic_write_json(output_root / "verification.json", verification)
    markdown = _verification_markdown(verification)
    atomic_write_bytes(output_root / "VERIFICATION.md", markdown.encode("utf-8"))
    return verification


def _verification_markdown(verification: Dict[str, Any]) -> str:
    summary = verification["validation_summary"]
    checks = verification["checks"]
    lines: List[str] = [
        "# GridPipe Verification",
        "",
        f"- Overall: {'PASS' if verification['passed'] else 'FAIL'}",
        f"- Seed: `{verification['seed']}`",
        f"- Run id: `{verification['run']['run_id']}`",
        f"- Checkpoint sequence: stopped at `{verification['run']['stopped_sequence']}`, completed at `{verification['run']['resumed_sequence']}`",
        f"- Full export SHA-256: `{verification['digests']['export_sha256']}`",
        f"- Table region IoU mean: `{summary['table_region_iou_mean']}`",
        f"- Chart region IoU mean: `{summary['chart_region_iou_mean']}`",
        f"- Cell row/column accuracy mean: `{summary['cell_row_col_accuracy_mean']}`",
        f"- Chart series MAE mean: `{summary['chart_series_mae_mean']}`",
        f"- Accepted shards: `{summary['accepted_shard_count']}`; unprocessed: `{summary['unprocessed_count']}`",
        "",
        "## Checks",
        "",
    ]
    for name, value in checks.items():
        lines.append(f"- `{name}`: `{str(value).lower()}`")
    lines.extend(
        [
            "",
            "## Error Codes",
            "",
            f"- `E_INVALID_REGION`: `{verification['error_probes']['invalid_region'].get('code')}`",
            f"- `E_TABLE_STRUCTURE`: `{verification['error_probes']['table_structure'].get('code')}`",
            f"- `E_CHART_AXIS`: `{verification['error_probes']['chart_axis'].get('code')}`",
            f"- `E_STALE_CHECKPOINT`: `{verification['error_probes']['stale_checkpoint'].get('code')}`",
            f"- `E_INVALID_STAGE`: `{verification['error_probes']['invalid_stage'].get('code')}`",
            "",
            "All experiments are local, deterministic and use no network, container or GPU.",
            "",
        ]
    )
    return "\n".join(lines)
