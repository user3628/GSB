"""Deterministic end-to-end demonstration with a reproducible report digest."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from .activation import activation_schedule
from .checkpoints import publish_block, scan_stage
from .engine import ExecutionEngine
from .errors import (
    E_INVALID_STAGE,
    E_NUMERIC,
    E_SHAPE_MISMATCH,
    E_STALE_CHECKPOINT,
    BusinessError,
)
from .paths import STAGES
from .tensorio import npy_bytes
from .util import atomic_write_json, canonical_json, digest_bytes, digest_json
from .workbench import Workbench


def _fresh_directory(path: Path, root: Path) -> None:
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"demo directory must remain inside workspace: {path}") from exc
    if resolved.exists():
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True, exist_ok=True)


def _sha(path: Path) -> str:
    return digest_bytes(path.read_bytes())


def _checkpoint_manifest(wb: Workbench, job_id: str) -> dict[str, list[dict[str, Any]]]:
    bundle = wb.scan_all_stages(job_id)
    return {
        stage: [
            {
                "sequence": record["sequence"],
                "block": record["block"]["key"],
                "product_digest": record["product_digest"],
                "status": record["status"],
            }
            for record in bundle["scans"][stage]["records"]
        ]
        for stage in STAGES
    }


def _checkpoint_records(wb: Workbench, job_id: str) -> dict[str, str]:
    root = wb.paths.job(job_id).checkpoints_dir
    return {
        path.relative_to(root).as_posix(): digest_bytes(path.read_bytes())
        for path in sorted(root.rglob("seq-*.json"))
    }


def _clean_run(wb: Workbench, engine: ExecutionEngine, sample: str, config: str, job_id: str) -> dict[str, Any]:
    wb.import_job(
        source_path=f"samples/{sample}.npz",
        config_path=f"configs/{config}.yaml",
        job_id=job_id,
    )
    result = engine.run_job(job_id)
    paths = wb.paths.job(job_id)
    status = wb.job_status(job_id)
    summary_path = paths.output_path("summary.json")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    return {
        "sample": sample,
        "config": config,
        "status": result["status"],
        "complete": result["complete"],
        "business_code": result["business_code"],
        "alerts": status["alerts"],
        "version_fingerprint": result["version_fingerprint"],
        "activation_sha256": _sha(paths.output_path("activation.npy")),
        "reordered_sha256": _sha(paths.output_path("reordered.npy")),
        "summary_sha256": _sha(summary_path),
        "summary": summary,
    }


def _recovery_demo(root: Path, base: Path) -> dict[str, Any]:
    full_wb = Workbench(root, base / "full")
    full_engine = ExecutionEngine(full_wb)
    full_wb.import_job(
        source_path="samples/tensor_4d_gelu_swap.npz",
        config_path="configs/gelu_swap.yaml",
        job_id="resume-demo",
    )
    full_engine.run_job("resume-demo")
    full_dir = full_wb.paths.job("resume-demo").outputs_dir
    full_manifest = _checkpoint_manifest(full_wb, "resume-demo")

    resumed_wb = Workbench(root, base / "recovered")
    resumed_engine = ExecutionEngine(resumed_wb)
    resumed_wb.import_job(
        source_path="samples/tensor_4d_gelu_swap.npz",
        config_path="configs/gelu_swap.yaml",
        job_id="resume-demo",
    )
    stopped = resumed_engine.run_job("resume-demo", stop_after_stage="activation")
    resumed_wb.request_stop("resume-demo")
    stop_visible = resumed_wb.job_status("resume-demo")["stop_requested"]
    resumed = resumed_engine.run_job("resume-demo", clear_stop=True)
    resumed_dir = resumed_wb.paths.job("resume-demo").outputs_dir
    output_equal = {
        name: (full_dir / name).read_bytes() == (resumed_dir / name).read_bytes()
        for name in ("activation.npy", "reordered.npy", "summary.json")
    }
    resumed_manifest = _checkpoint_manifest(resumed_wb, "resume-demo")
    checkpoint_equal = full_manifest == resumed_manifest
    checkpoint_records_equal = _checkpoint_records(full_wb, "resume-demo") == _checkpoint_records(
        resumed_wb, "resume-demo"
    )
    sequences = {
        stage: [item["sequence"] for item in resumed_manifest[stage]]
        for stage in STAGES
    }
    contiguous = {
        stage: values == list(range(1, len(values) + 1))
        for stage, values in sequences.items()
    }
    return {
        "stopped_status": stopped["status"],
        "stopped_at_stage": stopped["stopped_at"]["stage"] if stopped.get("stopped_at") else None,
        "stop_request_observable": stop_visible,
        "resume_status": resumed["status"],
        "activation_reused_on_resume": resumed["stages"]["activation"]["reused_blocks"],
        "activation_computed_on_resume": resumed["stages"]["activation"]["computed_blocks"],
        "outputs_byte_equal": output_equal,
        "checkpoint_manifest_equal": checkpoint_equal,
        "checkpoint_records_byte_equal": checkpoint_records_equal,
        "sequences": sequences,
        "sequences_contiguous": contiguous,
        "full_summary_sha256": _sha(full_dir / "summary.json"),
        "resumed_summary_sha256": _sha(resumed_dir / "summary.json"),
        "passed": all(output_equal.values())
        and checkpoint_equal
        and checkpoint_records_equal
        and all(contiguous.values()),
    }


def _invalidation_demo(wb: Workbench, engine: ExecutionEngine) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    activation_before = wb.paths.job("silu-tile").output_path("activation.npy").read_bytes()
    reordered_before = wb.paths.job("silu-tile").output_path("reordered.npy").read_bytes()

    wb.import_job(
        source_path="samples/matrix_silu_tile.npz",
        config_path="configs/silu_tile_mean.yaml",
        job_id="silu-tile",
    )
    event = wb.invalidations("silu-tile")["events"][-1]
    run = engine.run_job("silu-tile")
    activation_unchanged = (
        activation_before == wb.paths.job("silu-tile").output_path("activation.npy").read_bytes()
    )
    reordered_unchanged = (
        reordered_before == wb.paths.job("silu-tile").output_path("reordered.npy").read_bytes()
    )
    cases.append(
        {
            "change": "sum_strategy: sum -> mean",
            "earliest_stage": event["earliest_stage"],
            "affected_stages": event["affected_stages"],
            "unaffected_stages": event["unaffected_stages"],
            "reasons": event["reasons"],
            "reused_blocks": {stage: run["stages"][stage]["reused_blocks"] for stage in STAGES},
            "computed_blocks": {stage: run["stages"][stage]["computed_blocks"] for stage in STAGES},
            "upstream_bytes_unchanged": activation_unchanged and reordered_unchanged,
        }
    )

    manifest = json.loads((wb.root / "samples" / "shape_manifest.json").read_text(encoding="utf-8"))
    for entry in manifest["shards"]:
        if entry["id"] == "matrix_silu_tile":
            entry["target_shape"] = [11, 9]
    swap_manifest = wb.paths.workdir / "swap_manifest.json"
    atomic_write_json(swap_manifest, manifest)
    wb.import_job(
        source_path="samples/matrix_silu_tile.npz",
        config_path="configs/silu_tile_swap.yaml",
        job_id="silu-tile",
        manifest_path=swap_manifest,
    )
    event = wb.invalidations("silu-tile")["events"][-1]
    run = engine.run_job("silu-tile")
    write_activation_unchanged = (
        activation_before == wb.paths.job("silu-tile").output_path("activation.npy").read_bytes()
    )
    cases.append(
        {
            "change": "reorder_mode: tile -> swap_head_seq",
            "earliest_stage": event["earliest_stage"],
            "affected_stages": event["affected_stages"],
            "unaffected_stages": event["unaffected_stages"],
            "reasons": event["reasons"],
            "reused_blocks": {stage: run["stages"][stage]["reused_blocks"] for stage in STAGES},
            "computed_blocks": {stage: run["stages"][stage]["computed_blocks"] for stage in STAGES},
            "upstream_bytes_unchanged": write_activation_unchanged,
        }
    )

    mutable = wb.paths.workdir / "mutable_matrix.npz"
    original = np.load(wb.root / "samples" / "matrix_silu_tile.npz")["tensor"].copy()
    np.savez(mutable, tensor=original)
    wb.import_job(
        source_path=mutable,
        config_path="configs/silu_tile.yaml",
        job_id="input-change",
        manifest_path=None,
    )
    engine.run_job("input-change")
    changed = original.copy()
    changed[0, 0] = 321.0
    np.savez(mutable, tensor=changed)
    wb.import_job(
        source_path=mutable,
        config_path="configs/silu_tile.yaml",
        job_id="input-change",
        manifest_path=None,
    )
    event = wb.invalidations("input-change")["events"][-1]
    run = engine.run_job("input-change")
    cases.append(
        {
            "change": "input tensor content byte",
            "earliest_stage": event["earliest_stage"],
            "affected_stages": event["affected_stages"],
            "unaffected_stages": event["unaffected_stages"],
            "reasons": event["reasons"],
            "reused_blocks": {stage: run["stages"][stage]["reused_blocks"] for stage in STAGES},
            "computed_blocks": {stage: run["stages"][stage]["computed_blocks"] for stage in STAGES},
            "upstream_bytes_unchanged": None,
        }
    )
    return cases


def _error_demo(root: Path, wb: Workbench, engine: ExecutionEngine) -> dict[str, Any]:
    errors: dict[str, Any] = {}
    wb.import_job(
        source_path="samples/matrix_silu_tile.npz",
        config_path="configs/silu_tile.yaml",
        job_id="error-invalid-stage",
    )
    try:
        engine.run_job("error-invalid-stage", stages=["reduce"])
    except BusinessError as error:
        errors[error.code] = {
            "message": error.message,
            "details": error.details,
        }

    wb.import_job(
        source_path="samples/matrix_silu_tile.npz",
        config_path="configs/silu_tile.yaml",
        job_id="error-shape",
    )
    paths = wb.paths.job("error-shape")
    job = json.loads(paths.job_file.read_text(encoding="utf-8"))
    job["declared_target_shape"] = [99, 99]
    atomic_write_json(paths.job_file, job)
    try:
        engine.run_job("error-shape")
    except BusinessError as error:
        errors[error.code] = {"message": error.message, "details": error.details}

    wb.import_job(
        source_path="samples/tensor_4d_gelu_diag.npz",
        config_path="configs/gelu_swap_diag.yaml",
        job_id="error-numeric",
    )
    numeric = engine.run_job("error-numeric")
    diagnostics = wb.diagnostics("error-numeric")
    errors[E_NUMERIC] = {
        "run_status": numeric["status"],
        "jobs_status_code": wb.job_status("error-numeric")["business_code"],
        "diagnostic_blocks": len(diagnostics["diagnostics"]),
        "sample_diagnostics": diagnostics["diagnostics"][:3],
    }

    wb.import_job(
        source_path="samples/matrix_silu_tile.npz",
        config_path="configs/silu_tile.yaml",
        job_id="error-stale",
    )
    state = wb.sync_job("error-stale")
    job_record, stale_paths, tensor, config, source_shape, _declared = wb.load_context("error-stale")
    schedule = activation_schedule(source_shape, config.row_block, config.col_block)
    try:
        publish_block(
            stale_paths,
            stage="activation",
            epoch=1,
            requested_sequence=1,
            plan_fingerprint=state["stage_plans"]["activation"]["fingerprint"],
            input_fingerprint=tensor.digest,
            config_version=state["stage_plans"]["activation"]["config_version"],
            workspace_config_version=config.version,
            schedule=schedule,
            schedule_item=schedule[1],
            artifact_bytes=npy_bytes(np.zeros((1, 1), dtype=np.float64)),
            product_kind="npy",
            status="ok",
            metadata={"shape": [1, 1], "status": "ok"},
        )
    except BusinessError as error:
        errors[error.code] = {"message": error.message, "details": error.details}

    expected = {E_INVALID_STAGE, E_SHAPE_MISMATCH, E_NUMERIC, E_STALE_CHECKPOINT}
    return {"codes": errors, "all_expected_codes_observed": expected.issubset(errors)}


def run_demo(*, root: Path, workdir: str | Path, report_path: Path) -> dict[str, Any]:
    root = root.resolve()
    demo_root = (root / workdir).resolve() if not Path(workdir).is_absolute() else Path(workdir).resolve()
    try:
        demo_root.relative_to(root)
    except ValueError as exc:
        raise ValueError("demo workdir must be inside the workspace root") from exc
    _fresh_directory(demo_root, root)
    wb = Workbench(root, demo_root / "workspace")
    engine = ExecutionEngine(wb)

    clean_runs = {
        "gelu-swap": _clean_run(wb, engine, "tensor_4d_gelu_swap", "gelu_swap", "gelu-swap"),
        "silu-tile": _clean_run(wb, engine, "matrix_silu_tile", "silu_tile", "silu-tile"),
        "sigmoid-tile": _clean_run(
            wb, engine, "tensor_4d_sigmoid_tile", "sigmoid_tile", "sigmoid-tile"
        ),
        "gelu-diag": _clean_run(wb, engine, "tensor_4d_gelu_diag", "gelu_swap_diag", "gelu-diag"),
    }
    recovery = _recovery_demo(root, demo_root / "recovery")
    invalidations = _invalidation_demo(wb, engine)
    errors = _error_demo(root, wb, engine)

    sample_manifest = json.loads((root / "samples" / "shape_manifest.json").read_text(encoding="utf-8"))
    clean_digests = {
        name: {
            "activation_sha256": item["activation_sha256"],
            "reordered_sha256": item["reordered_sha256"],
            "summary_sha256": item["summary_sha256"],
            "version_fingerprint": item["version_fingerprint"],
            "business_code": item["business_code"],
        }
        for name, item in clean_runs.items()
    }
    invalidation_digest = [
        {
            "change": item["change"],
            "earliest_stage": item["earliest_stage"],
            "affected_stages": item["affected_stages"],
            "reasons": item["reasons"],
            "reused_blocks": item["reused_blocks"],
        }
        for item in invalidations
    ]
    demo_digest = digest_json(
        {
            "schema": "blocklab.demo.v1",
            "seed": sample_manifest.get("seed"),
            "clean_runs": clean_digests,
            "recovery": {
                "outputs_byte_equal": recovery["outputs_byte_equal"],
                "checkpoint_manifest_equal": recovery["checkpoint_manifest_equal"],
                "checkpoint_records_byte_equal": recovery["checkpoint_records_byte_equal"],
                "sequences_contiguous": recovery["sequences_contiguous"],
                "full_summary_sha256": recovery["full_summary_sha256"],
                "resumed_summary_sha256": recovery["resumed_summary_sha256"],
            },
            "invalidations": invalidation_digest,
            "error_codes": sorted(errors["codes"]),
        }
    )
    passed = (
        all(item["complete"] for item in clean_runs.values())
        and recovery["passed"]
        and errors["all_expected_codes_observed"]
        and all(
            case["earliest_stage"]
            == {"sum_strategy: sum -> mean": "reduce", "reorder_mode: tile -> swap_head_seq": "reorder", "input tensor content byte": "activation"}[case["change"]]
            for case in invalidations
        )
    )
    report = {
        "schema": "blocklab.demo-report.v1",
        "ok": passed,
        "seed": sample_manifest.get("seed"),
        "sample_count": len(sample_manifest.get("shards", [])),
        "samples": [
            {
                "id": entry["id"],
                "shape": entry["shape"],
                "target_shape": entry["target_shape"],
                "layout": entry["layout"],
                "anomalies": entry["anomalies"],
            }
            for entry in sample_manifest.get("shards", [])
        ],
        "clean_runs": {
            name: {
                key: value
                for key, value in {
                    **item,
                    "summary": None,
                }.items()
                if key != "summary"
            }
            for name, item in clean_runs.items()
        },
        "recovery": recovery,
        "invalidations": invalidations,
        "business_errors": errors,
        "fixed_demo_digest": demo_digest,
        "passed": passed,
    }
    report_path = report_path.resolve()
    try:
        report_path.relative_to(root)
    except ValueError as exc:
        raise ValueError("report path must remain inside the workspace root") from exc
    atomic_write_json(report_path, report)
    return {
        "ok": passed,
        "report_path": report_path.relative_to(root).as_posix(),
        "fixed_demo_digest": demo_digest,
        "clean_summaries": {
            name: item["summary_sha256"] for name, item in clean_runs.items()
        },
        "recovery": {
            "passed": recovery["passed"],
            "outputs_byte_equal": recovery["outputs_byte_equal"],
            "checkpoint_manifest_equal": recovery["checkpoint_manifest_equal"],
            "checkpoint_records_byte_equal": recovery["checkpoint_records_byte_equal"],
        },
        "invalidation_scopes": {
            item["change"]: item["earliest_stage"] for item in invalidations
        },
        "business_codes": sorted(errors["codes"]),
    }

