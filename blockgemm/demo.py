"""End-to-end deterministic local demonstration."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np

from .checkpoint import CheckpointStore, atomic_write_json
from .engine import describe_execution, execute_problem
from .manifest import ensure_samples, generate_samples, load_case, load_manifest


ROOT = Path(__file__).resolve().parent.parent


def _effective(a: np.ndarray, b: np.ndarray, a_transpose: bool, b_transpose: bool) -> tuple[np.ndarray, np.ndarray]:
    a_eff = np.swapaxes(a, -1, -2) if a_transpose else a
    b_eff = np.swapaxes(b, -1, -2) if b_transpose else b
    return a_eff, b_eff


def _reference_check(loaded: Any) -> dict[str, Any]:
    a_eff, b_eff = _effective(
        loaded.a,
        loaded.b,
        bool(loaded.entry["a_transpose"]),
        bool(loaded.entry["b_transpose"]),
    )
    with np.errstate(invalid="ignore", over="ignore"):
        reference = np.matmul(a_eff, b_eff)
    return {"reference": reference}


def _compare_output(output: np.ndarray, loaded: Any) -> dict[str, Any]:
    reference = _reference_check(loaded)["reference"]
    if output.shape != reference.shape:
        return {"ok": False, "reason": "shape_mismatch", "expected": list(reference.shape), "actual": list(output.shape)}
    finite_mask = np.isfinite(reference) & np.isfinite(output)
    if np.any(finite_mask):
        max_abs = float(np.max(np.abs(output[finite_mask] - reference[finite_mask])))
    else:
        max_abs = 0.0
    nonfinite_equal = bool(np.array_equal(np.isnan(output), np.isnan(reference))) and bool(
        np.array_equal(np.isposinf(output), np.isposinf(reference))
    ) and bool(np.array_equal(np.isneginf(output), np.isneginf(reference)))
    ok = nonfinite_equal and np.allclose(output, reference, rtol=1e-10, atol=1e-10, equal_nan=True)
    return {
        "ok": bool(ok),
        "max_abs_error_vs_numpy": max_abs,
        "nonfinite_pattern_equal": nonfinite_equal,
    }


def _clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _checkpoint_product_digests(workspace: Path) -> list[str]:
    digests = []
    for path in sorted((workspace / "checkpoints").glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        digests.append(record["product_digest"])
    return digests


def _files_byte_identical(first_dir: Path, second_dir: Path, pattern: str) -> bool:
    first_files = sorted(first_dir.glob(pattern))
    second_files = sorted(second_dir.glob(pattern))
    if [path.name for path in first_files] != [path.name for path in second_files]:
        return False
    return all(a.read_bytes() == b.read_bytes() for a, b in zip(first_files, second_files))


def _run_subprocess(args: list[str]) -> tuple[int, dict[str, Any], str]:
    completed = subprocess.run(
        [sys.executable, "-m", "blockgemm", *args],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    stdout = completed.stdout.strip()
    try:
        payload = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError:
        payload = {}
    return completed.returncode, payload, completed.stderr.strip()


def _sample_manifest_digest(manifest_path: Path) -> str:
    return hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def _run_full_cases(manifest_path: Path, demo_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    manifest = load_manifest(manifest_path)
    reports = []
    manifest_cases = {entry["id"]: entry for entry in manifest["cases"]}
    for case_id, entry in manifest_cases.items():
        loaded = load_case(manifest_path, case_id)
        workspace = demo_root / "cases" / case_id
        _clean_dir(workspace)
        result = execute_problem(
            workspace=workspace,
            a=loaded.a,
            b=loaded.b,
            a_transpose=bool(entry["a_transpose"]),
            b_transpose=bool(entry["b_transpose"]),
            expected=loaded.expected,
            verify_blocks=3,
            case_id=case_id,
            source={
                "manifest_case": case_id,
                "a_file": str(loaded.a_path),
                "b_file": str(loaded.b_path),
            },
        )
        assert result.output is not None and result.summary is not None
        comparison = _compare_output(result.output, loaded)
        if not comparison["ok"]:
            raise RuntimeError(f"reference mismatch for {case_id}: {comparison}")
        reports.append(
            {
                "case": case_id,
                "kind": entry["kind"],
                "tags": entry["tags"],
                "shape": {
                    "M": entry["M"],
                    "N": entry["N"],
                    "K": entry["K"],
                    "batch": entry["batch"],
                },
                "a_transpose": entry["a_transpose"],
                "b_transpose": entry["b_transpose"],
                "block_count": result.summary["block_count"],
                "plan": result.summary["plan"],
                "plan_fingerprint": result.summary["plan_fingerprint"],
                "version_fingerprint": result.summary["version_fingerprint"],
                "output_digest": result.summary["output"]["array_payload_digest"],
                "output_file_sha256": result.summary["output"]["file_sha256"],
                "numeric_verified_blocks": result.summary["numeric_verification"]["sampled_sequences"],
                "nonfinite_blocks": result.summary["nonfinite_blocks"],
                "numpy_reference": comparison,
            }
        )
    return reports, manifest


def _stop_resume_demo(manifest_path: Path, demo_root: Path) -> dict[str, Any]:
    baseline_workspace = demo_root / "resume_baseline"
    interrupted_workspace = demo_root / "resume_interrupted"
    _clean_dir(baseline_workspace)
    _clean_dir(interrupted_workspace)

    common = [
        "--manifest",
        str(manifest_path),
        "--case",
        "medium_gemm",
    ]
    baseline_code, baseline_payload, baseline_stderr = _run_subprocess(
        ["run", *common, "--workspace", str(baseline_workspace)]
    )
    if baseline_code != 0 or not baseline_payload.get("completed"):
        raise RuntimeError(
            f"baseline run failed: code={baseline_code} stderr={baseline_stderr} payload={baseline_payload}"
        )

    stop_code, stop_payload, stop_stderr = _run_subprocess(
        [
            "run",
            *common,
            "--workspace",
            str(interrupted_workspace),
            "--stop-after-blocks",
            "1",
        ]
    )
    if stop_code != 0 or not stop_payload.get("stopped"):
        raise RuntimeError(
            f"deterministic stop run failed: code={stop_code} stderr={stop_stderr} payload={stop_payload}"
        )
    stopped_prefix = int(stop_payload.get("next_sequence", 0))
    if stopped_prefix <= 0:
        raise RuntimeError("stop demonstration published no complete blocks")
    stop_status = CheckpointStore(interrupted_workspace).status()

    resume_code, resume_payload, resume_stderr = _run_subprocess(
        ["resume", *common, "--workspace", str(interrupted_workspace)]
    )
    if resume_code != 0 or not resume_payload.get("completed"):
        raise RuntimeError(
            f"resume run failed: code={resume_code} stderr={resume_stderr} payload={resume_payload}"
        )

    baseline_output = baseline_workspace / "output" / "result.npy"
    resumed_output = interrupted_workspace / "output" / "result.npy"
    baseline_summary = baseline_workspace / "reports" / "summary.json"
    resumed_summary = interrupted_workspace / "reports" / "summary.json"
    output_bytes_equal = baseline_output.read_bytes() == resumed_output.read_bytes()
    summary_bytes_equal = baseline_summary.read_bytes() == resumed_summary.read_bytes()
    block_digests_equal = _checkpoint_product_digests(baseline_workspace) == _checkpoint_product_digests(
        interrupted_workspace
    )
    record_bytes_equal = _files_byte_identical(
        baseline_workspace / "checkpoints", interrupted_workspace / "checkpoints", "*.json"
    )
    artifact_bytes_equal = _files_byte_identical(
        baseline_workspace / "blocks", interrupted_workspace / "blocks", "*.npy"
    )
    if not (output_bytes_equal and summary_bytes_equal and block_digests_equal and record_bytes_equal and artifact_bytes_equal):
        raise RuntimeError(
            "stop/resume byte comparison failed: "
            f"output={output_bytes_equal} summary={summary_bytes_equal} blocks={block_digests_equal} "
            f"records={record_bytes_equal} artifacts={artifact_bytes_equal}"
        )
    return {
        "case": "medium_gemm",
        "stop_after_published_blocks": 1,
        "checkpoint_prefix_at_stop": stopped_prefix,
        "checkpoints_at_stop": stop_status["valid_prefix"],
        "output_existed_at_stop": stop_status["output_available"],
        "stop_marker_present_at_stop": stop_status["stop_requested"],
        "resume_decision": resume_payload.get("resume_decision"),
        "resumed_computed_blocks": resume_payload.get("computed_blocks"),
        "resumed_reused_blocks": resume_payload.get("reused_blocks"),
        "output_bytes_identical": output_bytes_equal,
        "summary_bytes_identical": summary_bytes_equal,
        "block_product_digests_identical": block_digests_equal,
        "checkpoint_record_bytes_identical": record_bytes_equal,
        "block_artifact_bytes_identical": artifact_bytes_equal,
        "baseline_output_sha256": hashlib.sha256(baseline_output.read_bytes()).hexdigest(),
        "resumed_output_sha256": hashlib.sha256(resumed_output.read_bytes()).hexdigest(),
        "baseline_summary_sha256": hashlib.sha256(baseline_summary.read_bytes()).hexdigest(),
        "resumed_summary_sha256": hashlib.sha256(resumed_summary.read_bytes()).hexdigest(),
    }


def _choose_local_change(descriptors: list[Any]) -> tuple[str, int, int]:
    first = descriptors[0]
    for descriptor in descriptors[1:]:
        if descriptor.col_range != first.col_range:
            return "B", 0, descriptor.col_start
        if descriptor.row_range != first.row_range:
            return "A", descriptor.row_start, 0
    raise RuntimeError("selected plan produced only one logical input tile")


def _invalidation_demo(manifest_path: Path, demo_root: Path) -> dict[str, Any]:
    loaded = load_case(manifest_path, "small_gemm")
    l2_budget = 32
    workspace = demo_root / "invalidation_reuse"
    fresh_workspace = demo_root / "invalidation_fresh"
    _clean_dir(workspace)
    _clean_dir(fresh_workspace)
    first = execute_problem(
        workspace=workspace,
        a=loaded.a,
        b=loaded.b,
        a_transpose=False,
        b_transpose=False,
        expected=loaded.expected,
        l2_cache_kib=l2_budget,
        verify_blocks=3,
        case_id="small_gemm",
    )
    assert first.completed
    context, _decision = describe_execution(
        workspace=workspace,
        a=loaded.a,
        b=loaded.b,
        a_transpose=False,
        b_transpose=False,
        expected=loaded.expected,
        l2_cache_kib=l2_budget,
        verify_blocks=3,
    )
    if len(context.descriptors) < 2:
        raise RuntimeError("invalidation demo needs at least two output blocks")
    side, row, col = _choose_local_change(context.descriptors)
    a_modified = loaded.a.copy()
    b_modified = loaded.b.copy()
    if side == "A":
        a_modified[row, 0] = a_modified[row, 0] + np.float64(0.125)
    else:
        b_modified[0, col] = b_modified[0, col] + np.float64(0.125)

    changed = execute_problem(
        workspace=workspace,
        a=a_modified,
        b=b_modified,
        a_transpose=False,
        b_transpose=False,
        expected=loaded.expected,
        l2_cache_kib=l2_budget,
        verify_blocks=3,
        case_id="small_gemm",
    )
    fresh = execute_problem(
        workspace=fresh_workspace,
        a=a_modified,
        b=b_modified,
        a_transpose=False,
        b_transpose=False,
        expected=loaded.expected,
        l2_cache_kib=l2_budget,
        verify_blocks=3,
        case_id="small_gemm",
    )
    if changed.resume_decision.reason != "input_content_changed":
        raise RuntimeError(f"unexpected invalidation reason: {changed.resume_decision.to_dict()}")
    if not (0 < changed.resume_decision.start_sequence < changed.resume_decision.scan.valid_prefix + 1):
        raise RuntimeError(f"invalidation did not preserve an earlier prefix: {changed.resume_decision.to_dict()}")
    if changed.output_path is None or fresh.output_path is None:
        raise RuntimeError("invalidation demo did not produce outputs")
    output_equal = changed.output_path.read_bytes() == fresh.output_path.read_bytes()
    summary_equal = (
        changed.summary_path is not None
        and fresh.summary_path is not None
        and changed.summary_path.read_bytes() == fresh.summary_path.read_bytes()
    )
    if not (output_equal and summary_equal):
        raise RuntimeError(
            f"reused-block rebuild differs from scratch: output={output_equal} summary={summary_equal}"
        )
    report = CheckpointStore(workspace).read_invalidation()
    return {
        "case": "small_gemm",
        "l2_cache_kib": l2_budget,
        "changed_side": side,
        "changed_index": [row, col],
        "total_blocks": len(context.descriptors),
        "reused_blocks": changed.resume_decision.reused_blocks,
        "earliest_affected_sequence": changed.resume_decision.start_sequence,
        "reason": changed.resume_decision.reason,
        "earliest_affected_block": changed.resume_decision.details.get("earliest_affected_block"),
        "a_stage_changed": changed.resume_decision.details.get("a_stage_changed"),
        "b_stage_changed": changed.resume_decision.details.get("b_stage_changed"),
        "invalidation_report": report,
        "output_bytes_match_fresh_full_build": output_equal,
        "summary_bytes_match_fresh_full_build": summary_equal,
    }


def run_demo(
    *,
    manifest_path: str | Path = "samples/manifest.json",
    output_dir: str | Path = "runs/demo",
    force_samples: bool = False,
) -> dict[str, Any]:
    manifest_path = Path(manifest_path)
    demo_root = Path(output_dir)
    if force_samples or not manifest_path.is_file():
        generate_samples(manifest_path.parent, force=force_samples)
    ensure_samples(manifest_path)
    demo_root.mkdir(parents=True, exist_ok=True)

    case_reports, manifest = _run_full_cases(manifest_path, demo_root)
    stop_resume = _stop_resume_demo(manifest_path, demo_root)
    invalidation = _invalidation_demo(manifest_path, demo_root)
    demo_summary = {
        "schema_version": 1,
        "status": "ok",
        "sample_manifest": str(manifest_path.as_posix()),
        "sample_manifest_sha256": _sample_manifest_digest(manifest_path),
        "sample_count": len(manifest["cases"]),
        "cases": case_reports,
        "stop_resume": stop_resume,
        "invalidation_reuse": invalidation,
        "acceptance": {
            "all_cases_match_numpy_reference": all(report["numpy_reference"]["ok"] for report in case_reports),
            "stop_resume_output_byte_identical": stop_resume["output_bytes_identical"],
            "stop_resume_summary_byte_identical": stop_resume["summary_bytes_identical"],
            "stop_resume_checkpoint_records_byte_identical": stop_resume["checkpoint_record_bytes_identical"],
            "stop_resume_block_artifacts_byte_identical": stop_resume["block_artifact_bytes_identical"],
            "invalidation_matches_fresh_build": invalidation["output_bytes_match_fresh_full_build"]
            and invalidation["summary_bytes_match_fresh_full_build"],
        },
    }
    summary_path = demo_root / "demo-summary.json"
    atomic_write_json(summary_path, demo_summary)
    return {
        "summary_path": str(summary_path),
        "demo_summary_sha256": hashlib.sha256(summary_path.read_bytes()).hexdigest(),
        "acceptance": demo_summary["acceptance"],
        "cases": [
            {
                "case": report["case"],
                "block_count": report["block_count"],
                "plan": report["plan"],
                "output_digest": report["output_digest"],
                "reference_ok": report["numpy_reference"]["ok"],
            }
            for report in case_reports
        ],
        "stop_resume": {
            "prefix_at_stop": stop_resume["checkpoint_prefix_at_stop"],
            "computed_after_resume": stop_resume["resumed_computed_blocks"],
            "reused_after_resume": stop_resume["resumed_reused_blocks"],
            "output_bytes_identical": stop_resume["output_bytes_identical"],
            "summary_bytes_identical": stop_resume["summary_bytes_identical"],
        },
        "invalidation": invalidation,
    }
