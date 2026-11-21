"""Deterministic end-to-end demonstration and verification report."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

import numpy as np

from .data import generate_datasets
from .fingerprint import sha256_file
from .http_server import create_server
from .pipeline import Pipeline, import_fragment, load_output
from .storage import atomic_write_json
from .strategies import _tier_caps


def _post(url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = json.dumps(payload or {}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.loads(response.read().decode("utf-8"))


def _get(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=300) as response:
        return json.loads(response.read().decode("utf-8"))


def _fresh_workspace(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _run_one(
    *,
    workspace: Path,
    manifest: dict[str, Any],
    fragment_id: str,
    op: str,
    axis: int,
    overwrite: bool,
) -> dict[str, Any]:
    record = next(item for item in manifest["fragments"] if item["fragment_id"] == fragment_id)
    if workspace.exists():
        if overwrite:
            shutil.rmtree(workspace)
        else:
            try:
                existing = Pipeline(workspace)
                if existing.status()["state"] == "complete":
                    summary = existing.summary()
                    report = existing.error_report()
                    return _run_record(workspace, summary, report, tier=record["tier"])
            except Exception:
                pass
            raise RuntimeError(
                f"demo workspace exists and is not complete: {workspace}; pass --overwrite"
            )
    import_fragment(
        workspace,
        record["path"],
        op=op,
        axis=axis,
        fragment_id=fragment_id,
        cache_budget_bytes=4 * 1024 * 1024,
        memory_budget_bytes=512 * 1024 * 1024,
        tolerance={"atol": 1e-4, "rtol": 1e-4},
    )
    pipeline = Pipeline(workspace)
    started = time.perf_counter()
    status = pipeline.run()
    elapsed = time.perf_counter() - started
    if status["state"] != "complete":
        raise RuntimeError(f"pipeline did not complete: {status['state']}")
    summary = pipeline.summary()
    report = pipeline.error_report()
    result = _run_record(workspace, summary, report, tier=record["tier"])
    result["wall_seconds"] = elapsed
    return result


def _run_record(
    workspace: Path,
    summary: dict[str, Any],
    report: dict[str, Any],
    *,
    tier: str | None,
) -> dict[str, Any]:
    plan = summary["plan"]
    finite_sample_blocks = [
        block for block in report["sample_blocks"] if block["within_tolerance"]
    ]
    return {
        "workspace": str(workspace),
        "fragment_id": summary["fragment"]["fragment_id"],
        "tier": tier or summary["fragment"].get("tier"),
        "op": summary["operation"]["op"],
        "axis": summary["operation"]["axis"],
        "shape": summary["fragment"]["shape"],
        "anomaly_fragment": summary["fragment"].get("anomaly_fragment", False),
        "plan": {
            "row_block": plan["row_block"],
            "col_block": plan["col_block"],
            "sum_strategy": plan["sum_strategy"],
            "reason": plan["reason"],
            "score": plan["score"],
            "candidate_count": plan["candidate_count"],
            "rejected_candidate_count": len(plan["rejected_candidates"]),
            "predicted_error_bound": plan["predicted_error_bound"],
            "workspace_bytes": plan["workspace_bytes"],
        },
        "output": summary["result"]["output"],
        "output_digest": summary["result"]["output_digest"],
        "cache": summary["cache"],
        "validation": {
            "status": report["status"],
            "sampled_block_count": report["sampled_block_count"],
            "failed_block_count": len(report["failed_blocks"]),
            "sampled_blocks_within_tolerance": len(finite_sample_blocks),
            "sampled_bounds_cover_observed": all(
                block.get("bound_covers_observed") is True
                for block in report["sample_blocks"]
            ),
            "error_estimate_count": len(report["error_estimates"]),
            "tolerance": report["tolerance"],
            "input_nonfinite": report["input_nonfinite"],
            "zero_rows": report["all_zero_rows"],
            "first_error_estimate": report["error_estimates"][0] if report["error_estimates"] else None,
        },
        "versions": {
            "plans": summary["versions"]["plans"],
            "results": summary["versions"]["results"],
        },
        "stage_chain": summary["stage_chain"],
    }


def _check(name: str, passed: bool, details: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "details": details}


def _http_smoke(
    *,
    root: Path,
    manifest: dict[str, Any],
    baseline_output_sha: str,
) -> dict[str, Any]:
    workspace = root / "http_smoke_workspace"
    _fresh_workspace(workspace)
    small = next(item for item in manifest["fragments"] if item["fragment_id"] == "small_base")
    server = create_server("127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"
    try:
        health = _get(base + "/health")
        imported = _post(
            base + "/api/fragments/import",
            {
                "workspace": str(workspace),
                "fragment": small["path"],
                "fragment_id": "small_base",
                "op": "softmax",
                "axis": 1,
                "cache_budget_bytes": 4 * 1024 * 1024,
                "memory_budget_bytes": 512 * 1024 * 1024,
            },
        )
        planned = _post(
            base + "/api/runs/stage", {"workspace": str(workspace), "stage": "plan"}
        )
        reported = _post(
            base + "/api/runs/run", {"workspace": str(workspace), "until_stage": "reduce"}
        )
        stop = _post(
            base + "/api/runs/stop",
            {"workspace": str(workspace), "reason": "http_demo_stage_boundary"},
        )
        status_stopped = _get(base + "/api/runs/status?workspace=" + str(workspace))
        resumed = _post(base + "/api/runs/resume", {"workspace": str(workspace)})
        summary = _get(base + "/api/runs/summary?workspace=" + str(workspace))
        plan_view = _get(base + "/api/runs/plan?workspace=" + str(workspace))
        versions = _get(base + "/api/runs/versions?workspace=" + str(workspace))
        report = _get(base + "/api/runs/errors?workspace=" + str(workspace))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    output_path = workspace / "outputs" / "output.npy"
    resumed_sha = sha256_file(output_path)
    return {
        "health": health,
        "import_code": imported.get("code"),
        "state_after_plan": planned["result"]["state"],
        "state_after_reduce": reported["result"]["state"],
        "stop_requested": stop["result"]["status"]["stop_requested"],
        "state_while_stopped": status_stopped["result"]["state"],
        "resumed_state": resumed["result"]["state"],
        "summary_status": report["result"]["status"],
        "summary_validation_status": summary["result"]["validation"]["report"]["status"]
        if isinstance(summary["result"]["validation"]["report"].get("status"), str)
        else report["result"]["status"],
        "plan_endpoint_rows": plan_view["result"]["plan"]["plan"]["rows"],
        "version_kinds": sorted(versions["result"].keys()),
        "error_report_status": report["result"]["status"],
        "output_sha256": resumed_sha,
        "matches_baseline": resumed_sha == baseline_output_sha,
        "resume_stage_observed": status_stopped["result"]["next_stage"],
    }


def _cli_smoke(workspace: Path) -> dict[str, Any]:
    command = [sys.executable, "-m", "chunkops", "--compact", "status", "--workspace", str(workspace)]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
    payload = json.loads(completed.stdout) if completed.stdout.strip() else None
    return {
        "command": command,
        "returncode": completed.returncode,
        "state": payload.get("state") if payload else None,
        "stderr": completed.stderr,
    }


def run_demo(
    root: str = ".",
    *,
    include_large: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    data_root = root_path / "data"
    artifacts = root_path / "artifacts"
    run_root = artifacts / "demo_runs"
    artifacts.mkdir(parents=True, exist_ok=True)
    run_root.mkdir(parents=True, exist_ok=True)
    manifest = generate_datasets(data_root, include_large=include_large, overwrite=overwrite)

    specs: list[tuple[str, str, int]] = []
    tiers = ["small", "medium"] + (["large"] if include_large else [])
    for tier in tiers:
        specs.append((f"{tier}_base", "softmax", 1))
        specs.append((f"{tier}_base", "layernorm", 0))
    specs.extend(
        [
            ("small_anomaly", "softmax", 1),
            ("small_anomaly", "layernorm", 0),
        ]
    )

    runs: list[dict[str, Any]] = []
    for fragment_id, op, axis in specs:
        workspace = run_root / f"{fragment_id}_{op}_axis{axis}"
        record = _run_one(
            workspace=workspace,
            manifest=manifest,
            fragment_id=fragment_id,
            op=op,
            axis=axis,
            overwrite=overwrite,
        )
        runs.append(record)

    baseline = next(
        run
        for run in runs
        if run["fragment_id"] == "small_base" and run["op"] == "softmax"
    )
    baseline_sha = sha256_file(Path(baseline["workspace"]) / "outputs" / "output.npy")

    recovery_workspace = run_root / "small_base_softmax_recovery"
    _fresh_workspace(recovery_workspace)
    small = next(item for item in manifest["fragments"] if item["fragment_id"] == "small_base")
    import_fragment(
        recovery_workspace,
        small["path"],
        op="softmax",
        axis=1,
        fragment_id="small_base",
        cache_budget_bytes=4 * 1024 * 1024,
        memory_budget_bytes=512 * 1024 * 1024,
    )
    recovery = Pipeline(recovery_workspace)
    stopped = recovery.run(stop_after_stage="plan")
    stopped_checkpoint = stopped["latest_stage"]
    stopped_next = stopped["next_stage"]
    stop_state = stopped["state"]
    resumed = recovery.resume()
    recovery_sha = sha256_file(recovery_workspace / "outputs" / "output.npy")
    recovery_summary = recovery.summary()

    http_smoke = _http_smoke(
        root=artifacts,
        manifest=manifest,
        baseline_output_sha=baseline_sha,
    )
    cli_smoke = _cli_smoke(Path(baseline["workspace"]))

    plan_rows = {
        run["fragment_id"] + ":" + run["op"]: {
            "row_block": run["plan"]["row_block"],
            "col_block": run["plan"]["col_block"],
            "sum_strategy": run["plan"]["sum_strategy"],
        }
        for run in runs
    }
    base_plans = {
        key: value for key, value in plan_rows.items() if key.endswith("_base:softmax")
    }
    plan_signatures = {
        (value["row_block"], value["col_block"], value["sum_strategy"])
        for value in base_plans.values()
    }
    small_plan = next(
        run["plan"]
        for run in runs
        if run["fragment_id"] == "small_base" and run["op"] == "softmax"
    )
    small_cap_row, small_cap_col = _tier_caps(128, 512)
    anomaly_run = next(
        run
        for run in runs
        if run["fragment_id"] == "small_anomaly" and run["op"] == "softmax"
    )
    anomaly_nonfinite = anomaly_run["validation"]["input_nonfinite"]
    manifest_anomaly = next(
        item for item in manifest["fragments"] if item["fragment_id"] == "small_anomaly"
    )
    all_reports_ok = all(run["validation"]["status"] == "ok" for run in runs)
    all_error_estimates = all(run["validation"]["error_estimate_count"] > 0 for run in runs)
    all_bounds_cover_observed = all(
        run["validation"]["sampled_bounds_cover_observed"] for run in runs
    )
    all_checkpointed = all(len(run["stage_chain"]) == 4 for run in runs)
    output_shapes_match = all(run["output"]["size_bytes"] > 0 for run in runs)
    zero_rows_present = bool(anomaly_run["validation"]["zero_rows"])
    nan_present = anomaly_nonfinite["nan_count"] > 0
    inf_present = (
        anomaly_nonfinite["pos_inf_count"] > 0
        and anomaly_nonfinite["neg_inf_count"] > 0
    )
    first_nan_matches = (
        anomaly_nonfinite["first_nan"]["position"]
        == manifest_anomaly["nonfinite"]["first_nan"]["position"]
    )
    axes_covered = {run["axis"] for run in runs} == {0, 1}
    tiers_covered = {run["tier"] for run in runs} >= set(tiers)

    checks = [
        _check(
            "three_tiers_covered",
            tiers_covered,
            sorted({run["tier"] for run in runs}),
        ),
        _check("two_reduction_axes_covered", axes_covered, sorted({run["axis"] for run in runs})),
        _check("all_validation_reports_ok", all_reports_ok, len(runs)),
        _check("all_checkpoint_chains_complete", all_checkpointed, [len(r["stage_chain"]) for r in runs]),
        _check(
            "error_estimates_present_for_every_output_block",
            all_error_estimates,
            {run["fragment_id"] + ":" + run["op"]: run["validation"]["error_estimate_count"] for run in runs},
        ),
        _check(
            "sampled_error_bounds_cover_observed_deviations",
            all_bounds_cover_observed,
            {
                run["fragment_id"] + ":" + run["op"]: run["validation"][
                    "sampled_bounds_cover_observed"
                ]
                for run in runs
            },
        ),
        _check(
            "plan_changes_with_scale",
            len(plan_signatures) >= 2,
            plan_rows,
        ),
        _check(
            "small_plan_avoids_large_blocks",
            small_plan["row_block"] <= small_cap_row and small_plan["col_block"] <= small_cap_col,
            {"plan": small_plan, "caps": {"row": small_cap_row, "col": small_cap_col}},
        ),
        _check(
            "plan_rejection_trail_present",
            all(run["plan"]["rejected_candidate_count"] > 0 for run in runs),
            {run["fragment_id"] + ":" + run["op"]: run["plan"]["rejected_candidate_count"] for run in runs},
        ),
        _check(
            "zero_rows_nan_inf_present_and_nonfinite_propagated",
            zero_rows_present and nan_present and inf_present and first_nan_matches,
            {
                "zero_rows": anomaly_run["validation"]["zero_rows"],
                "nonfinite": anomaly_nonfinite,
                "manifest_first_nan": manifest_anomaly["nonfinite"]["first_nan"],
            },
        ),
        _check(
            "stop_after_plan_then_resume",
            stop_state == "stopped"
            and stopped_checkpoint == "plan"
            and stopped_next == "reduce"
            and resumed["state"] == "complete",
            {
                "state_after_stop": stop_state,
                "latest_checkpoint": stopped_checkpoint,
                "next_stage": stopped_next,
                "state_after_resume": resumed["state"],
            },
        ),
        _check(
            "resume_output_byte_identical_to_full_run",
            recovery_sha == baseline_sha,
            {
                "full_sha256": baseline_sha,
                "recovered_sha256": recovery_sha,
                "recovery_checkpoint_chain": recovery_summary["stage_chain"],
            },
        ),
        _check(
            "http_import_stage_status_stop_resume_export",
            http_smoke["matches_baseline"]
            and http_smoke["state_after_plan"] == "planned"
            and http_smoke["state_after_reduce"] == "reduced"
            and http_smoke["state_while_stopped"] == "stopped"
            and http_smoke["resumed_state"] == "complete"
            and http_smoke["error_report_status"] == "ok",
            http_smoke,
        ),
        _check(
            "cli_status_integration",
            cli_smoke["returncode"] == 0 and cli_smoke["state"] == "complete",
            cli_smoke,
        ),
        _check(
            "output_layout_available",
            output_shapes_match,
            {run["fragment_id"] + ":" + run["op"]: run["output"] for run in runs},
        ),
    ]

    verification = {
        "schema_version": 1,
        "status": "passed" if all(item["passed"] for item in checks) else "failed",
        "environment": {
            "python": sys.version.split()[0],
            "numpy": np.__version__,
            "root": str(root_path),
        },
        "dataset": {
            "root": str(data_root),
            "manifest": str(data_root / "manifest.json"),
            "fragment_count": len(manifest["fragments"]),
            "fragment_ids": [item["fragment_id"] for item in manifest["fragments"]],
            "coverage": manifest["coverage"],
        },
        "runs": runs,
        "plan_progression": plan_rows,
        "recovery": {
            "stopped_state": stopped,
            "resumed_state": resumed,
            "full_output_sha256": baseline_sha,
            "recovered_output_sha256": recovery_sha,
            "byte_identical": recovery_sha == baseline_sha,
        },
        "http_smoke": http_smoke,
        "cli_smoke": cli_smoke,
        "checks": checks,
    }
    report_path = artifacts / "verification.json"
    atomic_write_json(report_path, verification)
    markdown = _render_markdown(verification)
    (artifacts / "verification.md").write_text(markdown, encoding="utf-8")
    verification["verification_report"] = str(report_path)
    verification["markdown_report"] = str(artifacts / "verification.md")
    return verification


def _render_markdown(verification: dict[str, Any]) -> str:
    lines = [
        "# ChunkOps 本地验证报告",
        "",
        f"- 总体状态: **{verification['status']}**",
        f"- Python: `{verification['environment']['python']}`",
        f"- NumPy: `{verification['environment']['numpy']}`",
        f"- 分片数量: {verification['dataset']['fragment_count']}",
        "",
        "## 自动分块结果",
        "",
        "| 分片 | 算子 | 轴 | 行块 | 列块 | 求和策略 | 验证块 | 误差估计数 | 输出 SHA256 |",
        "|---|---|---:|---:|---:|---|---:|---:|---|",
    ]
    for run in verification["runs"]:
        lines.append(
            "| {fragment} | {op} | {axis} | {row_block} | {col_block} | {strategy} | "
            "{samples} | {estimates} | `{digest}` |".format(
                fragment=run["fragment_id"],
                op=run["op"],
                axis=run["axis"],
                row_block=run["plan"]["row_block"],
                col_block=run["plan"]["col_block"],
                strategy=run["plan"]["sum_strategy"],
                samples=run["validation"]["sampled_block_count"],
                estimates=run["validation"]["error_estimate_count"],
                digest=run["output_digest"][:20] + "…",
            )
        )
    lines.extend(["", "## 验收检查", ""])
    for check in verification["checks"]:
        marker = "PASS" if check["passed"] else "FAIL"
        lines.append(f"- [{marker}] `{check['name']}`")
    lines.extend(
        [
            "",
            "## 停止与恢复",
            "",
            f"- 停止状态: `{verification['recovery']['stopped_state']['state']}`",
            f"- 最近检查点: `{verification['recovery']['stopped_state']['latest_stage']}`",
            f"- 恢复后状态: `{verification['recovery']['resumed_state']['state']}`",
            f"- 全量输出 SHA256: `{verification['recovery']['full_output_sha256']}`",
            f"- 恢复输出 SHA256: `{verification['recovery']['recovered_output_sha256']}`",
            f"- 逐字节一致: **{verification['recovery']['byte_identical']}**",
            "",
            "## 本地 HTTP",
            "",
            f"- 健康检查: `{verification['http_smoke']['health']['result']['status']}`",
            f"- 阶段状态: plan=`{verification['http_smoke']['state_after_plan']}`, "
            f"reduce=`{verification['http_smoke']['state_after_reduce']}`, "
            f"stopped=`{verification['http_smoke']['state_while_stopped']}`",
            f"- 恢复后: `{verification['http_smoke']['resumed_state']}`",
            f"- HTTP 输出与全量基线一致: **{verification['http_smoke']['matches_baseline']}**",
            "",
        ]
    )
    return "\n".join(lines)
