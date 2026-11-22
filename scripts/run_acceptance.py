"""Run the local acceptance plan over HTTP and persist evidence artifacts."""

from __future__ import annotations

import json
import platform
import sys
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capacity_lab.deterministic_service import start_server
from capacity_lab.server import CapacityApplication, CapacityHttpServer
from tests.support import http_call, load_fixture


def main() -> int:
    output_dir = ROOT / "artifacts"
    output_dir.mkdir(parents=True, exist_ok=True)
    service, _service_thread = start_server(port=0)
    tempdir = tempfile.TemporaryDirectory(prefix="capacity-acceptance-")
    application = CapacityApplication(Path(tempdir.name) / "acceptance.db")
    server = CapacityHttpServer(("127.0.0.1", 0), application)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        plan = load_fixture("sample_plan.json")
        plan["target"] = f"http://127.0.0.1:{service.server_port}/load"
        status, submitted = http_call(
            base_url, "PUT", "/api/plans/capacity-demo", plan
        )
        _require(status == 200, submitted)
        status, run_payload = http_call(
            base_url,
            "POST",
            "/api/plans/capacity-demo/runs",
            {"async": False},
            timeout=60,
        )
        _require(status == 200, run_payload)
        run = run_payload["data"]
        status, stats_payload = http_call(base_url, "GET", "/api/recovery/latest")
        _require(status == 200, stats_payload)
        startup_recovery = stats_payload["data"]
        service_stats = _service_stats(service.server_port)
        concurrency_checks = []
        for round_data in run["rounds"]:
            stream = (
                f"plan:capacity-demo:v{run['plan_version']}:{round_data['stage_key']}:"
                f"c{round_data['concurrency']}"
            )
            observed = service_stats["by_stream"].get(stream, {}).get("max_active", 0)
            concurrency_checks.append(
                {
                    "round_id": round_data["round_id"],
                    "concurrency": round_data["concurrency"],
                    "observed_service_peak": observed,
                    "within_limit": observed <= round_data["concurrency"],
                }
            )

        rejected = next(
            round_data
            for round_data in run["rounds"]
            if round_data["conclusion"]["verdict"] == "REJECT"
        )
        rejected_conclusion = rejected["conclusion"]
        boundary = rejected_conclusion["threshold_checks"]["p99_latency_ms_max"][
            "pass_boundary_fixed"
        ]
        nearest = rejected_conclusion["p99_comparison"]["nearest_rank"]["fixed"]
        linear = rejected_conclusion["p99_comparison"]["linear"]["fixed"]
        interpolation_observation = {
            "round_id": rejected["round_id"],
            "concurrency": rejected["concurrency"],
            "threshold_fixed": plan["thresholds"]["p99_latency_ms_max"],
            "relative_tolerance_ppm": plan["numeric_contract"]["relative_tolerance_ppm"],
            "pass_boundary_fixed": boundary,
            "nearest_rank_p99_fixed": nearest,
            "linear_p99_fixed": linear,
            "selected_method": rejected_conclusion["quantile_method"],
            "selected_verdict": rejected_conclusion["verdict"],
            "nearest_rank_would_pass": nearest <= boundary,
            "linear_would_pass": linear <= boundary,
            "same_sample_opposite_threshold_outcome": (nearest <= boundary)
            != (linear <= boundary),
            "nearest_minus_linear_ms": rejected_conclusion["p99_comparison"][
                "nearest_minus_linear_ms"
            ],
        }

        timeout_plan = load_fixture("timeout_plan.json")
        timeout_plan["target"] = f"http://127.0.0.1:{service.server_port}/load"
        status, timeout_submission = http_call(
            base_url, "PUT", "/api/plans/timeout-error-drill", timeout_plan
        )
        _require(status == 200, timeout_submission)
        status, timeout_run_payload = http_call(
            base_url,
            "POST",
            "/api/plans/timeout-error-drill/runs",
            {"async": False},
            timeout=60,
        )
        _require(status == 200, timeout_run_payload)
        timeout_run = timeout_run_payload["data"]
        timeout_round_id = timeout_run["rounds"][0]["round_id"]
        status, timeout_round_payload = http_call(
            base_url,
            "GET",
            f"/api/rounds/{timeout_round_id}?include_samples=true",
        )
        _require(status == 200, timeout_round_payload)
        timeout_round = timeout_round_payload["data"]

        environment = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "numpy": np.__version__,
            "container_used": False,
            "target_scope": "loopback deterministic local service only",
        }
        report = {
            "environment": environment,
            "submitted_plan": {
                "plan_id": submitted["data"]["plan_id"],
                "version": submitted["data"]["version"],
                "fingerprint": submitted["data"]["fingerprint"],
                "stored_thresholds": submitted["data"]["stored_thresholds"],
                "numeric_contract": submitted["data"]["numeric_contract"],
                "decision_rules": submitted["data"]["decision_rules"],
            },
            "capacity_run": {
                "run_id": run["run_id"],
                "status": run["status"],
                "plan_version": run["plan_version"],
                "inflection": run["inflection"],
                "rejected_stage": run["rejected_stage"],
                "decision_basis": run["decision_basis"],
                "conclusion_version": run["conclusion_version"],
                "rounds": [_round_evidence(round_data) for round_data in run["rounds"]],
            },
            "interpolation_observation": interpolation_observation,
            "concurrency_checks": concurrency_checks,
            "startup_recovery": startup_recovery,
            "error_timeout_drill": {
                "run_id": timeout_run["run_id"],
                "verdict": timeout_run["rounds"][0]["conclusion"]["verdict"],
                "sample_aggregate": timeout_round["sample_aggregate"],
                "threshold_checks": timeout_run["rounds"][0]["conclusion"][
                    "threshold_checks"
                ],
            },
        }
        json_path = output_dir / "acceptance_run_report.json"
        markdown_path = output_dir / "acceptance_report.md"
        json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False),
            encoding="utf-8",
        )
        markdown_path.write_text(_markdown(report), encoding="utf-8")
        print(f"wrote {json_path}")
        print(f"wrote {markdown_path}")
        return 0
    finally:
        server.shutdown()
        server.server_close()
        service.shutdown()
        service.server_close()
        tempdir.cleanup()


def _round_evidence(round_data: dict[str, Any]) -> dict[str, Any]:
    conclusion = round_data["conclusion"]
    metrics = conclusion["metrics"]
    return {
        "round_id": round_data["round_id"],
        "kind": round_data["kind"],
        "stage_index": round_data["stage_index"],
        "attempt_no": round_data["attempt_no"],
        "concurrency": round_data["concurrency"],
        "sample_size": round_data["sample_size"],
        "plan_version": round_data["plan_version"],
        "quantile_method": round_data["quantile_method"],
        "conclusion_version": conclusion["conclusion_version"],
        "verdict": conclusion["verdict"],
        "sample_count": metrics["sample_count"],
        "sample_digest": metrics["sample_digest"],
        "p50_latency_ms": metrics["latency_ms"]["p50"]["decimal"],
        "p95_latency_ms": metrics["latency_ms"]["p95"]["decimal"],
        "p99_latency_ms": metrics["latency_ms"]["p99"]["decimal"],
        "p99_nearest_rank_ms": conclusion["p99_comparison"]["nearest_rank"]["decimal"],
        "p99_linear_ms": conclusion["p99_comparison"]["linear"]["decimal"],
        "p99_nearest_minus_linear_ms": conclusion["p99_comparison"][
            "nearest_minus_linear_ms"
        ],
        "throughput_rps": metrics["throughput_rps"]["value"]["decimal"],
        "error_rate": metrics["error_rate"]["value"]["decimal"],
        "timeout_count": metrics["timeout_count"],
        "error_count": metrics["error_count"],
        "threshold_checks": conclusion["threshold_checks"],
        "ratio_rounding_accumulation": metrics["error_rate"]["ledger"]["cumulative"],
        "computation_digest": conclusion["computation_digest"],
        "failed_thresholds": conclusion["reason"]["failed_thresholds"],
    }


def _service_stats(port: int) -> dict[str, Any]:
    from urllib.request import urlopen

    with urlopen(f"http://127.0.0.1:{port}/stats", timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))["stats"]


def _markdown(report: dict[str, Any]) -> str:
    environment = report["environment"]
    submitted = report["submitted_plan"]
    run = report["capacity_run"]
    observation = report["interpolation_observation"]
    timeout_drill = report["error_timeout_drill"]
    ratio_accumulation = json.dumps(
        run["rounds"][0]["ratio_rounding_accumulation"],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    lines = [
        "# 容量评估验收报告",
        "",
        f"生成时间：`{environment['generated_at']}`  ",
        f"环境：Python `{environment['python']}` / NumPy `{environment['numpy']}` / `{environment['platform']}`  ",
        "运行方式：本地回环 HTTP，未使用容器。",
        "",
        "## 计划与裁决",
        "",
        f"- 计划：`{submitted['plan_id']}` v{submitted['version']}",
        f"- 指纹：`{submitted['fingerprint']}`",
        f"- 轮次：`{run['run_id']}`，状态 `{run['status']}`",
        f"- 拐点：`{run['inflection']['kind']}`，并发 `{run['inflection']['concurrency']}`",
        f"- 否决原档位：并发 `{run['rejected_stage']['concurrency']}`",
        f"- 决策：`{run['decision_basis']['outcome']}`",
        "",
        "| 轮次 | 类型 | 并发 | 判定 | 样本 | P50 ms | P95 ms | P99 ms | 吞吐 rps | 错误率 | 结论版本 |",
        "|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for round_data in run["rounds"]:
        lines.append(
            "| `{round_id}` | {kind} | {concurrency} | {verdict} | {sample_count} | "
            "{p50_latency_ms} | {p95_latency_ms} | {p99_latency_ms} | "
            "{throughput_rps} | {error_rate} | {conclusion_version} |".format(
                **round_data
            )
        )
    lines.extend(
        [
            "",
            "## 插值方式可解释性",
            "",
            f"- 同一否决轮次：`{observation['round_id']}`",
            f"- 阈值允许边界（定点）：`{observation['pass_boundary_fixed']}`，相对容差 `{observation['relative_tolerance_ppm']}` ppm",
            f"- 最近秩 P99：`{observation['nearest_rank_p99_fixed']}`（结论 `{observation['selected_verdict']}`）",
            f"- 线性插值 P99：`{observation['linear_p99_fixed']}`",
            f"- 最近秩 - 线性：`{observation['nearest_minus_linear_ms']}` ms",
            f"- 同一样本在阈值边界下是否产生相反结论：`{observation['same_sample_opposite_threshold_outcome']}`",
            "",
            "计划显式选择最近秩，因此裁决使用最近秩；线性结果仅用于解释，不覆盖已提交结论。",
            "",
            "## 误差与并发",
            "",
            f"- 定点比例累积舍入误差：`{ratio_accumulation}`",
            f"- 并发检查全部不超过档位上限：`{all(item['within_limit'] for item in report['concurrency_checks'])}`",
            "",
            "## 错误注入与真实超时",
            "",
            f"- 轮次 `{timeout_drill['run_id']}` 判定：`{timeout_drill['verdict']}`",
            f"- 样本：`{timeout_drill['sample_aggregate']['sample_count']}`，错误 `{timeout_drill['sample_aggregate']['error_count']}`，超时 `{timeout_drill['sample_aggregate']['timeout_count']}`",
            f"- 错误率阈值触发：`{timeout_drill['threshold_checks']['error_rate_max']['triggered']}`",
            "",
            "## 中断恢复",
            "",
            f"- 本次正常启动恢复报告：未完成意图 `{report['startup_recovery']['unresolved_intents']}`",
            "- 强制终止演练结果见 `artifacts/force_kill_recovery_report.json` 与 `artifacts/force_kill_drill_summary.json`。",
            "- 强制终止后未落盘样本不被声称恢复；恢复报告只基于 SQLite 已提交事实。",
            "",
        ]
    )
    return "\n".join(lines)


def _require(condition: bool, payload: Any) -> None:
    if not condition:
        raise AssertionError(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(main())

