from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
ARTIFACTS = ROOT / "artifacts"
RUNTIME = ARTIFACTS / "demo-runtime"


class ApiFailure(RuntimeError):
    def __init__(self, status: int, payload: dict[str, Any]):
        self.status = status
        self.payload = payload
        error = payload.get("error", {})
        self.code = error.get("code")
        super().__init__(f"HTTP {status} {self.code}: {error.get('message')}")


class Client:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        *,
        timeout: float = 20.0,
    ) -> tuple[int, dict[str, Any]]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base_url + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
            raise ApiFailure(error.code, payload) from None

    def ok(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        *,
        timeout: float = 20.0,
    ) -> dict[str, Any]:
        status, payload = self.request(method, path, body, timeout=timeout)
        if status >= 400 or not payload.get("ok"):
            raise ApiFailure(status, payload)
        return payload["data"]

    def expect_error(
        self,
        method: str,
        path: str,
        code: str,
        body: Any | None = None,
    ) -> dict[str, Any]:
        try:
            self.request(method, path, body)
        except ApiFailure as failure:
            if failure.code != code:
                raise AssertionError(
                    f"expected {code}, got {failure.code}: {failure.payload}"
                ) from failure
            return {"status": failure.status, **failure.payload}
        raise AssertionError(f"expected business error {code}, request succeeded")


def load(name: str) -> Any:
    with (SAMPLES / name).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.25)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def wait_health(client: Client, process: subprocess.Popen[Any], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"server exited before health check, exit={process.returncode}"
            )
        try:
            client.ok("GET", "/api/health", timeout=1.0)
            return
        except Exception as error:  # noqa: BLE001 - startup race
            last_error = error
            time.sleep(0.15)
    raise RuntimeError(f"server health check timed out: {last_error}")


def start_server(port: int, db_path: Path, *, fault_injection: bool) -> tuple[subprocess.Popen[Any], Client]:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    log_path = RUNTIME / "server.log"
    log_handle = log_path.open("a", encoding="utf-8")
    env = os.environ.copy()
    env["RISKLAB_DB_PATH"] = str(db_path)
    env["RISKLAB_FAULT_INJECTION"] = "1" if fault_injection else "0"
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    process._risklab_log_handle = log_handle  # type: ignore[attr-defined]
    client = Client(f"http://127.0.0.1:{port}")
    wait_health(client, process)
    return process, client


def stop_server(process: subprocess.Popen[Any]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    handle = getattr(process, "_risklab_log_handle", None)
    if handle is not None:
        handle.close()


def risk_summary(risk: dict[str, Any]) -> dict[str, Any]:
    return {
        "risk_score": risk["risk_score"],
        "risk_level": risk["risk_level"],
        "risk_threshold": risk["risk_threshold"],
        "action": risk["decision"]["action"],
        "policy_version": risk["policy_version"],
        "window_version": risk["window_versions"]["window_version"],
        "window_epoch": risk["window_versions"]["batch_window_epoch"],
        "sample_count": risk["candidate_stats"]["sample_count"],
        "p90_latency_ms": risk["candidate_stats"]["p90_latency_ms"],
    }


def collect_batches(client: Client) -> list[dict[str, Any]]:
    batches = client.ok("GET", "/api/batches")["batches"]
    result = []
    for batch in batches:
        item = {
            "batch_id": batch["batch_id"],
            "service": batch["service"],
            "status": batch["status"],
            "status_trajectory": [
                {
                    "from": transition["from_status"],
                    "to": transition["to_status"],
                    "action": transition["action"],
                    "policy_version": transition["policy_version"],
                    "window_version": transition["window_version"],
                    "window_epoch": transition["window_epoch"],
                }
                for transition in client.ok(
                    "GET", f"/api/batches/{batch['batch_id']}/trajectory"
                )["trajectory"]
            ],
            "windows": batch["windows"],
        }
        try:
            item["risk_full"] = risk_summary(
                client.ok("GET", f"/api/batches/{batch['batch_id']}/risk?mode=FULL")
            )
        except ApiFailure as failure:
            item["risk_full_error"] = {
                "status": failure.status,
                **(failure.payload.get("error") or {}),
            }
        result.append(item)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the deterministic local ChangeGuard demonstration."
    )
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument(
        "--db", type=Path, default=RUNTIME / "workbench-demo.sqlite3"
    )
    parser.add_argument(
        "--keep-db", action="store_true", help="keep the previous demo database"
    )
    args = parser.parse_args()

    if not port_available(args.port):
        print(f"port 127.0.0.1:{args.port} is already in use", file=sys.stderr)
        return 2
    RUNTIME.mkdir(parents=True, exist_ok=True)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    db_path = args.db.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not args.keep_db:
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(db_path) + suffix)
            if candidate.exists():
                candidate.unlink()
    log_path = RUNTIME / "server.log"
    if log_path.exists():
        log_path.unlink()

    script = load("demo-script.json")
    policies = {
        item["version"]: item
        for item in (load("policy-v1.json"), load("policy-v2.json"))
    }
    obs_bundle = load("observations.json")
    scenario_results: dict[str, Any] = {}
    processes: list[subprocess.Popen[Any]] = []
    started_at = datetime.now(timezone.utc)
    try:
        process, client = start_server(args.port, db_path, fault_injection=True)
        processes.append(process)
        seeded = client.ok("POST", "/api/demo/seed", {})
        scenario_results["seed"] = {
            "batch_count": seeded["batch_count"],
            "policy_count": seeded["policy_count"],
            "actions": seeded["actions"],
        }

        stale = client.expect_error(
            "POST", "/api/policies", "E_STALE_POLICY", policies[1]
        )
        scenario_results["stale_policy"] = {
            "code": stale["error"]["code"],
            "context": stale["error"]["context"],
        }

        pass_decision = client.ok(
            "POST",
            "/api/batches/B-PASS/decision",
            {"mode": "FULL", "decision_time_ms": 1_763_900_070_000},
        )
        assert pass_decision["batch"]["status"] == "PASSED"
        scenario_results["release"] = {
            "batch_id": "B-PASS",
            "risk": risk_summary(pass_decision["risk"]),
            "applied": pass_decision["applied"],
            "action": pass_decision["action"],
            "status": pass_decision["batch"]["status"],
            "decision_id": pass_decision["decision_id"],
        }

        rollback = client.ok(
            "POST",
            "/api/batches/B-ROLLBACK/decision",
            {"mode": "FULL", "decision_time_ms": 1_763_910_070_000},
        )
        assert rollback["batch"]["status"] == "ROLLED_BACK"
        scenario_results["auto_rollback"] = {
            "batch_id": "B-ROLLBACK",
            "risk": risk_summary(rollback["risk"]),
            "applied": rollback["applied"],
            "action": rollback["action"],
            "status": rollback["batch"]["status"],
            "cooldown": rollback["cooldown"],
            "decision_id": rollback["decision_id"],
        }

        cooldown = client.expect_error(
            "POST",
            "/api/batches/B-ROLLBACK/decision",
            "E_ROLLBACK_COOLDOWN",
            {"mode": "FULL", "decision_time_ms": 1_763_910_100_000},
        )
        scenario_results["cooldown_retrigger"] = {
            "code": cooldown["error"]["code"],
            "context": cooldown["error"]["context"],
            "still_rolled_back": client.ok("GET", "/api/batches/B-ROLLBACK")[
                "status"
            ],
        }

        comparison = client.ok("GET", "/api/batches/B-COMPARE/compare")
        scenario_results["fast_full_compare"] = {
            "batch_id": "B-COMPARE",
            "assertions": comparison["assertions"],
            "full": risk_summary(comparison["full"]),
            "fast": risk_summary(comparison["fast"]),
            "fast_confidence": comparison["fast"]["confidence"],
            "fast_decision_detail": {
                key: comparison["fast"]["decision"].get(key)
                for key in (
                    "requires_full_review",
                    "threshold_crossed",
                    "confidence_interval_straddles_threshold",
                    "confidence_upper_gap",
                    "confidence_upper_bound",
                    "confidence_bound_exceeded",
                    "can_auto_rollback",
                    "reasons",
                )
            },
            "differences": comparison["differences"],
            "integrity_note": comparison["integrity_note"],
        }

        policy_switch = {}
        for version in (1, 2):
            risk = client.ok(
                "GET", f"/api/batches/B-COMPARE/risk?mode=FULL&policy_version={version}"
            )
            policy_switch[str(version)] = {
                **risk_summary(risk),
                "policy_digest": risk["policy_digest"],
            }
        policy_switch["decision_changed"] = (
            policy_switch["1"]["action"] != policy_switch["2"]["action"]
        )
        scenario_results["policy_switch"] = policy_switch

        insufficient = client.expect_error(
            "GET", "/api/batches/B-INSUFFICIENT/risk?mode=FULL", "E_INSUFFICIENT_SAMPLES"
        )
        scenario_results["insufficient_samples"] = {
            "code": insufficient["error"]["code"],
            "context": insufficient["error"]["context"],
        }

        duplicate_case = obs_bundle["fault_cases"]["identical_duplicate"]
        duplicate = client.ok(
            "POST",
            "/api/observations",
            {"batch_id": duplicate_case["batch_id"], **duplicate_case["payload"]},
        )
        scenario_results["duplicate_idempotent"] = {
            "accepted_count": duplicate["accepted_count"],
            "duplicate_count": duplicate["duplicate_count"],
        }

        conflict_case = obs_bundle["fault_cases"]["conflicting_duplicate"]
        conflict = client.expect_error(
            "POST",
            "/api/observations",
            "E_CONFLICTING_REPORT",
            {"batch_id": conflict_case["batch_id"], **conflict_case["payload"]},
        )
        scenario_results["duplicate_conflict"] = {
            "code": conflict["error"]["code"],
            "context": conflict["error"]["context"],
        }

        late_case = obs_bundle["fault_cases"]["late_sample_after_close"]
        late = client.expect_error(
            "POST",
            "/api/observations",
            "E_LATE_SAMPLE",
            {"batch_id": late_case["batch_id"], **late_case["payload"]},
        )
        scenario_results["late_sample"] = {
            "code": late["error"]["code"],
            "context": late["error"]["context"],
        }

        client.ok(
            "POST",
            "/api/_demo/faults/arm",
            {
                "operation": "RISK_DECISION_ROLLBACK",
                "batch_id": "B-CRASH",
            },
        )
        crash_http_outcome: dict[str, Any]
        try:
            _, crash_response = client.request(
                "POST",
                "/api/batches/B-CRASH/decision",
                {"mode": "FULL", "decision_time_ms": 1_763_940_070_000},
                timeout=10.0,
            )
            crash_http_outcome = {"unexpected_response": crash_response}
        except Exception as error:  # connection is expected to die mid-request
            crash_http_outcome = {
                "connection_failed_as_expected": True,
                "exception_type": type(error).__name__,
            }
        try:
            exit_code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            exit_code = process.wait(timeout=5)
        handle = getattr(process, "_risklab_log_handle", None)
        if handle is not None:
            handle.close()
        if exit_code != 137:
            raise AssertionError(f"expected forced exit code 137, got {exit_code}")

        process2, client2 = start_server(args.port, db_path, fault_injection=False)
        processes.append(process2)
        recovered_batch = client2.ok("GET", "/api/batches/B-CRASH")
        recovery_report = client2.ok("GET", "/api/recovery/report")
        recovered_risk = client2.ok("GET", "/api/batches/B-CRASH/risk?mode=FULL")
        aborted_intents = client2.ok(
            "GET", "/api/intents?status=ABORTED&limit=20"
        )["intents"]
        scenario_results["forced_exit_recovery"] = {
            "forced_exit_code": exit_code,
            "request_outcome": crash_http_outcome,
            "expected_uncommitted_action": "AUTO_ROLLBACK",
            "status_after_restart": recovered_batch["status"],
            "window_epoch_after_restart": recovered_batch["window_epoch"],
            "risk_recomputed_after_restart": risk_summary(recovered_risk),
            "recovery_report": recovery_report,
            "aborted_intent_ids": [item["intent_id"] for item in aborted_intents],
            "recovery_difference": (
                "未提交事务中的 ROLLED_BACK 状态与冷却占用均不可见；重启后批次仍为 "
                f"{recovered_batch['status']}，恢复扫描将意图标记为 ABORTED。"
            ),
        }

        batch_summaries = collect_batches(client2)
        client2.ok("GET", "/api/health")
        finished_at = datetime.now(timezone.utc)
        report = {
            "schema_version": 1,
            "application": "ChangeGuard",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "base_url": f"http://127.0.0.1:{args.port}",
            "database_path": str(db_path),
            "python": sys.version,
            "scenarios": scenario_results,
            "batches": batch_summaries,
            "expected_script_ids": [item["id"] for item in script["scenarios"]],
            "server_log": str(log_path),
        }
        report_path = ARTIFACTS / "demo-report.json"
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        summary_path = ARTIFACTS / "demo-summary.md"
        summary_path.write_text(render_summary(report), encoding="utf-8")
        print(render_console_summary(report))
        print(f"\nfull report: {report_path}")
        print(f"summary:     {summary_path}")
        return 0
    finally:
        for process in processes:
            stop_server(process)


def render_console_summary(report: dict[str, Any]) -> str:
    scenarios = report["scenarios"]
    lines = [
        "ChangeGuard deterministic demo",
        f"DB: {report['database_path']}",
        "",
        "Batch risk / status",
    ]
    for batch in report["batches"]:
        risk = batch.get("risk_full")
        risk_text = (
            f"risk={risk['risk_score']} level={risk['risk_level']} action={risk['action']}"
            if risk
            else f"error={batch.get('risk_full_error', {}).get('code')}"
        )
        trajectory = " -> ".join(
            [item["to"] for item in batch["status_trajectory"]]
        )
        lines.append(
            f"- {batch['batch_id']}: {risk_text}; status={batch['status']}; trace={trajectory}"
        )
    compare = scenarios["fast_full_compare"]
    lines.extend(
        [
            "",
            "Fast vs full",
            f"- full: risk={compare['full']['risk_score']} action={compare['full']['action']}",
            f"- fast: risk={compare['fast']['risk_score']} action={compare['fast']['action']}",
            f"- fast CI: [{compare['fast_confidence']['lower']}, {compare['fast_confidence']['upper']}], requires_full_review={compare['fast_decision_detail']['requires_full_review']}",
            "",
            "Rollback and cooldown",
            f"- B-ROLLBACK: {scenarios['auto_rollback']['status']}, cooldown_until_ms={scenarios['auto_rollback']['cooldown']['cooldown_until_ms']}",
            f"- retrigger: {scenarios['cooldown_retrigger']['code']}, remaining_ms={scenarios['cooldown_retrigger']['context']['remaining_ms']}",
            "",
            "Forced exit recovery",
            f"- exit code: {scenarios['forced_exit_recovery']['forced_exit_code']}",
            f"- status after restart: {scenarios['forced_exit_recovery']['status_after_restart']}",
            f"- pending found / aborted: {scenarios['forced_exit_recovery']['recovery_report']['pending_found']} / {scenarios['forced_exit_recovery']['recovery_report']['aborted_count']}",
            f"- boundary mismatches: {scenarios['forced_exit_recovery']['recovery_report']['boundary_mismatch_count']}",
            "",
            f"policy switch decision changed: {scenarios['policy_switch']['decision_changed']}",
        ]
    )
    return "\n".join(lines)


def render_summary(report: dict[str, Any]) -> str:
    scenarios = report["scenarios"]
    lines = [
        "# ChangeGuard 实际验证摘要",
        "",
        f"- 运行时间：{report['started_at']} → {report['finished_at']}",
        f"- Python：`{report['python'].splitlines()[0]}`",
        f"- SQLite：`{report['database_path']}`",
        f"- HTTP：`{report['base_url']}`",
        "- 数据来源：本仓库 `samples/` 固定本地样本；无外部集群或容器依赖。",
        "",
        "## 批次结果",
        "",
        "| 批次 | 完整风险 | 等级 | 裁决 | 最终状态 | 状态轨迹 |",
        "|---|---:|---|---|---|---|",
    ]
    for batch in report["batches"]:
        risk = batch.get("risk_full")
        if risk:
            risk_text, level, action = risk["risk_score"], risk["risk_level"], risk["action"]
        else:
            error = batch.get("risk_full_error", {})
            risk_text = level = action = error.get("code", "N/A")
        trace = " → ".join(item["to"] for item in batch["status_trajectory"])
        lines.append(
            f"| `{batch['batch_id']}` | {risk_text} | {level} | {action} | `{batch['status']}` | `{trace}` |"
        )
    compare = scenarios["fast_full_compare"]
    lines.extend(
        [
            "",
            "## 快速与完整对照",
            "",
            f"- 同一策略版本：`{compare['assertions']['same_policy_version']}`；同一窗口版本：`{compare['assertions']['same_window_version']}`；同一窗口 epoch：`{compare['assertions']['same_window_epoch']}`。",
            f"- 完整：风险 `{compare['full']['risk_score']}`，动作 `{compare['full']['action']}`，样本 `{compare['full']['sample_count']}`。",
            f"- 快速：风险 `{compare['fast']['risk_score']}`，动作 `{compare['fast']['action']}`，样本 `{compare['fast']['sample_count']}`。",
            f"- 快速 95% 置信区间：`[{compare['fast_confidence']['lower']}, {compare['fast_confidence']['upper']}]`；置信上界与点估计间距 `{compare['fast_decision_detail']['confidence_upper_gap']}`，配置上限 `{compare['fast_decision_detail']['confidence_upper_bound']}`。",
            f"- 需完整复核：`{compare['fast_decision_detail']['requires_full_review']}`；快速可直接回滚：`{compare['fast_decision_detail']['can_auto_rollback']}`。",
            "",
            "## 回滚与冷却期",
            "",
            f"- `B-ROLLBACK` 最终状态 `{scenarios['auto_rollback']['status']}`，冷却至 `{scenarios['auto_rollback']['cooldown']['cooldown_until_ms']}`。",
            f"- 冷却期重触发返回 `{scenarios['cooldown_retrigger']['code']}`，剩余 `{scenarios['cooldown_retrigger']['context']['remaining_ms']} ms`，批次状态仍为 `{scenarios['cooldown_retrigger']['still_rolled_back']}`。",
            "",
            "## 策略版本切换",
            "",
            f"- 同一 `B-COMPARE` 窗口在 v1：风险 `{scenarios['policy_switch']['1']['risk_score']}`，阈值 `{scenarios['policy_switch']['1']['risk_threshold']}`，动作 `{scenarios['policy_switch']['1']['action']}`。",
            f"- 同一 `B-COMPARE` 窗口在 v2：风险 `{scenarios['policy_switch']['2']['risk_score']}`，阈值 `{scenarios['policy_switch']['2']['risk_threshold']}`，动作 `{scenarios['policy_switch']['2']['action']}`；决策变化 `{scenarios['policy_switch']['decision_changed']}`。",
            "",
            "## 强制终止恢复",
            "",
            f"- 故障注入进程退出码：`{scenarios['forced_exit_recovery']['forced_exit_code']}`。",
            f"- 重启后 `B-CRASH` 状态：`{scenarios['forced_exit_recovery']['status_after_restart']}`；重启后风险可重新计算为 `{scenarios['forced_exit_recovery']['risk_recomputed_after_restart']['risk_score']}`。",
            f"- 恢复报告：扫描到 `{scenarios['forced_exit_recovery']['recovery_report']['pending_found']}` 个 PENDING 意图，标记 `{scenarios['forced_exit_recovery']['recovery_report']['aborted_count']}` 个 ABORTED，边界不一致 `{scenarios['forced_exit_recovery']['recovery_report']['boundary_mismatch_count']}` 个。",
            f"- 恢复差异：{scenarios['forced_exit_recovery']['recovery_difference']}",
            "",
            "## 幂等与迟到",
            "",
            f"- 完全重复上报：接受 `{scenarios['duplicate_idempotent']['accepted_count']}`，幂等重复 `{scenarios['duplicate_idempotent']['duplicate_count']}`。",
            f"- 内容冲突重复：`{scenarios['duplicate_conflict']['code']}`，保留首次上报。",
            f"- 关闭窗口后的迟到样本：`{scenarios['late_sample']['code']}`，窗口统计未被改写。",
            f"- 样本不足：`{scenarios['insufficient_samples']['code']}`，观测 `{scenarios['insufficient_samples']['context']['sample_count']}` < 最小样本 `{scenarios['insufficient_samples']['context']['min_samples']}`。",
            "",
            "> 恢复报告只基于启动扫描时已持久化的 SQLite 事实；不承诺在强制终止前生成进程内完整退出报告。",
            "",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
