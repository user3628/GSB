#!/usr/bin/env python3
"""Run tests and the deterministic demo, then write validation artifacts."""

from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def summary_markdown(report: dict[str, Any], test_output: str, verification: dict[str, Any]) -> str:
    manifest = report["manifest"]["tiers"]
    lines = [
        "# RateGuard 实际验收摘要",
        "",
        f"验证时间：{verification['finished_at']}",
        f"Python：{verification['python']}；NumPy：{verification['numpy']}；SQLite：{verification['sqlite']}",
        f"数据库：`{report['database']}`",
        "",
        "## 自动化测试",
        "",
        "```text",
    ]
    test_lines = [line for line in test_output.splitlines() if line.startswith("Ran ") or line == "OK"]
    lines.extend(test_lines)
    lines.extend(["```", "", "## 三档固定样本", "", "| 规模 | 种子 | 事件数 | 批次 | 当前窗口数 | 最终状态 |", "|---|---:|---:|---:|---:|---|"])
    for tier in ("small", "medium", "large"):
        item = report["tiers"][tier]
        metadata = manifest[tier]
        lines.append(
            f"| {tier} | {metadata['seed']} | {metadata['event_count']} | "
            f"{metadata['batch_count']} | {len(item['windows'])} | {item['circuit_state']['state']} |"
        )
    lines.extend(["", "## 数值与一致性观察", ""])
    for tier in ("small", "medium", "large"):
        item = report["tiers"][tier]
        windows = item["windows"]
        max_window = max(windows, key=lambda window: Decimal(window["max_method_deviation_us"]))
        strict_result = item["strict_insufficient_sample"]
        if strict_result is None:
            strict_note = "所有窗口均满足 P99 样本下限"
        else:
            strict_note = (
                "小样本严格查询 "
                f"{strict_result['payload'].get('error', {}).get('code')}"
            )
        lines.append(
            f"- {tier}: 最大两插值偏差 {max_window['max_method_deviation_us']} μs；"
            f"{strict_note}。"
        )
        transitions = item["transitions"]
        if transitions:
            chain = " -> ".join([transitions[0]["from_state"]] + [t["to_state"] for t in transitions])
            lines.append(f"- {tier} 熔断轨迹：`{chain}`。")
        successful_decisions = [d for d in item["decisions"] if d["http_status"] == 200]
        failed_decisions = [d for d in item["decisions"] if d["http_status"] != 200]
        if failed_decisions:
            codes = sorted({d["payload"].get("error", {}).get("code") for d in failed_decisions})
            lines.append(
                f"- {tier} {len(failed_decisions)} 次裁决返回 `{', '.join(codes)}`，"
                "未用不足样本静默生产配额。"
            )
        for decision in successful_decisions:
            payload = decision["payload"]
            if decision["http_status"] == 200:
                lines.append(
                    f"- {tier} 裁决并发 {payload['decision']['current_concurrency']}："
                    f"{payload['decision']['code']}，配额 {payload['quota']['final_quota']}，"
                    f"同窗口版本={payload['consistency']['same_window_version_in_limiter_and_breaker']}。"
                )
    recovery = report["recovery"]
    recovery_report = recovery["recovery_report"]["report"]
    lines.extend(
        [
            "",
            "## 强制退出恢复",
            "",
            "```text",
            f"crash_request_disconnected = {recovery['crash_request_disconnected']}",
            f"child_exit_code = {recovery['child_exit_code']}",
            f"incomplete_intents_found = {recovery_report['incomplete_intents_found']}",
            f"batch_status = {recovery['aborted_batch']['batch']['status']}",
            f"abort_reason = {recovery['aborted_batch']['batch']['abort_reason']}",
            f"previous_complete_versions_preserved = {recovery_report['integrity']['previous_complete_versions_preserved']}",
            f"versions_before = {recovery['composite_versions_before_crash']}",
            f"versions_after = {recovery['composite_versions_after_restart']}",
            "```",
            "",
            "恢复报告只包含重启后数据库中已持久化的事实；被强制终止的进程没有生成退出报告。",
            "",
            "## 证据文件",
            "",
            "- `artifacts/test-results.txt`",
            "- `artifacts/demo_console.txt`",
            "- `artifacts/demo_report.json`",
            "- `artifacts/demo.sqlite3`",
            "- `artifacts/verification.json`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    started_at = utc_now()
    tests = run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"])
    (ARTIFACTS / "test-results.txt").write_text(tests.stdout, encoding="utf-8")
    if tests.returncode != 0:
        print(tests.stdout)
        return tests.returncode

    demo = run(
        [
            sys.executable,
            "scripts/demo.py",
            "--db",
            "artifacts/demo.sqlite3",
            "--out",
            "artifacts",
        ]
    )
    (ARTIFACTS / "demo-process-output.txt").write_text(demo.stdout, encoding="utf-8")
    if demo.returncode != 0:
        print(demo.stdout)
        return demo.returncode

    report = json.loads((ARTIFACTS / "demo_report.json").read_text(encoding="utf-8"))
    import numpy

    with sqlite3.connect(ARTIFACTS / "demo.sqlite3") as connection:
        sqlite_version = connection.execute("select sqlite_version()").fetchone()[0]
        quick_check = connection.execute("pragma quick_check").fetchone()[0]

    verification = {
        "started_at": started_at,
        "finished_at": utc_now(),
        "python": sys.version.split()[0],
        "numpy": numpy.__version__,
        "sqlite": sqlite_version,
        "tests_return_code": tests.returncode,
        "tests_passed": any(line.strip() == "OK" for line in tests.stdout.splitlines()),
        "test_count": int(re.search(r"Ran (\d+) tests", tests.stdout).group(1)),
        "demo_return_code": demo.returncode,
        "demo_passed": demo.returncode == 0,
        "sqlite_quick_check": quick_check,
        "recovery_incomplete_intents": report["recovery"]["recovery_report"]["report"][
            "incomplete_intents_found"
        ],
        "recovery_versions_preserved": report["recovery"]["recovery_report"]["report"][
            "integrity"
        ]["previous_complete_versions_preserved"],
    }
    (ARTIFACTS / "verification.json").write_text(
        json.dumps(verification, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    (ARTIFACTS / "validation-summary.md").write_text(
        summary_markdown(report, tests.stdout, verification), encoding="utf-8"
    )
    print(json.dumps(verification, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

