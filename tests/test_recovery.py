"""强制终止恢复测试。

通过 GRAPH_BENCH_CRASH_POINT 让子进程在第二阶段事务 commit 前 os._exit，
模拟 kill -9；随后新进程打开同一数据库执行 recover()，验证：
- 未提交事务由 SQLite 回滚，当前版本仍指向最后一个完整版本；
- PENDING 意图被标记 ABORTED；
- 恢复报告只包含已落盘事实。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from conftest import load_sample
from graph_bench.service import BenchService
from graph_bench.storage import INTENT_ABORTED, Storage

ROOT = Path(__file__).resolve().parent.parent
HELPER = ROOT / "tests" / "_crash_helper.py"


def run_helper(db_path: Path, crash_point: str | None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    env["GRAPH_BENCH_DB"] = str(db_path)
    if crash_point:
        env["GRAPH_BENCH_CRASH_POINT"] = crash_point
    return subprocess.run(
        [sys.executable, str(HELPER)],
        env=env,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )


def test_crash_during_rebuild_rolls_back_and_recovers(tmp_path: Path) -> None:
    db_path = tmp_path / "crash.db"

    first = run_helper(db_path, None)
    assert first.returncode == 0, first.stderr
    service = BenchService(Storage(db_path))
    assert service.storage.get_current_version() == 1

    crashed = run_helper(db_path, "after_derived_before_commit")
    assert crashed.returncode == 86, crashed.stdout + crashed.stderr

    # 崩溃后、恢复前：版本指针仍是 1，意图停留在 PENDING。
    crashed_storage = Storage(db_path)
    assert crashed_storage.get_current_version() == 1
    pending = crashed_storage.scan_pending_intents()
    assert [item["intent_id"] for item in pending] == ["crash-increment"]
    assert crashed_storage.load_snapshot_rows(2) is None

    recovered = BenchService(Storage(db_path))
    report = recovered.recover()
    assert report["current_version_after"] == 1
    assert report["pending_intent_count"] == 1
    assert report["actions"][0]["action"] == "marked_aborted"

    intent = recovered.get_intent("crash-increment")
    assert intent["status"] == INTENT_ABORTED
    assert intent["error_code"] == "ABORTED_AFTER_CRASH"

    # 同一 request_id 以相同内容可重新提交并成功。
    result = recovered.submit_increment(load_sample("increment_weight_auth.json"), "crash-increment")
    assert result["snapshot_version"] == 2
    assert recovered.storage.get_current_version() == 2

    persisted_reports = recovered.storage.list_recovery_reports()
    assert persisted_reports, "恢复报告应持久化到 SQLite"
    report_body = json.loads(persisted_reports[0]["report_json"])
    assert report_body["durable_versions"] == [1]
    assert "note" in report_body
