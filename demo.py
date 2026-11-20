"""端到端演示：固定本地样本 + 本地回环测试端点。

四个场景：
  1. 全量分发（全部成功，拓扑顺序）
  2. 含失败组件的分发（业务拒绝 / 连接失败 / 超时 三类失败码，
     分别演示 continue 部分应用与 rollback 回滚收敛）
  3. 并发重复触发（同版本单飞，仅一个实际执行，其余等待复用）
  4. 强制终止后恢复（子进程 SIGKILL，重启扫描意图并按事实回滚）

运行：python3 demo.py
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from config_dispatch.coordinator import Coordinator  # noqa: E402
from config_dispatch.endpoint_server import EndpointCluster  # noqa: E402
from config_dispatch.errors import DispatchError  # noqa: E402
from config_dispatch.push_client import PushClient  # noqa: E402
from config_dispatch.recovery import RecoveryService  # noqa: E402
from config_dispatch.repository import Repository  # noqa: E402
from config_dispatch.verifier import verify  # noqa: E402

SAMPLES = ROOT / "samples"
RUNS = ROOT / "runs"


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def print_trajectory(coordinator: Coordinator, version: int) -> None:
    trajectories = coordinator.get_trajectory(version)
    for component_id in sorted(trajectories):
        chain = " -> ".join(
            f"{item['from_state'] or '(none)'}->{item['to_state']}"
            for item in trajectories[component_id]
        )
        print(f"    {component_id:18s} {chain}")


def apply_overrides(declarations_raw: list[dict], script: dict) -> dict:
    effective = {}
    for raw in declarations_raw:
        item = dict(raw)
        override = script.get(item["component_id"], {})
        if "endpoint_override" in override:
            item["endpoint"] = override["endpoint_override"]
        effective[item["component_id"]] = item
    return effective


def snapshot_files() -> dict[int, Path]:
    return {
        int(path.stem.split("_v")[1]): path
        for path in sorted(SAMPLES.glob("snapshot_v*.json"))
    }


def wait_state(db_path: str, version: int, component_id: str, state: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        connection = sqlite3.connect(db_path, timeout=5)
        try:
            row = connection.execute(
                "SELECT state FROM component_states WHERE version=? AND component_id=?",
                (version, component_id),
            ).fetchone()
        finally:
            connection.close()
        if row and row[0] == state:
            return True
        time.sleep(0.05)
    return False


def main() -> int:
    RUNS.mkdir(exist_ok=True)
    db_path = str(RUNS / "demo.db")
    if os.path.exists(db_path):
        for suffix in ("", "-wal", "-shm"):
            target = db_path + suffix
            if os.path.exists(target):
                os.remove(target)

    declarations_raw = load_json(SAMPLES / "components.json")
    success_script = load_json(SAMPLES / "endpoint_all_success.json")
    failure_script = load_json(SAMPLES / "endpoint_with_failures.json")
    kill_script = load_json(SAMPLES / "endpoint_kill_demo.json")
    snapshots = {version: load_json(path) for version, path in snapshot_files().items()}

    repo = Repository(db_path)
    coordinator = Coordinator(repo, PushClient(timeout_seconds=1.0))
    coordinator.register_declarations(declarations_raw)
    for version in sorted(snapshots):
        coordinator.submit_snapshot(snapshots[version])
    print(f"已提交快照版本: {repo.list_snapshot_versions()}")

    # ------------------------------------------------ 场景一：全量分发
    banner("场景 1：全量分发（v1，全部成功，依赖拓扑顺序）")
    with EndpointCluster(
        {item["component_id"]: item for item in declarations_raw}, success_script
    ):
        plan = coordinator.ensure_plan(1)
        print(f"  影响组件: {plan['affected_components']}")
        print(f"  拓扑顺序: {plan['ordered_components']}")
        run = coordinator.dispatch(1, failure_policy="rollback")
        print(f"  运行结论: {run['status']}")
        print("  组件状态轨迹:")
        print_trajectory(coordinator, 1)

    # -------------------------------- 场景二 A：continue 策略的部分应用
    banner("场景 2A：含失败组件（v2，continue 策略 -> 部分应用）")
    effective_fail = apply_overrides(declarations_raw, failure_script)
    with EndpointCluster(effective_fail, failure_script):
        run = coordinator.dispatch(2, failure_policy="continue")
        print(f"  运行结论: {run['status']} ({run['error_code']})")
        print(f"  失败摘要: {run['error_message']}")
        for row in repo.list_component_states(2):
            extra = f" [{row['error_code']}]" if row["error_code"] else ""
            print(f"    {row['component_id']:18s} {row['state']}{extra}")

    # -------------------------------- 场景二 B：rollback 策略的回滚收敛
    banner("场景 2B：回滚收敛（v3，rollback 策略 -> 全部回滚）")
    # v3 仅改 queue.name；为演示完整失败面，复用失败脚本但仅让 api-gateway 超时
    rollback_script = {
        component_id: {"/apply": {"behavior": "success"}, "/rollback": {"behavior": "success"}}
        for component_id in effective_fail
    }
    rollback_script["api-gateway"] = {
        "/apply": {"behavior": "delay", "delay_seconds": 5},
        "/rollback": {"behavior": "success"},
    }
    with EndpointCluster(
        {item["component_id"]: item for item in declarations_raw}, rollback_script
    ):
        run = coordinator.dispatch(3, failure_policy="rollback")
        print(f"  运行结论: {run['status']} ({run['error_code']})")
        print(f"  结论说明: {run['error_message']}")
        print_trajectory(coordinator, 3)

    # ------------------------------------------------ 场景三：并发重复触发
    banner("场景 3：并发重复触发（v1 已终结 -> 全部复用；v4 单飞演示）")
    with EndpointCluster(
        {item["component_id"]: item for item in declarations_raw}, success_script
    ):
        results: list[dict] = []
        errors: list[dict] = []

        def worker() -> None:
            try:
                results.append(coordinator.dispatch(4, failure_policy="continue"))
            except DispatchError as exc:
                errors.append(exc.to_dict())

        threads = [threading.Thread(target=worker) for _ in range(5)]
        start = time.time()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.time() - start
        statuses = sorted(item["status"] for item in results)
        print(f"  5 个并发线程耗时: {elapsed:.2f}s，结论集合: {statuses}")
        plan4 = coordinator.ensure_plan(4)
        applied_count = sum(
            1 for row in repo.list_component_states(4) if row["state"] == "APPLIED"
        )
        print(f"  v4 计划组件数: {len(plan4['ordered_components'])}，APPLIED 记录数: {applied_count}")
        # 端点收到的 /apply 次数应与计划组件数一致（单飞，无重复推送）
        print(f"  并发错误数: {len(errors)}")

    # ------------------------------------------------ 场景四：强制终止恢复
    banner("场景 4：强制终止后恢复（v4 重新用于独立工作进程演示）")
    kill_db = str(RUNS / "kill_demo.db")
    for suffix in ("", "-wal", "-shm"):
        target = kill_db + suffix
        if os.path.exists(target):
            os.remove(target)
    kill_repo = Repository(kill_db)
    kill_coordinator = Coordinator(kill_repo, PushClient(timeout_seconds=5.0))
    kill_coordinator.register_declarations(declarations_raw)
    kill_coordinator.submit_snapshot(snapshots[1])
    kill_coordinator.submit_snapshot(snapshots[4])
    kill_repo.close()

    worker_cmd = [
        sys.executable,
        "-m",
        "config_dispatch.worker",
        "--db",
        kill_db,
        "--components",
        str(SAMPLES / "components.json"),
        "--endpoint-script",
        str(SAMPLES / "endpoint_kill_demo.json"),
        "--version",
        "4",
        "--policy",
        "rollback",
        "--timeout",
        "5",
    ]
    proc = subprocess.Popen(
        worker_cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    print("  工作进程已启动，等待在途 DISPATCHED 事实落库 ...")
    if not wait_state(kill_db, 4, "queue-adapter", "APPLIED", timeout=10):
        stdout, stderr = proc.communicate(timeout=5)
        raise RuntimeError(f"未等到在途状态\nSTDOUT:{stdout}\nSTDERR:{stderr}")
    print("  已观测到 queue-adapter=APPLIED 持久化事实，发送 SIGKILL ...")
    proc.kill()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.wait(timeout=5)
    print(f"  工作进程退出码: {proc.returncode}（被信号 {signal.Signals(-proc.returncode).name} 终止）")

    # 重启：新协调器扫描未完成意图并按持久化事实恢复
    restarted_repo = Repository(kill_db)
    restarted = Coordinator(restarted_repo, PushClient(timeout_seconds=2.0))
    before = restarted_repo.list_component_states(4)
    print("  重启后持久化事实（恢复前）:")
    for row in before:
        print(f"    {row['component_id']:18s} {row['state']}")
    recovery = RecoveryService(restarted).recover()
    print("  恢复报告（仅含持久化事实与据此采取的动作）:")
    print(json.dumps(recovery, ensure_ascii=False, indent=4))
    print("  恢复后组件状态:")
    for row in restarted_repo.list_component_states(4):
        print(f"    {row['component_id']:18s} {row['state']}")
    kill_result = verify(restarted_repo).to_dict()
    print(f"  恢复库验证结论: ok={kill_result['ok']}")
    restarted_repo.close()

    # ------------------------------------------------ 统一验证
    banner("验证工具：主演示库一致性检查")
    result = verify(repo).to_dict()
    for check in result["checks"]:
        print(f"  [{'PASS' if check['ok'] else 'FAIL'}] {check['name']}")
    print(f"  总体结论: ok={result['ok']}，违例数: {len(result['violations'])}")
    report_path = RUNS / "verification_report.json"
    report_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"  结构化报告已写入: {report_path.relative_to(ROOT)}")
    repo.close()
    overall_ok = result["ok"] and kill_result["ok"]
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
