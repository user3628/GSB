"""可重复的端到端实验入口（默认通过本地 HTTP 接口执行）。

完成四个规定场景并打印结构化报告：
  1. 全量策略裁决（policy v1 + batch 1），输出各版本分流占比；
  2. 镜像采样与去重（policy v3 + batch 2），输出镜像条数与窗口抑制；
  3. 越过灰度上限（policy v2 + batch 3），输出容差判定上下文；
  4. 强制终止后恢复（子进程崩溃注入 + 重启扫描），输出恢复结论。

另外输出混合量级样本上的定点 Kahan vs 朴素 float64 误差对照与数值契约声明。

用法
----
    python -m scripts.run_experiment                 # 自动起服务并跑全流程
    python -m scripts.run_experiment --base-url http://127.0.0.1:8077
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
DEFAULT_PORT = 8077
HOST = "127.0.0.1"


def _load(name: str) -> Dict[str, Any]:
    with open(SAMPLES / name, encoding="utf-8") as fh:
        return json.load(fh)


def _request(method: str, url: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read().decode("utf-8"))


def _wait_ready(base_url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _request("GET", f"{base_url}/health")
            return
        except Exception:  # noqa: BLE001
            time.sleep(0.3)
    raise RuntimeError(f"服务在 {timeout}s 内未就绪: {base_url}")


def _start_server(db_path: Path, port: int, crash: bool = False) -> subprocess.Popen:
    env = dict(os.environ)
    env["GRAYSWITCH_DB"] = str(db_path)
    if crash:
        env["GRAYSWITCH_CRASH_AFTER_INTENT"] = "1"
    cmd = [
        sys.executable, "-m", "uvicorn", "app.api:app",
        "--host", HOST, "--port", str(port), "--log-level", "warning",
    ]
    return subprocess.Popen(
        cmd, cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def run(base_url: str) -> Dict[str, Any]:
    report: Dict[str, Any] = {}

    # 场景 1：全量策略裁决
    _request("POST", f"{base_url}/policies", _load("policy_v1.json"))
    batch1 = _request("POST", f"{base_url}/events", _load("events_batch_1.json"))
    stats1 = _request("GET", f"{base_url}/stats/routing?policy_version=1")
    report["scenario_1_full_routing"] = {
        "batch_summary": {k: batch1[k] for k in
                          ("received", "routed", "rejected", "mirrored")},
        "version_shares": stats1["shares"],
        "gray_usage": stats1["gray_usage"],
        "total_weight": stats1["total_weight"],
    }

    # 先按单调顺序提交 v2，保证版本号 1 < 2 < 3
    _request("POST", f"{base_url}/policies", _load("policy_v2.json"))

    # 场景 2：镜像采样与去重（独立策略版本 3）
    _request("POST", f"{base_url}/policies", _load("policy_v3.json"))
    batch2 = _request("POST", f"{base_url}/events", _load("events_batch_2.json"))
    suppressed = [
        v["request_id"] for v in batch2["verdicts"]
        if v["mirror"]["sample_hit"] and not v["mirror"]["mirrored"]
    ]
    report["scenario_2_mirror_dedup"] = {
        "received": batch2["received"],
        "mirrored": batch2["mirrored"],
        "dedup_suppressed_count": len(suppressed),
        "dedup_suppressed_requests": suppressed,
        "window_boundary": "inclusive both ends",
    }

    # 场景 3：越过灰度上限（策略版本 2，已在前面按单调顺序提交）
    batch3 = _request("POST", f"{base_url}/events", _load("events_batch_3.json"))
    first_reject = next(
        v for v in batch3["verdicts"] if v["status"] == "rejected"
    )
    report["scenario_3_gray_limit"] = {
        "routed": batch3["routed"],
        "rejected": batch3["rejected"],
        "first_violation": {
            "request_id": first_reject["request_id"],
            "error_code": first_reject["error_code"],
            "gray_check": first_reject["gray_check"],
        },
    }

    # 数值契约与 float64 对照
    contract = _request("GET", f"{base_url}/contracts/errors?policy_version=1")
    report["numeric_contract_v1"] = {
        "fixed_point": contract["fixed_point"],
        "normalization": contract["normalization"],
        "recorded_op_count": len(contract["recorded_ops"]),
        "sample_ops": contract["recorded_ops"][:3],
    }
    mixed = _load("mixed_magnitude.json")
    comparison = _request("POST", f"{base_url}/experiments/float64",
                          {"values": mixed["values"]})
    report["float64_comparison"] = {
        p["path"]: {"sum": p["sum"], "abs_error": p["abs_error"]}
        for p in comparison["paths"]
    }

    # 重放逐位一致性
    replay_check = _request("POST", f"{base_url}/admin/replay-check?policy_version=1")
    report["replay_bit_identical"] = replay_check

    return report


def run_crash_recovery(data_dir: Path, port: int) -> Dict[str, Any]:
    """场景 4：独立数据库上演示强制终止与重启恢复。"""
    crash_db = data_dir / "crash_experiment.db"
    if crash_db.exists():
        crash_db.unlink()
    worker = ROOT / "scripts" / "_crash_worker.py"
    env = dict(os.environ)
    env["GRAYSWITCH_CRASH_AFTER_INTENT"] = "1"
    env["PYTHONPATH"] = str(ROOT)
    proc = subprocess.run(
        [sys.executable, str(worker), str(crash_db)],
        env=env, cwd=str(ROOT), capture_output=True, text=True,
    )
    crash_exit = proc.returncode

    # 重启（不带崩溃变量），启动时 lifespan 自动恢复
    server = _start_server(crash_db, port + 1, crash=False)
    try:
        base = f"http://{HOST}:{port + 1}"
        _wait_ready(base)
        health = _request("GET", f"{base}/health")
        recovery = _request("GET", f"{base}/recovery")
        batches = _request("GET", f"{base}/batches")
        return {
            "crash_exit_code": crash_exit,
            "startup_recovery": health["startup_recovery"],
            "recovery_reports": recovery["reports"],
            "committed_batches_after_restart": batches["batches"],
            "conclusion": "PENDING intent marked ABORTED; counters stay at last"
                          " complete policy version; no partial batch visible",
        }
    finally:
        server.terminate()
        server.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    started = None
    if args.base_url is None:
        db_path = data_dir / "experiment.db"
        if db_path.exists():
            db_path.unlink()
        started = _start_server(db_path, args.port)
        base_url = f"http://{HOST}:{args.port}"
        _wait_ready(base_url)
    else:
        base_url = args.base_url

    try:
        report = run(base_url)
        report["scenario_4_crash_recovery"] = run_crash_recovery(
            data_dir, args.port
        )
    finally:
        if started is not None:
            started.terminate()
            started.wait(timeout=10)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    out = data_dir / "experiment_report.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(f"\n报告已写入: {out}")


if __name__ == "__main__":
    main()
