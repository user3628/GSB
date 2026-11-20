"""端到端演示：固定本地样本的四段流程。

1. 全量索引（B1）
2. 仅追加增量（B2，走 APPEND_ONLY 快路径）
3. 重打时间戳触发级联失效（B3 混合批次先演示 REBUILD，再 RETIMESTAMP）
4. 强制终止后恢复（独立子进程在 INDEXING 意图落盘后崩溃，重启重排并提交）

脚本同时启动真实 uvicorn（127.0.0.1:8765）验证 HTTP API，
所有输出写入 runtime/demo_output.txt 并打印到控制台。
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from logindex.store import reset_database  # noqa: E402

DB_PATH = PROJECT / "runtime" / "demo.db"
HOST, PORT = "127.0.0.1", 8765
BASE = f"http://{HOST}:{PORT}"
OUTPUT = PROJECT / "runtime" / "demo_output.txt"


class Recorder:
    def __init__(self):
        self.lines = []

    def say(self, title, payload=None):
        text = f"\n===== {title} ====="
        self.lines.append(text)
        print(text)
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False, indent=2)
            self.lines.append(body)
            print(body)

    def flush(self):
        OUTPUT.write_text("\n".join(self.lines) + "\n", encoding="utf-8")


def wait_port_ready(timeout=30.0):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{BASE}/health", timeout=3.0)
            if resp.status_code == 200:
                return
        except (OSError, httpx.HTTPError) as exc:
            last_error = exc
            time.sleep(0.3)
    raise RuntimeError(f"服务未在超时内就绪: {last_error}")


def wait_port_free(timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.3):
                time.sleep(0.2)
        except OSError:
            return
    raise RuntimeError("端口仍被占用")


def start_server(env_extra=None):
    env = os.environ.copy()
    env["LOGINDEX_DB"] = str(DB_PATH)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "logindex.api:create_app",
         "--factory", "--host", HOST, "--port", str(PORT)],
        cwd=PROJECT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        wait_port_ready()
    except RuntimeError:
        proc.kill()
        output = proc.stdout.read() if proc.stdout else ""
        raise RuntimeError(f"uvicorn 启动失败:\n{output}")
    return proc


def stop_server(proc):
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    wait_port_free()


def sample(name):
    return (PROJECT / "samples" / name).read_text(encoding="utf-8").splitlines()


def main():
    rec = Recorder()
    reset_database(DB_PATH)
    for suffix in ("-wal", "-shm"):
        side = Path(str(DB_PATH) + suffix)
        if side.exists():
            side.unlink()

    server = start_server()
    client = httpx.Client(base_url=BASE, timeout=10)
    try:
        rec.say("环境准备", {"db": str(DB_PATH), "api": BASE,
                            "clean_state": client.get("/health").json()})

        # 段 1：全量索引
        r1 = client.post("/batches", json={
            "batch_id": "B1", "lines": sample("batch1_full.txt")}).json()
        rec.say("段1 全量索引 B1", {
            "version": r1["version"], "path_mode": r1["path_mode"],
            "rebuilt_partitions": r1["rebuilt_partitions"],
            "affected_services": r1["affected_services"],
            "record_count": r1["record_count"]})

        # 段 2：仅追加增量
        r2 = client.post("/batches", json={
            "batch_id": "B2", "base_version": 1,
            "lines": sample("batch2_append.txt")}).json()
        rec.say("段2 仅追加增量 B2", {
            "version": r2["version"], "path_mode": r2["path_mode"],
            "rebuilt_partitions": r2["rebuilt_partitions"],
            "rebuild_reason": r2["rebuild_reason"]})

        # 段 3a：混合批次强制走 REBUILD
        r3 = client.post("/batches", json={
            "batch_id": "B3", "base_version": 2,
            "lines": sample("batch3_mixed.txt")}).json()
        rec.say("段3a 混合批次 B3（触发重建路径）", {
            "version": r3["version"], "path_mode": r3["path_mode"],
            "rebuilt_partitions": r3["rebuilt_partitions"],
            "rebuild_reason": r3["rebuild_reason"]})

        # 段 3b：重打时间戳，级联失效 + 同事务原子重建
        rt = client.post("/retimestamp", json={
            "batch_id": "B1", "line_no": 3,
            "new_timestamp_ms": 1700000000250}).json()
        rec.say("段3b 重打时间戳（级联失效原子重建）", {
            "version": rt["version"], "rebuilt_partitions": rt["rebuilt_partitions"],
            "rebuild_reason": rt["rebuild_reason"],
            "intervals_after": rt["intervals"]})

        hits = client.get("/records/query", params={
            "service": "auth-svc", "level": "WARN"}).json()
        rec.say("段3 查询验证 auth-svc/WARN", {"hit_count": hits["hit_count"],
                                              "records": hits["records"]})
        state = client.get("/index-state").json()
        rec.say("段3 当前物化索引摘要", {
            "version": state["version"],
            "timeline_partition_count": len(state["timeline_index"]),
            "classification_row_count": len(state["classification_index"]),
            "batch_stats_row_count": len(state["batch_stats"])})
        historical = client.get("/index-state", params={"version": 1}).json()
        rec.say("段3 版本可见性：回看 v1", {
            "version": historical["version"], "materialized": historical["materialized"],
            "timeline_partition_count": len(historical["timeline_index"])})
    finally:
        client.close()
        stop_server(server)

    # 段 4：独立子进程武装崩溃点后提交，在 INDEXING 中途强制终止
    rec.say("段4 强制终止演练开始", {"batch": "CRASH1"})
    crash_helper = PROJECT / "runtime" / "_demo_crash.py"
    crash_helper.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "sys.path.insert(0, str(Path(__file__).resolve().parent.parent))\n"
        "from logindex.engine import LogIndexEngine\n"
        f"eng = LogIndexEngine({str(DB_PATH)!r})\n"
        "eng.arm_crash_after_indexing()\n"
        f"lines = Path({str(PROJECT / 'samples' / 'batch_crash.txt')!r}).read_text(encoding='utf-8').splitlines()\n"
        "try:\n"
        "    eng.submit_batch('CRASH1', lines)\n"
        "except SystemExit as exc:\n"
        "    print('CRASH-HELPER:', exc, file=sys.stderr)\n"
        "    raise\n",
        encoding="utf-8",
    )
    crashed = subprocess.run(
        [sys.executable, str(crash_helper)], cwd=PROJECT,
        capture_output=True, text=True,
    )
    rec.say("段4 崩溃子进程结果", {
        "exit_code": crashed.returncode, "stderr_tail": crashed.stderr.strip()[-200:]})

    # 重启服务：启动即恢复，报告必须只基于持久化事实
    recovered_server = start_server()
    try:
        report = httpx.get(f"{BASE}/recovery-report", timeout=10).json()
        rec.say("段4 重启后的恢复报告", report)
        batches = httpx.get(f"{BASE}/batches", timeout=10).json()
        rec.say("段4 恢复后的批次清单", batches)

        # 重排 PENDING 批次并提交，版本延续到最后完整边界之后
        replay = httpx.post(f"{BASE}/batches", json={
            "batch_id": "CRASH1",
            "lines": sample("batch_crash.txt")}, timeout=10).json()
        rec.say("段4 重排后提交 CRASH1", replay)

        final_hits = httpx.get(f"{BASE}/records/query", timeout=10).json()
        rec.say("段4 最终全量查询命中", {"hit_count": final_hits["hit_count"]})
        final_state = httpx.get(f"{BASE}/index-state", timeout=10).json()
        rec.say("段4 最终索引版本", {
            "current_version": final_state["version"],
            "timeline_partition_count": len(final_state["timeline_index"])})
    finally:
        stop_server(recovered_server)

    rec.say("演示完成", {"output_file": str(OUTPUT)})
    rec.flush()


if __name__ == "__main__":
    main()
