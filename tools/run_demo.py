"""端到端 HTTP 演示：本地拉起仅回环的 uvicorn 子进程，跑全部样本场景。

用法： python tools/run_demo.py
输出确定性的核对结果，并在最后关闭服务。全程不访问网络外部地址。
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SAMPLES = ROOT / "samples"
BASE_URL = "http://127.0.0.1:8080"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_healthy() -> None:
    for _ in range(100):
        try:
            status, body = request("GET", "/health")
            if status == 200:
                return
        except urllib.error.URLError:
            pass
        time.sleep(0.1)
    raise RuntimeError("本地服务未在预期时间内就绪")


def sample(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


def show(title: str, status: int, body: dict) -> None:
    print(f"\n=== {title} [HTTP {status}] ===")
    print(json.dumps(body, ensure_ascii=False, sort_keys=True, indent=2))


def main() -> int:
    port = free_port()
    global BASE_URL
    BASE_URL = f"http://127.0.0.1:{port}"
    db_path = ROOT / "data" / "demo.db"
    env = dict(os.environ)
    env["GRAPH_BENCH_DB"] = str(db_path)
    env["PYTHONPATH"] = str(ROOT)
    if db_path.exists():
        db_path.unlink()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(db_path) + suffix)
        if sidecar.exists():
            sidecar.unlink()

    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "graph_bench.api:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    failures = 0
    try:
        wait_healthy()
        captured: dict[str, tuple[int, dict]] = {}
        show("健康检查", *request("GET", "/health"))
        captured["snapshot"] = request("POST", "/snapshots", {"request_id": "demo-s1", "payload": sample("snapshot_v1.json")})
        show("1.整份快照 v1", *captured["snapshot"])
        show("2.拓扑分层", *request("GET", "/views/layers"))
        show("3.批次视图", *request("GET", "/views/batches"))
        show("4.gateway 影响面", *request("GET", "/resources/gateway/impact"))
        captured["bad_weight"] = request("POST", "/increments", {"request_id": "demo-i3", "payload": sample("increment_bad_weight.json")})
        show("5.非法权重（v1 上校验失败，状态不变）", *captured["bad_weight"])
        captured["increment"] = request("POST", "/increments", {"request_id": "demo-i1", "payload": sample("increment_weight_auth.json")})
        show("6.增量权重调整", *captured["increment"])
        show("7.幂等重放", *request("POST", "/increments", {"request_id": "demo-i1", "payload": sample("increment_weight_auth.json")}))
        captured["stale"] = request("POST", "/increments", {"request_id": "demo-i2", "payload": sample("increment_stale_base.json")})
        show("8.过期基础版本", *captured["stale"])
        show("9.预算内部分结果", *request("GET", "/resources/gateway/impact?budget_ms=1&delay_per_item_ms=5"))
        show("10.依赖环快照", *request("POST", "/snapshots", {"request_id": "demo-cycle", "payload": sample("snapshot_cycle_v9.json")}))
        captured["cycle_view"] = request("GET", "/views/layers")
        show("11.环场景分层契约", *captured["cycle_view"])
        captured["bad_snapshot"] = request("POST", "/snapshots", {"request_id": "demo-bad", "payload": sample("snapshot_bad_checksum_v2.json")})
        show("12.错误摘要快照被拒", *captured["bad_snapshot"])
        status, intents = request("GET", "/intents")
        show("13.意图日志", status, intents)
        expectations = {
            "stale": "E_STALE_BASE",
            "bad_weight": "E_SPEC_INVALID",
            "cycle_view": "E_DEPENDENCY_CYCLE",
            "bad_snapshot": "E_SNAPSHOT_INVALID",
        }
        for key, expected in expectations.items():
            http_status, body = captured[key]
            code = body.get("error", {}).get("code")
            if code != expected:
                print(f"[FAIL] 期望 {expected}，实际 {code}")
                failures += 1
        if captured["snapshot"][1]["snapshot_version"] != 1 or captured["increment"][1]["snapshot_version"] != 2:
            print("[FAIL] 版本推进不符合预期")
            failures += 1
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()

    if failures:
        print(f"\n演示存在 {failures} 处失败")
        return 1
    print("\n全部演示场景符合预期，结果可复现。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
