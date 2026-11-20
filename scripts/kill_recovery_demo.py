from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "kill_demo.sqlite3"
PORT = 8877
BASE_URL = f"http://127.0.0.1:{PORT}"


def wait_ready(process: subprocess.Popen[bytes], timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"服务提前退出，退出码 {process.returncode}")
        try:
            with urllib.request.urlopen(f"{BASE_URL}/healthz", timeout=0.5) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.1)
    raise RuntimeError("等待服务就绪超时")


def request(path: str, method: str = "GET", body: bytes | None = None, headers: dict[str, str] | None = None):
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=body,
        method=method,
        headers=headers or {},
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        return response.status, response.read()


def start_service(crash_point: str | None = None) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["RG_DB_PATH"] = str(DB_PATH)
    if crash_point:
        env["RG_CRASH_POINT"] = crash_point
    else:
        env.pop("RG_CRASH_POINT", None)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "replication_guard.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(PORT),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    wait_ready(process)
    return process


def stop_service(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(DB_PATH) + suffix)
        if sidecar.exists():
            sidecar.unlink()

    first = start_service()
    payload = (ROOT / "samples" / "policies.jsonl").read_bytes()
    try:
        status, _ = request(
            "/api/policies/import?format=jsonl&batch_id=baseline-before-crash",
            method="POST",
            body=payload,
            headers={"Content-Type": "application/x-ndjson"},
        )
        print(f"基线导入 HTTP {status}")
    finally:
        stop_service(first)

    crashing = start_service("before_commit")
    try:
        request(
            "/api/policies/import?format=jsonl&batch_id=crash-during-import",
            method="POST",
            body=payload,
            headers={"Content-Type": "application/x-ndjson"},
        )
    except (urllib.error.URLError, ConnectionResetError, ConnectionError):
        pass
    crashing.wait(timeout=10)
    print(f"崩溃点 before_commit 后服务退出码：{crashing.returncode}")

    restarted = start_service()
    try:
        status, raw = request("/api/recovery-report")
        report = json.loads(raw)
        print("恢复报告 HTTP", status)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        status, raw = request("/api/policies")
        policies = json.loads(raw)
        print(f"恢复后数据集版本 {policies['datasetVersion']}，策略数 {len(policies['policies'])}")
        try:
            request("/api/policies/import?format=jsonl&batch_id=crash-during-import", method="POST", body=payload,
                    headers={"Content-Type": "application/x-ndjson"})
        except urllib.error.HTTPError as exc:
            body = json.loads(exc.read())
            print(f"同批次重提 HTTP {exc.code}，业务码 {body['code']}，deduplicated={body['details'].get('deduplicated')}")
    finally:
        stop_service(restarted)


if __name__ == "__main__":
    main()

