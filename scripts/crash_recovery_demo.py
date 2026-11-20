from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request(base_url: str, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base_url + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_health(base_url: str) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if request(base_url, "GET", "/health")[0] == 200:
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.1)
    raise RuntimeError("server did not become healthy")


def start_server(port: int, runtime: Path, delay: float) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(ROOT),
            "PLANNER_RUNTIME_DIR": str(runtime),
            "PLANNER_INITIAL_SNAPSHOT": "snapshot_v1.json",
            "PLANNER_BUILD_DELAY_SECONDS": str(delay),
            "PLANNER_LOG_LEVEL": "warning",
        }
    )
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "join_planner.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_started_intent(runtime: Path, timeout: float = 15.0) -> Path:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for path in sorted((runtime / "intents").glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if payload.get("state") == "started":
                return path
        time.sleep(0.05)
    raise RuntimeError("no started intent appeared before timeout")


def main() -> None:
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(prefix="join-planner-crash-") as temporary:
        runtime = Path(temporary) / "runtime"
        first = start_server(port, runtime, delay=10.0)
        thread: threading.Thread | None = None
        try:
            wait_health(base_url)
            result: dict[str, Any] = {}

            def submit() -> None:
                try:
                    result["response"] = request(
                        base_url,
                        "POST",
                        "/v1/plans",
                        {
                            "query_id": "q_product_priority",
                            "snapshot_version": 1,
                            "mode": "exact",
                        },
                    )
                except Exception as exc:
                    result["error"] = type(exc).__name__

            thread = threading.Thread(target=submit, daemon=True)
            thread.start()
            started_path = wait_started_intent(runtime)
            print("started intent:", started_path.name)
            first.kill()
            first.wait(timeout=10)
            print("forced termination delivered; server pid:", first.pid)
            thread.join(timeout=3)

            second = start_server(port, runtime, delay=0.0)
            try:
                wait_health(base_url)
                status, report = request(base_url, "GET", "/v1/recovery/report")
                if status != 200:
                    raise RuntimeError(f"recovery report failed: {status} {report}")
                print("recovery report:")
                print(json.dumps(report, indent=2, ensure_ascii=False))
                if report["counts"]["recovered_aborted"] < 1:
                    raise RuntimeError("recovery did not report a recovered aborted build")
            finally:
                second.terminate()
                try:
                    second.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    second.kill()
                    second.wait(timeout=5)
        finally:
            if first.poll() is None:
                first.kill()
                first.wait(timeout=5)
            if thread is not None:
                thread.join(timeout=1)


if __name__ == "__main__":
    main()

