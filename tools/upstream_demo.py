from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def reserve_closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_ready(client: httpx.Client, base_url: str, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            response = client.get(base_url + "/ok")
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError("loopback fixture did not become ready")


def run_demo(port: int) -> dict[str, Any]:
    base_url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [sys.executable, "-m", "tools.local_test_endpoint", "--port", str(port)],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    outcomes: dict[str, Any] = {}
    try:
        with httpx.Client(timeout=1.0) as client:
            wait_ready(client, base_url)
            normal = client.get(base_url + "/ok")
            outcomes["success"] = {"status_code": normal.status_code, "body": normal.json()}
            try:
                client.get(base_url + "/slow", params={"delay_ms": 500}, timeout=0.15)
                outcomes["timeout"] = {"handled": False, "reason": "unexpected success"}
            except httpx.ReadTimeout as exc:
                outcomes["timeout"] = {
                    "handled": True,
                    "exception": type(exc).__name__,
                    "timeout_seconds": 0.15,
                }

            closed_port = reserve_closed_port()
            try:
                client.get(f"http://127.0.0.1:{closed_port}/unavailable", timeout=0.5)
                outcomes["connection_failure"] = {"handled": False, "reason": "unexpected success"}
            except httpx.ConnectError as exc:
                outcomes["connection_failure"] = {
                    "handled": True,
                    "exception": type(exc).__name__,
                    "target": f"127.0.0.1:{closed_port}",
                }
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    return outcomes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demonstrate timeout and connection failure handling")
    parser.add_argument("--port", type=int, default=8766)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(json.dumps(run_demo(args.port), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
