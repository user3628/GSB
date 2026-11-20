from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
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
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_health(base_url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = request(base_url, "GET", "/health")
            if status == 200:
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.1)
    raise RuntimeError("planner server did not become healthy")


def print_plan(label: str, plan: dict[str, Any]) -> None:
    cost = plan["cost"]
    print(f"\n[{label}] {plan['query_id']} mode={plan['mode']} snapshot=v{plan['snapshot']['version']}")
    print("order:", " -> ".join(plan["selected_order"]))
    print(
        "cost:",
        f"scan={cost['scan']['fixed']}",
        f"join={cost['join']['fixed']}",
        f"write={cost['intermediate_write']['fixed']}",
        f"total={cost['total']['fixed']}",
    )
    search = plan["search"]
    print(
        "search:",
        f"evaluated={search['candidates_evaluated']}",
        f"space={search['candidate_space_size']}",
        f"pruned={search['pruned_branches']}",
        f"guarantee={search['optimality_guarantee']}",
    )
    if plan["correlation_corrections"]:
        print("correlation:", json.dumps(plan["correlation_corrections"], ensure_ascii=False))


def main() -> None:
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(prefix="join-planner-demo-") as temporary:
        runtime = Path(temporary) / "runtime"
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(ROOT),
                "PLANNER_RUNTIME_DIR": str(runtime),
                "PLANNER_INITIAL_SNAPSHOT": "snapshot_v1.json",
                "PLANNER_FAST_BUDGET": "4",
                "PLANNER_LOG_LEVEL": "warning",
            }
        )
        process = subprocess.Popen(
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
        try:
            wait_health(base_url)
            status, loaded = request(
                base_url,
                "POST",
                "/v1/snapshots/load",
                {"filename": "snapshot_v2.json"},
            )
            print("snapshot load:", status, loaded["snapshot"]["version"], loaded["snapshot"]["checksum"])
            queries = ["q_customer_revenue", "q_product_priority", "q_employee_manager"]
            for query_id in queries:
                for mode in ("fast", "exact"):
                    status, plan = request(
                        base_url,
                        "POST",
                        "/v1/plans",
                        {"query_id": query_id, "snapshot_version": 2, "mode": mode},
                    )
                    if status != 200:
                        raise RuntimeError(f"plan request failed: {status} {plan}")
                    print_plan(f"{mode}", plan)

            status, retired = request(
                base_url,
                "POST",
                "/v1/plans",
                {"query_id": "q_customer_revenue", "snapshot_version": 1, "mode": "exact"},
            )
            print("\nretired-version check:", status, retired["code"])
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if process.returncode not in (0, -15, 1):
                output = process.stdout.read() if process.stdout else ""
                print(output, file=sys.stderr)


if __name__ == "__main__":
    main()

