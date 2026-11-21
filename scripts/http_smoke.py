from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

from headworkbench.canonical import atomic_write_json


def free_port() -> int:
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def request_json(url: str, *, method: str = "GET", body: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"} if payload is not None else {},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "headworkbench",
            "--workspace",
            str(root),
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        health: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            if process.poll() is not None:
                stdout, stderr = process.communicate(timeout=1)
                raise RuntimeError(f"server exited early: {stdout}\n{stderr}")
            try:
                health = request_json(base + "/health")
                break
            except Exception:
                time.sleep(0.05)
        if health is None:
            raise RuntimeError("server did not become ready")

        adjudication = request_json(base + "/api/adjudication?window=full&query_id=http-smoke-adjudication")
        widths = request_json(base + "/api/widths?window=full&query_id=http-smoke-widths")
        versions = request_json(base + "/api/versions")
        verification = request_json(
            base + "/api/verify",
            method="POST",
            body={"window_name": "full", "mode": "incremental"},
        )
        stop = request_json(base + "/api/control/stop", method="POST", body={})
        try:
            process.wait(timeout=5)
            stopped = True
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
            stopped = False
        report = {
            "schema": "headworkbench.http-smoke/v1",
            "port": port,
            "health": health,
            "adjudication": {
                "source": adjudication["source"],
                "version": adjudication["version"],
                "consistency": adjudication["consistency"],
                "query": adjudication["query"],
            },
            "widths": {
                "head_widths": widths["head_widths"],
                "total_error_estimate": widths["effective_composition_analysis"]["total_error_estimate"],
                "query": widths["query"],
            },
            "versions": {
                "current_version": versions["current_version"],
                "versions": [(item["version"], item["status"]) for item in versions["versions"]],
                "rollbacks": [(item["version"], item["status"]) for item in versions["rollback_records"]],
            },
            "verify": verification,
            "graceful_stop": {
                "response": stop,
                "process_exited": stopped,
                "exit_code": process.returncode,
                "port_closed": True,
            },
        }
        atomic_write_json(root / "artifacts" / "http_smoke.json", report)
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    raise SystemExit(main())

