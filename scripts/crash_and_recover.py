from __future__ import annotations

import argparse
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


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(url: str, method: str = "GET", body: Any = None, timeout: float = 5.0) -> tuple[int, Any]:
    payload = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        method=method,
        headers={"Content-Type": "application/json"} if payload is not None else {},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def wait_ready(port: int, process: subprocess.Popen[bytes], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited before becoming ready, return code={process.returncode}")
        try:
            status, _ = request_json(f"http://127.0.0.1:{port}/api/v1/health", timeout=1)
            if status == 200:
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError, json.JSONDecodeError):
            time.sleep(0.15)
    raise RuntimeError("server did not become ready")


def start_server(port: int, db_path: Path, env_extra: dict[str, str]) -> subprocess.Popen[bytes]:
    environment = os.environ.copy()
    environment.pop("REPAIR_CRASH_POINT", None)
    environment.pop("REPAIR_CRASH_MODE", None)
    environment.update(env_extra)
    environment["REPAIR_DB_PATH"] = str(db_path)
    environment["PYTHONPATH"] = str(ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def stop_server(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description="Inject a real process exit and verify startup reconciliation over HTTP")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "forced_kill_recovery.json")
    args = parser.parse_args()
    layout = json.loads((DATA / "sample_layout.json").read_text(encoding="utf-8"))
    trace = json.loads((DATA / "recovery_trace.json").read_text(encoding="utf-8"))

    with tempfile.TemporaryDirectory(prefix="read-repair-crash-") as temp_dir:
        db_path = Path(temp_dir) / "crash.db"
        port = free_port()
        crashing_server = start_server(
            port,
            db_path,
            {
                "REPAIR_CRASH_POINT": "after_task_commit",
                "REPAIR_CRASH_MODE": "exit",
            },
        )
        try:
            wait_ready(port, crashing_server)
            status, _ = request_json(f"http://127.0.0.1:{port}/api/v1/layouts", "POST", layout)
            if status != 201:
                raise RuntimeError(f"layout load failed: {status}")
            print("submitting recovery trace; process is armed to exit after the first committed shard...")
            try:
                request_json(f"http://127.0.0.1:{port}/api/v1/traces", "POST", trace, timeout=5)
            except (urllib.error.URLError, ConnectionError, TimeoutError, json.JSONDecodeError):
                pass
            return_code = crashing_server.wait(timeout=15)
            if return_code != 42:
                raise RuntimeError(f"expected forced exit code 42, got {return_code}")
            print("forced process exit observed: code=42")
        finally:
            stop_server(crashing_server)

        recovered_server = start_server(port, db_path, {})
        try:
            wait_ready(port, recovered_server)
            _, report = request_json(f"http://127.0.0.1:{port}/api/v1/recovery")
        finally:
            stop_server(recovered_server)

        print(json.dumps(report, indent=2, ensure_ascii=False))
        summary = report["summary"]
        assert report["recovery_required"] is True
        assert report["clean_shutdown_report"] is False
        assert summary["aborted_rounds"] == 1
        assert summary["tasks_returned_to_ready"] == 1
        assert summary["reusable_committed_shards"] == 1
        facts = report["budget_facts"][0]
        assert facts["committed_bytes"] == 512
        assert facts["released_reserved_bytes_for_uncommitted_tasks"] == 512
        assert facts["remaining_bytes_recomputed"] == 3584

        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"\nRECOVERY PASSED · evidence written to {args.output}")


if __name__ == "__main__":
    main()
