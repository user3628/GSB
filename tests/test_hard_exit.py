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
from typing import Any

from .conftest import ROOT, SAMPLES


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _request(base_url: str, method: str, path: str, payload: dict[str, Any] | None = None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base_url + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_ready(base_url: str, process: subprocess.Popen[Any]) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"server exited early: {process.returncode}")
        try:
            if _request(base_url, "GET", "/health")["status"] == "ok":
                return
        except Exception:  # noqa: BLE001 - startup polling is deliberately broad
            time.sleep(0.1)
    raise AssertionError("server startup timed out")


def _start(db: Path, port: int, log_path: Path) -> subprocess.Popen[Any]:
    log = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "app.cli",
            "serve",
            "--db",
            str(db),
            "--samples",
            str(SAMPLES),
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    process._test_log = log  # type: ignore[attr-defined]
    return process


def _stop(process: subprocess.Popen[Any]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    handle = getattr(process, "_test_log", None)
    if handle is not None:
        handle.close()


def test_real_os_exit_recovery(tmp_path: Path):
    db = tmp_path / "hard-exit.db"
    log_path = tmp_path / "server.log"
    first_port = _free_port()
    first_url = f"http://127.0.0.1:{first_port}"
    first = _start(db, first_port, log_path)
    try:
        _wait_ready(first_url, first)
        version = _request(first_url, "GET", "/v1/layers/occupancy")["snapshot_version"]
        _request(
            first_url,
            "POST",
            "/v1/faults/injection",
            {"point": "after_intent", "mode": "hard_exit", "batch_id": "hard-crash"},
        )
        dropped = False
        try:
            _request(
                first_url,
                "POST",
                "/v1/batches",
                {"batch_id": "hard-crash", "snapshot_version": version},
            )
        except Exception:  # noqa: BLE001 - the server intentionally exits mid-request
            dropped = True
        assert dropped is True
        assert first.wait(timeout=15) == 73
    finally:
        _stop(first)

    second_port = _free_port()
    second_url = f"http://127.0.0.1:{second_port}"
    second = _start(db, second_port, log_path)
    try:
        _wait_ready(second_url, second)
        batch = _request(second_url, "GET", "/v1/batches/hard-crash")
        report = _request(second_url, "GET", "/v1/recovery/report")
        recovery = report["latest_startup_recovery"]
        assert batch["state"] == "ABORTED"
        assert all(item["state"] == "ABORTED" for item in batch["items"])
        assert batch["conclusion_matches_items"] is True
        assert recovery["forced_termination_detected"] is True
        assert recovery["unfinished_batch_count"] == 1
        assert recovery["safe_exit_summary_available"] is False
        assert all(item["used_bytes_delta"] == 0 for item in recovery["occupancy_difference"])
        safe = _request(second_url, "POST", "/v1/admin/safe-shutdown", {"reason": "test_done"})
        assert safe["report_type"] == "SAFE_SHUTDOWN"
    finally:
        _stop(second)
