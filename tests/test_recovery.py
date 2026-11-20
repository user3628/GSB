from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from join_planner.journal import IntentJournal


ROOT = Path(__file__).resolve().parent.parent


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _request(base_url: str, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base_url + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _wait_health(base_url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if _request(base_url, "GET", "/health")[0] == 200:
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.05)
    raise AssertionError("planner did not become healthy")


def _start(port: int, runtime: Path, delay: float) -> subprocess.Popen:
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
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_started_intent(runtime: Path, timeout: float = 10.0) -> Path:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for path in sorted((runtime / "intents").glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if payload.get("state") == "started":
                return path
        time.sleep(0.02)
    raise AssertionError("no started intent was persisted")


def test_recovery_rolls_back_orphan_plan(tmp_path: Path) -> None:
    journal = IntentJournal(tmp_path / "runtime")
    record = journal.begin("query", 1, '{"mode":"exact"}')
    plan_path = journal.write_plan(record, {"partial": True})
    assert plan_path.exists()
    report = journal.recover()
    assert report["counts"]["recovered_aborted"] == 1
    assert report["entries"][0]["plan_rolled_back"] is True
    assert not plan_path.exists()
    persisted = json.loads((journal.intent_dir / f"{record['intent_id']}.json").read_text(encoding="utf-8"))
    assert persisted["state"] == "aborted_recovered"


def test_forced_termination_produces_restart_recovery_report(tmp_path: Path) -> None:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    runtime = tmp_path / "runtime"
    first = _start(port, runtime, delay=10.0)
    second: subprocess.Popen | None = None
    request_thread: threading.Thread | None = None
    try:
        _wait_health(base_url)

        def submit() -> None:
            try:
                _request(
                    base_url,
                    "POST",
                    "/v1/plans",
                    {"query_id": "q_product_priority", "snapshot_version": 1, "mode": "exact"},
                )
            except Exception:
                pass

        request_thread = threading.Thread(target=submit, daemon=True)
        request_thread.start()
        intent_path = _wait_started_intent(runtime)
        first.kill()
        first.wait(timeout=10)

        second = _start(port, runtime, delay=0.0)
        _wait_health(base_url)
        status, report = _request(base_url, "GET", "/v1/recovery/report")
        assert status == 200
        assert report["source"] == "persisted_intent_files_only"
        assert report["counts"]["recovered_aborted"] >= 1
        assert any(entry["intent_id"] == intent_path.stem for entry in report["entries"])
        persisted = json.loads(intent_path.read_text(encoding="utf-8"))
        assert persisted["state"] == "aborted_recovered"
    finally:
        if first.poll() is None:
            first.kill()
            first.wait(timeout=5)
        if second is not None and second.poll() is None:
            second.terminate()
            try:
                second.wait(timeout=10)
            except subprocess.TimeoutExpired:
                second.kill()
                second.wait(timeout=5)
        if request_thread is not None:
            request_thread.join(timeout=1)

