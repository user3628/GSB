from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import requests

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vecengine.examples import projection_plan


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Force-kill the engine and verify persisted recovery facts")
    parser.add_argument("--data-dir", default="data/samples")
    parser.add_argument("--work-dir", default="artifacts/recovery-drill")
    parser.add_argument("--timeout", type=float, default=90.0)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    data_dir = (root / args.data_dir).resolve()
    work_dir = (root / args.work_dir).resolve()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    db_path = work_dir / "recovery.db"
    results_dir = work_dir / "results"
    report_path = work_dir / "recovery_report.json"
    report: dict[str, Any] = {
        "passed": False,
        "forced_termination": {"method": "process.kill (SIGKILL/TerminateProcess)"},
        "assertions": {},
    }
    server_process: subprocess.Popen[Any] | None = None
    restarted_process: subprocess.Popen[Any] | None = None
    first_log = None
    second_log = None
    try:
        port = _free_port()
        server_process, first_log = _start_server(
            root,
            data_dir,
            db_path,
            results_dir,
            port,
            work_dir / "server-first.log",
        )
        base_url = f"http://127.0.0.1:{port}"
        _wait_for_health(base_url, args.timeout)
        request_id = "recovery-drill-medium"
        execution = {
            "path": "vector",
            "batch_size": 512,
            "memory_budget_bytes": 512 * 1024 * 1024,
            "test_batch_delay_ms": 25,
        }

        response_holder: dict[str, Any] = {}

        def submit() -> None:
            try:
                response_holder["response"] = requests.post(
                    f"{base_url}/sessions",
                    json={
                        "request_id": request_id,
                        "plan": projection_plan("medium"),
                        "execution": execution,
                    },
                    timeout=180,
                )
            except Exception as exc:  # connection is expected to break after kill
                response_holder["exception"] = f"{type(exc).__name__}: {exc}"

        submit_thread = threading.Thread(target=submit, daemon=True)
        submit_thread.start()
        persisted = _wait_for_pending_batch(db_path, request_id, args.timeout)
        session_id = persisted["session_id"]
        report["forced_termination"].update(
            {
                "request_id": request_id,
                "session_id": session_id,
                "persisted_before_kill": persisted,
                "submit_thread_result": None,
            }
        )
        server_process.kill()
        server_process.wait(timeout=20)
        submit_thread.join(timeout=20)
        if not response_holder:
            submit_result: Any = "no response before kill"
        elif "exception" in response_holder:
            submit_result = response_holder["exception"]
        else:
            submit_result = {
                "status_code": response_holder["response"].status_code,
                "body": response_holder["response"].text[:500],
            }
        report["forced_termination"]["submit_thread_result"] = submit_result

        restarted_port = _free_port()
        restarted_process, second_log = _start_server(
            root,
            data_dir,
            db_path,
            results_dir,
            restarted_port,
            work_dir / "server-restarted.log",
        )
        restarted_url = f"http://127.0.0.1:{restarted_port}"
        _wait_for_health(restarted_url, args.timeout)
        session_response = requests.get(f"{restarted_url}/sessions/{session_id}", timeout=30)
        reports_response = requests.get(f"{restarted_url}/recovery/report", timeout=30)
        result_response = requests.get(f"{restarted_url}/sessions/{session_id}/result", timeout=30)
        batches_response = requests.get(f"{restarted_url}/sessions/{session_id}/batches", timeout=30)
        session_payload = session_response.json().get("data", {}) if session_response.content else {}
        reports = reports_response.json().get("data", {}).get("reports", []) if reports_response.content else []
        recovery = next((item for item in reports if item.get("session_id") == session_id), None)
        batches = batches_response.json().get("data", {}).get("batches", []) if batches_response.content else []
        result_error = result_response.json().get("error", {}) if result_response.content else {}
        report["http_recovery_report"] = recovery
        report["session_after_restart"] = {
            "status_code": session_response.status_code,
            "status": session_payload.get("status"),
            "error": session_payload.get("error"),
        }
        report["result_query"] = {
            "status_code": result_response.status_code,
            "error_code": result_error.get("code"),
        }
        report["persisted_batches_after_restart"] = batches
        assertions = {
            "session_is_aborted": session_payload.get("status") == "aborted",
            "recovery_report_exists": recovery is not None,
            "report_says_aborted": bool(recovery and recovery.get("status_after") == "aborted"),
            "report_has_completed_batch_boundary": bool(
                recovery and recovery.get("last_completed_batch") is not None
            ),
            "report_has_pending_intent": bool(recovery and recovery.get("pending_intent_count", 0) >= 1),
            "report_claims_no_result": bool(recovery and recovery.get("result_available") is False),
            "result_query_is_aborted": result_response.status_code == 409
            and result_error.get("code") == "E_SESSION_ABORTED",
            "batch_stats_are_persisted": len(batches) >= 1,
        }
        report["assertions"] = assertions
        report["passed"] = all(assertions.values())
        print(json.dumps({"recovery_drill": report}, ensure_ascii=False, indent=2))
        return 0 if report["passed"] else 1
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(json.dumps({"recovery_drill": report}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    finally:
        for process in (server_process, restarted_process):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        for handle in (first_log, second_log):
            if handle is not None:
                handle.close()
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _start_server(
    root: Path,
    data_dir: Path,
    db_path: Path,
    results_dir: Path,
    port: int,
    log_path: Path,
) -> tuple[subprocess.Popen[Any], Any]:
    environment = os.environ.copy()
    environment["VECENGINE_TEST_HOOKS"] = "1"
    log_handle = log_path.open("w", encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "vecengine.server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--db",
        str(db_path),
        "--data-dir",
        str(data_dir),
        "--results-dir",
        str(results_dir),
    ]
    process = subprocess.Popen(
        command,
        cwd=root,
        env=environment,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, log_handle


def _wait_for_health(base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(f"{base_url}/health", timeout=1)
            if response.status_code == 200:
                return
            last_error = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            last_error = type(exc).__name__
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def _wait_for_pending_batch(db_path: Path, request_id: str, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_seen: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            connection = sqlite3.connect(db_path, timeout=2.0)
            connection.row_factory = sqlite3.Row
            try:
                row = connection.execute(
                    "SELECT session_id, status, completed_batches, processed_rows "
                    "FROM sessions WHERE request_id=?",
                    (request_id,),
                ).fetchone()
                if row is not None:
                    pending = connection.execute(
                        "SELECT intent_id, kind, batch_index FROM intent_log "
                        "WHERE session_id=? AND status='pending' AND kind='execute_batch' "
                        "AND batch_index >= 1 ORDER BY intent_id DESC LIMIT 1",
                        (row["session_id"],),
                    ).fetchone()
                    last_seen = {
                        "session_id": row["session_id"],
                        "status": row["status"],
                        "completed_batches": int(row["completed_batches"]),
                        "processed_rows": int(row["processed_rows"]),
                        "pending_batch_intent": dict(pending) if pending is not None else None,
                    }
                    if int(row["completed_batches"]) >= 1 and pending is not None:
                        return last_seen
            finally:
                connection.close()
        except sqlite3.Error:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"did not observe a pending batch before timeout; last={last_seen}")


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


if __name__ == "__main__":
    raise SystemExit(main())
