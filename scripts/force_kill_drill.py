"""Hard-kill a process during SAMPLING, restart it, and verify recovery evidence."""

from __future__ import annotations

import argparse
import json
import os
import signal
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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capacity_lab.deterministic_service import start_server


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(ROOT / "artifacts"))
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    service, _service_thread = start_server(port=0)
    tempdir = tempfile.TemporaryDirectory(prefix="capacity-force-kill-")
    db_path = Path(tempdir.name) / "force-kill.db"
    app_port = _free_port()
    base_url = f"http://127.0.0.1:{app_port}"
    first_process: subprocess.Popen[str] | None = None
    second_process: subprocess.Popen[str] | None = None
    try:
        first_process = _start_app(db_path, app_port)
        _wait_health(base_url, args.timeout, first_process)
        plan = json.loads((ROOT / "fixtures" / "interrupt_plan.json").read_text(encoding="utf-8"))
        plan["target"] = f"http://127.0.0.1:{service.server_port}/load"
        status, payload = _http(base_url, "PUT", "/api/plans/force-kill-drill", plan)
        _require(status == 200, f"plan submission failed: {status} {payload}")
        status, payload = _http(
            base_url,
            "POST",
            "/api/plans/force-kill-drill/runs",
            {"async": True},
        )
        _require(status == 200, f"run start failed: {status} {payload}")
        run_id = payload["data"]["run_id"]

        sampling_round: dict[str, Any] | None = None
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            status, current = _http(base_url, "GET", f"/api/runs/{run_id}")
            _require(status == 200, f"run query failed: {status} {current}")
            rounds = current["data"].get("rounds", [])
            if rounds and rounds[0]["status"] == "SAMPLING":
                sampling_round = rounds[0]
                break
            time.sleep(0.02)
        _require(sampling_round is not None, "round never entered SAMPLING")

        _hard_kill(first_process)
        first_exit = first_process.wait(timeout=10)
        first_process = None

        second_process = _start_app(db_path, app_port)
        _wait_health(base_url, args.timeout, second_process)
        status, recovery_payload = _http(base_url, "GET", "/api/recovery/latest")
        _require(status == 200, f"recovery query failed: {status} {recovery_payload}")
        report = recovery_payload["data"]
        status, run_payload = _http(base_url, "GET", f"/api/runs/{run_id}")
        _require(status == 200, f"run lookup failed after restart: {status} {run_payload}")
        restored_run = run_payload["data"]
        status, round_payload = _http(
            base_url, "GET", f"/api/rounds/{sampling_round['round_id']}"
        )
        _require(status == 200, f"round lookup failed after restart: {status} {round_payload}")
        restored_round = round_payload["data"]

        checks = {
            "unresolved_intent_reported": report.get("unresolved_intents") == 1,
            "round_marked_aborted": len(report.get("rounds_aborted", [])) == 1,
            "run_marked_aborted": restored_run.get("status") == "ABORTED",
            "round_marked_aborted": restored_round.get("status") == "ABORTED",
            "no_uncommitted_samples_exposed": "samples" not in restored_round,
            "no_conclusion_invented": "conclusion" not in restored_round,
            "persisted_samples_available_is_zero": report.get("persisted_samples_available") == 0,
            "does_not_claim_unsaved_recovery": report.get("unsaved_samples_recovered") is False,
            "disclosure_present": "无法恢复" in report.get("disclosure", ""),
        }
        _require(all(checks.values()), f"recovery checks failed: {checks}")

        summary = {
            "drill": "force_kill_during_sampling",
            "kill_method": "TerminateProcess" if os.name == "nt" else "SIGKILL",
            "sampling_observed": {
                "run_id": run_id,
                "round_id": sampling_round["round_id"],
                "round_status": sampling_round["status"],
                "concurrency": sampling_round["concurrency"],
                "sample_size": sampling_round["sample_size"],
            },
            "first_process_exit_code": first_exit,
            "restart_health": "ok",
            "recovery_report_id": report.get("report_id"),
            "checks": checks,
            "persisted_facts_only": True,
            "unsaved_samples_recovered": False,
            "statement": (
                "进程在 SAMPLING 中被硬杀；重启只把已提交的未完成意图和轮次标记为 ABORTED。"
                "已被采样但未提交到 SQLite 的样本不存在恢复依据，因此没有声称还原这些样本。"
            ),
        }
        recovery_path = output_dir / "force_kill_recovery_report.json"
        summary_path = output_dir / "force_kill_drill_summary.json"
        recovery_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"wrote {recovery_path}")
        print(f"wrote {summary_path}")
        return 0
    finally:
        if first_process is not None and first_process.poll() is None:
            first_process.terminate()
            try:
                first_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                first_process.kill()
        if second_process is not None and second_process.poll() is None:
            second_process.terminate()
            try:
                second_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                second_process.kill()
        service.shutdown()
        service.server_close()
        tempdir.cleanup()


def _start_app(db_path: Path, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "capacity_lab.cli",
            "serve",
            "--db",
            str(db_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def _wait_health(base_url: str, timeout: float, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"app process exited before health check: {process.returncode}\n{output}")
        try:
            status, payload = _http(base_url, "GET", "/api/health", timeout=1)
            if status == 200 and payload.get("ok"):
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
            pass
        time.sleep(0.05)
    raise TimeoutError("app health check timed out")


def _hard_kill(process: subprocess.Popen[str]) -> None:
    if os.name == "nt":
        process.kill()
    else:
        os.kill(process.pid, signal.SIGKILL)


def _http(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base_url + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8"))
        finally:
            exc.close()


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


if __name__ == "__main__":
    raise SystemExit(main())

