from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .samples import generate_samples, load_sample_payload


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http_json(
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base_url + path,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def start_server(
    data_dir: Path,
    port: int,
    log_path: Path,
    *,
    writer_delay: float = 0.0,
    fault_crash_after_temp: bool = False,
    fault_crash_delay: float = 0.7,
) -> tuple[subprocess.Popen[bytes], Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("wb")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    command = [
        sys.executable,
        "-m",
        "checkpoint_recovery.server",
        "--data-dir",
        str(data_dir),
        "--port",
        str(port),
        "--max-writing",
        "2",
        "--max-checkpoints",
        "2",
        "--min-round-interval",
        "1",
        "--writer-delay",
        str(writer_delay),
        "--log-level",
        "warning",
    ]
    if fault_crash_after_temp:
        command.extend(["--fault-crash-after-temp", "--fault-crash-delay", str(fault_crash_delay)])
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return process, log_handle


def wait_health(base_url: str, process: subprocess.Popen[bytes], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited before health check, code={process.returncode}")
        try:
            status, body = http_json(base_url, "GET", "/health", timeout=1.0)
            if status == 200 and body.get("ok"):
                return
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def stop_server(process: subprocess.Popen[bytes], log_handle: Any) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    log_handle.close()


def wait_committed(base_url: str, checkpoint_id: str, timeout: float = 20.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        status, body = http_json(base_url, "GET", f"/checkpoints/{checkpoint_id}", timeout=2.0)
        if status != 200:
            raise RuntimeError(f"checkpoint query failed: HTTP {status}: {body}")
        last = body["checkpoint"]
        if last["status"] in {"COMMITTED", "INVALID"}:
            return last
        time.sleep(0.08)
    raise RuntimeError(f"checkpoint did not reach a terminal state: {last}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic end-to-end checkpoint recovery demo")
    parser.add_argument("--work-dir", type=Path, default=PROJECT_ROOT / "artifacts" / "demo_run")
    parser.add_argument("--samples-dir", type=Path, default=PROJECT_ROOT / "artifacts" / "samples")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "artifacts" / "demo_output.json")
    args = parser.parse_args()
    work_dir = args.work_dir.resolve()
    samples_dir = args.samples_dir.resolve()
    output_path = args.output.resolve()
    for path in (work_dir, samples_dir):
        if path.exists():
            shutil.rmtree(path)
    samples_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    indexes = generate_samples(samples_dir, rounds=6)
    sample_by_round = {item["round"]: item for item in indexes}
    print(f"[samples] generated {len(indexes)} deterministic rounds at {samples_dir}")

    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    output: dict[str, Any] = {
        "base_url": base_url,
        "data_dir": str(work_dir),
        "samples_dir": str(samples_dir),
        "phase_1_writes": [],
        "prune": None,
        "forced_termination": {},
        "post_crash_recovery": {},
    }

    process, log_handle = start_server(
        work_dir,
        port,
        PROJECT_ROOT / "artifacts" / "logs" / "phase1_server.log",
        writer_delay=0.25,
    )
    try:
        wait_health(base_url, process)
        print(f"[phase 1] HTTP service ready at {base_url}")
        for round_number in (1, 2, 3, 4):
            payload = load_sample_payload(Path(sample_by_round[round_number]["metadata_path"]), f"demo-r{round_number}")
            status, body = http_json(base_url, "POST", "/checkpoints", payload)
            if status != 202:
                raise RuntimeError(f"write submit failed: HTTP {status}: {body}")
            checkpoint_id = body["checkpoint_id"]
            print(
                f"[write r{round_number}] submit=HTTP {status} immediate={body['status']} id={checkpoint_id}"
            )
            checkpoint = wait_committed(base_url, checkpoint_id)
            _, transitions = http_json(base_url, "GET", f"/checkpoints/{checkpoint_id}/transitions")
            trace = " -> ".join(item["to_state"] for item in transitions["items"])
            print(f"[write r{round_number}] terminal={checkpoint['status']} trace={trace}")
            output["phase_1_writes"].append(
                {
                    "round": round_number,
                    "checkpoint_id": checkpoint_id,
                    "submit_http_status": status,
                    "immediate_status": body["status"],
                    "terminal_status": checkpoint["status"],
                    "trace": trace,
                }
            )
        status, prune_body = http_json(base_url, "POST", "/prune", {})
        if status != 200:
            raise RuntimeError(f"prune failed: HTTP {status}: {prune_body}")
        output["prune"] = prune_body
        print(
            "[prune] pruned="
            + ",".join(str(item["round"]) for item in prune_body["pruned"])
            + " retained="
            + ",".join(str(item["round"]) for item in prune_body["retained"])
        )
        _, listed = http_json(base_url, "GET", "/checkpoints")
        print(
            "[phase 1] persisted states="
            + ", ".join(f"r{item['round']}:{item['status']}" for item in listed["items"])
        )
    finally:
        stop_server(process, log_handle)

    crash_port = free_port()
    crash_url = f"http://127.0.0.1:{crash_port}"
    crash_process, crash_log = start_server(
        work_dir,
        crash_port,
        PROJECT_ROOT / "artifacts" / "logs" / "crash_server.log",
        writer_delay=5.0,
        fault_crash_after_temp=True,
        fault_crash_delay=0.7,
    )
    try:
        wait_health(crash_url, crash_process)
        print(f"[phase 2] crash-injection HTTP service ready at {crash_url}")
        payload = load_sample_payload(Path(sample_by_round[5]["metadata_path"]), "demo-r5-crash")
        status, body = http_json(crash_url, "POST", "/checkpoints", payload)
        if status != 202:
            raise RuntimeError(f"crash write submit failed: HTTP {status}: {body}")
        output["forced_termination"] = {
            "checkpoint_id": body["checkpoint_id"],
            "submit_http_status": status,
            "immediate_status": body["status"],
        }
        print(
            f"[phase 2] r5 accepted as {body['status']} id={body['checkpoint_id']}; waiting for forced termination"
        )
        try:
            exit_code = crash_process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            crash_process.kill()
            exit_code = crash_process.wait(timeout=10)
        output["forced_termination"]["process_exit_code"] = exit_code
        print(f"[phase 2] writer process forcibly exited with code {exit_code}")
    finally:
        if crash_process.poll() is None:
            crash_process.kill()
            crash_process.wait(timeout=10)
        crash_log.close()

    restart_port = free_port()
    restart_url = f"http://127.0.0.1:{restart_port}"
    restart_process, restart_log = start_server(
        work_dir,
        restart_port,
        PROJECT_ROOT / "artifacts" / "logs" / "restart_server.log",
    )
    try:
        wait_health(restart_url, restart_process)
        print(f"[phase 3] restarted HTTP service at {restart_url}")
        _, startup = http_json(restart_url, "GET", "/recovery/startup")
        _, listed = http_json(restart_url, "GET", "/checkpoints")
        print(
            "[phase 3] persisted states="
            + ", ".join(f"r{item['round']}:{item['status']}" for item in listed["items"])
        )
        _, baseline = http_json(restart_url, "GET", "/recovery/baseline")
        target_round = 6
        scheduler_position = sample_by_round[target_round]["scheduler_position"]
        target_cursor = urllib.parse.quote(json.dumps(sample_by_round[target_round]["data_cursor"]))
        _, report_response = http_json(
            restart_url,
            "GET",
            f"/recovery/report?target_round={target_round}"
            f"&target_scheduler_position={scheduler_position}&target_data_cursor={target_cursor}",
        )
        report = report_response["report"]
        output["post_crash_recovery"] = {
            "startup_recovery": startup["startup_recovery"],
            "checkpoint_list": listed["items"],
            "baseline": baseline,
            "report": report,
        }
        print(
            "[phase 3] baseline round="
            f"{baseline['checkpoint']['round']} state={baseline['checkpoint']['status']} "
            f"fallback={baseline['fallback']['occurred']}"
        )
        difference = report["difference_report"]
        print(
            "[report] rounds_to_replay="
            f"{difference['rounds_to_replay']} optimizer_available={difference['optimizer_momentum_available']} "
            f"digest={difference['shard_digest_conclusion']} cursor_rewind={difference['data_cursor_rewind_required']}"
        )
        _, crashed_checkpoint = http_json(
            restart_url,
            "GET",
            f"/checkpoints/{output['forced_termination']['checkpoint_id']}/transitions",
        )
        output["post_crash_recovery"]["crashed_checkpoint_transitions"] = crashed_checkpoint["items"]
        print(
            "[phase 3] crashed checkpoint trace="
            + " -> ".join(item["to_state"] for item in crashed_checkpoint["items"])
        )
        print("[recovery-report]")
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        stop_server(restart_process, restart_log)

    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[done] full demo facts written to {output_path}")


if __name__ == "__main__":
    main()

