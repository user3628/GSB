#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import http.client
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = ROOT / "samples" / "fixed_trace.json"
DB_PATH = ROOT / "data" / "demo.db"
ARTIFACT_PATH = ROOT / "artifacts" / "demo_result.json"


def free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http_json(
    port: int,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        raw = response.read()
        return response.status, json.loads(raw.decode("utf-8"))
    finally:
        conn.close()


def wait_for_server(port: int, process: subprocess.Popen[Any], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited early with code {process.returncode}")
        try:
            status, body = http_json(port, "GET", "/healthz", timeout=1.0)
            if status == 200 and body.get("ok"):
                return
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        time.sleep(0.05)
    raise RuntimeError("server did not become healthy")


def start_server(port: int, log_name: str, extra_env: dict[str, str] | None = None):
    env = os.environ.copy()
    env["TCP_SIM_DB_PATH"] = str(DB_PATH)
    env["PYTHONPATH"] = str(ROOT)
    if extra_env:
        env.update(extra_env)
    log_path = ROOT / "artifacts" / log_name
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "tcp_sim.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=str(ROOT),
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_for_server(port, process)
    except BaseException:
        process.kill()
        process.wait(timeout=5)
        log_handle.close()
        raise
    return process, log_handle


def stop_server(process: subprocess.Popen[Any], log_handle: Any) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log_handle.close()


def require_ok(status: int, body: dict[str, Any], label: str) -> dict[str, Any]:
    if status != 200 or not body.get("ok"):
        raise RuntimeError(f"{label} failed: HTTP {status} {json.dumps(body, ensure_ascii=False)}")
    return body["data"]


def batch_payload(
    trace: dict[str, Any],
    *,
    run_id: str,
    batch_id: str,
    events: list[dict[str, Any]],
    final_batch: bool,
    expected_base: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "trace_id": trace["trace_id"],
        "run_id": run_id,
        "batch_id": batch_id,
        "events": events,
        "final_batch": final_batch,
        "config": trace["config"],
    }
    if expected_base is not None:
        payload["expected_base_checkpoint_id"] = expected_base
    return payload


def submit_batch(
    port: int,
    trace: dict[str, Any],
    *,
    run_id: str,
    batch_id: str,
    events: list[dict[str, Any]],
    final_batch: bool,
    expected_base: str | None = None,
) -> dict[str, Any]:
    status, body = http_json(
        port,
        "POST",
        "/v1/simulations/batches",
        batch_payload(
            trace,
            run_id=run_id,
            batch_id=batch_id,
            events=events,
            final_batch=final_batch,
            expected_base=expected_base,
        ),
    )
    return require_ok(status, body, f"submit {batch_id}")


def query_state(port: int, run_id: str, at_ms: int, wait_ms: int = 0) -> dict[str, Any]:
    status, body = http_json(
        port,
        "GET",
        f"/v1/runs/{run_id}/state?conn_id=c1&at_ms={at_ms}&wait_ms={wait_ms}",
        timeout=20.0,
    )
    return require_ok(status, body, f"query {run_id}@{at_ms}")


def print_trace(trace_data: dict[str, Any]) -> None:
    print("\n=== Window / threshold evolution (fixed trace, run=full) ===")
    print("idx  ts_ms  event    ack_class      cwnd  ssthresh  srtt_us  state        reason")
    for row in trace_data["trace"]:
        reason = row["reasons"][-1] if row["reasons"] else ""
        print(
            f"{row['event_index']:>3}  {row['sim_time_ms']:>5}  "
            f"{str(row['event_type']):<7}  {str(row['ack_class']):<13}  "
            f"{row['cwnd_bytes']:>4}  {row['ssthresh_bytes']:>8}  "
            f"{row['srtt_us']:>7}  {row['state']:<11}  {reason}"
        )


def main() -> int:
    trace = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    events = trace["events"]
    splits = trace["suggested_splits"]
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()

    result_bundle: dict[str, Any] = {
        "sample": str(SAMPLE_PATH.relative_to(ROOT)),
        "database": str(DB_PATH.relative_to(ROOT)),
        "loopback_only": True,
        "batch_results": {},
        "error_examples": {},
        "concurrent_queries": {},
        "comparisons": {},
        "crash_recovery": {},
    }

    port1 = free_loopback_port()
    process1, log1 = start_server(
        port1,
        "demo_server_1.log",
        {
            "TCP_SIM_CRASH_BATCH_ID": "crash-batch-2",
            "TCP_SIM_CRASH_AFTER_CHECKPOINTS": "5",
        },
    )
    try:
        print(f"Loopback demo server 1: http://127.0.0.1:{port1}")

        # 1) One-shot full trace.
        full = submit_batch(
            port1,
            trace,
            run_id="full-run",
            batch_id="full-all",
            events=events,
            final_batch=True,
        )
        result_bundle["batch_results"]["full"] = full
        print("\n=== One-shot batch ===")
        print(json.dumps(full, indent=2, ensure_ascii=False))

        # 2) Same trace split into four batches, with concurrent diagnostics while
        # the final batch is being committed.
        split_run = "split-run"
        base = None
        offset = 0
        split_results: list[dict[str, Any]] = []
        for index, end in enumerate(splits):
            is_final = index == len(splits) - 1
            if is_final:
                break
            batch = submit_batch(
                port1,
                trace,
                run_id=split_run,
                batch_id=f"split-batch-{index + 1}",
                events=events[offset:end],
                final_batch=False,
                expected_base=base,
            )
            split_results.append(batch)
            base = batch["committed_checkpoint_id"]
            offset = end

        query_times = [0, 10, 110, 118, 200, 220, 310, 320]
        concurrent_results: dict[int, dict[str, Any]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(query_times)) as pool:
            futures = {
                pool.submit(query_state, port1, split_run, at_ms, 6000): at_ms
                for at_ms in query_times
            }
            time.sleep(0.05)
            final_split = submit_batch(
                port1,
                trace,
                run_id=split_run,
                batch_id="split-batch-4",
                events=events[offset:],
                final_batch=True,
                expected_base=base,
            )
            split_results.append(final_split)
            for future in concurrent.futures.as_completed(futures):
                at_ms = futures[future]
                concurrent_results[at_ms] = future.result()
        result_bundle["batch_results"]["split"] = split_results
        result_bundle["concurrent_queries"] = {
            str(at_ms): {
                "run_status": value["run_status"],
                "event_index": value["event_index"],
                "sim_time_ms": value["sim_time_ms"],
                "snapshot_sha256": value["snapshot_sha256"],
            }
            for at_ms, value in sorted(concurrent_results.items())
        }
        print("\n=== Split batches ===")
        for batch in split_results:
            print(
                f"{batch['batch_id']}: events {batch['starting_event_index']}.."
                f"{batch['ending_event_index']} -> {batch['committed_checkpoint_id']}"
            )
        print("\n=== Concurrent query snapshots during final split commit ===")
        print(json.dumps(result_bundle["concurrent_queries"], indent=2, ensure_ascii=False))

        # The concurrent results must equal the same queries after finalization,
        # and must also equal the one-shot run at the same simulated times.
        for at_ms in query_times:
            full_query = query_state(port1, "full-run", at_ms)
            split_query = query_state(port1, split_run, at_ms)
            if full_query["snapshot_sha256"] != split_query["snapshot_sha256"]:
                raise AssertionError(f"full/split snapshot mismatch at {at_ms} ms")
            if concurrent_results[at_ms]["snapshot_sha256"] != split_query["snapshot_sha256"]:
                raise AssertionError(f"concurrent query mismatch at {at_ms} ms")

        status, body = http_json(
            port1,
            "POST",
            "/v1/runs/compare",
            {"left_run_id": "full-run", "right_run_id": split_run},
        )
        split_compare = require_ok(status, body, "full/split compare")
        result_bundle["comparisons"]["full_vs_split"] = split_compare
        print("\n=== Full vs split snapshot comparison ===")
        print(json.dumps(split_compare, indent=2, ensure_ascii=False))
        if not split_compare["identical"]:
            raise AssertionError("full and split runs are not bit-for-bit identical")

        status, body = http_json(port1, "GET", "/v1/error-codes")
        error_catalog = require_ok(status, body, "error code catalog")
        result_bundle["error_codes"] = error_catalog["codes"]

        # 3) Exercise three business error codes without changing committed state.
        status, body = http_json(
            port1,
            "POST",
            "/v1/simulations/batches",
            batch_payload(
                trace,
                run_id="error-trace",
                batch_id="error-trace-batch",
                events=[{"ts_ms": 1, "conn_id": "x", "event_type": "bogus", "payload_bytes": 0}],
                final_batch=True,
            ),
        )
        result_bundle["error_examples"]["E_TRACE_INVALID"] = {"status": status, "body": body}

        status, body = http_json(
            port1,
            "POST",
            "/v1/simulations/batches",
            batch_payload(
                trace,
                run_id="error-transition",
                batch_id="error-transition-batch",
                events=[
                    {
                        "ts_ms": 1,
                        "conn_id": "x",
                        "event_type": "ack",
                        "payload_bytes": 0,
                        "ack_seq": 0,
                    }
                ],
                final_batch=True,
            ),
        )
        result_bundle["error_examples"]["E_INVALID_TRANSITION"] = {"status": status, "body": body}

        status, body = http_json(port1, "GET", "/v1/batches/does-not-exist")
        result_bundle["error_examples"]["E_BATCH_NOT_FOUND"] = {"status": status, "body": body}

        # Prepare the crash demonstration: 18 events committed in an ACTIVE run,
        # then a wrong-base request must be rejected, then the remaining batch is
        # killed after five event checkpoints.
        crash_run = "crash-run"
        crash_prefix_end = splits[2]
        crash_prefix = submit_batch(
            port1,
            trace,
            run_id=crash_run,
            batch_id="crash-batch-1",
            events=events[:crash_prefix_end],
            final_batch=False,
        )
        result_bundle["batch_results"]["crash_prefix"] = crash_prefix

        status, body = http_json(
            port1,
            "POST",
            "/v1/simulations/batches",
            batch_payload(
                trace,
                run_id=crash_run,
                batch_id="stale-base-batch",
                events=events[crash_prefix_end : crash_prefix_end + 1],
                final_batch=False,
                expected_base="ckpt_intentionally_stale",
            ),
        )
        result_bundle["error_examples"]["E_STALE_BASE"] = {"status": status, "body": body}

        print("\n=== Error examples ===")
        for code, value in result_bundle["error_examples"].items():
            body_code = value["body"].get("error", {}).get("code")
            print(f"{code}: HTTP {value['status']} business_code={body_code}")

        crash_request_error = None
        try:
            submit_batch(
                port1,
                trace,
                run_id=crash_run,
                batch_id="crash-batch-2",
                events=events[crash_prefix_end:],
                final_batch=True,
                expected_base=crash_prefix["committed_checkpoint_id"],
            )
        except (OSError, http.client.HTTPException) as error:
            crash_request_error = f"{type(error).__name__}: {error}"
        try:
            process1.wait(timeout=10)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError("crash-injected server did not terminate") from error
        if process1.returncode in (0, None):
            raise RuntimeError(f"crash-injected server exited unexpectedly with {process1.returncode}")
        result_bundle["crash_recovery"]["forced_termination"] = {
            "batch_id": "crash-batch-2",
            "server_exit_code": process1.returncode,
            "client_error": crash_request_error,
            "crash_after_checkpoints": 5,
        }
        print(
            "\n=== Forced termination ==="
            f"\nserver exit={process1.returncode}; client observed={crash_request_error}"
        )
    finally:
        if process1.poll() is None:
            stop_server(process1, log1)
        else:
            log1.close()

    # 4) Restart. Startup recovery must use only persisted rows, roll the aborted
    # batch back to the prefix checkpoint, and expose that checkpoint for resume.
    port2 = free_loopback_port()
    process2, log2 = start_server(port2, "demo_server_2.log")
    try:
        print(f"\nLoopback demo server 2: http://127.0.0.1:{port2}")
        status, body = http_json(port2, "GET", "/v1/recovery/reports/latest")
        recovery = require_ok(status, body, "latest recovery report")
        result_bundle["crash_recovery"]["recovery_report"] = recovery
        print("\n=== Recovery report (persisted facts only) ===")
        print(json.dumps(recovery, indent=2, ensure_ascii=False))
        if not recovery.get("forced_termination_detected"):
            raise AssertionError("recovery report did not detect the forced termination")
        if not recovery.get("aborted_batches"):
            raise AssertionError("recovery report contains no aborted batch")

        aborted = recovery["aborted_batches"][0]
        resume_base = aborted["rollback_checkpoint_id"]
        resume = submit_batch(
            port2,
            trace,
            run_id="crash-run",
            batch_id="crash-resume-batch",
            events=events[aborted["rollback_event_index"] + 1 :],
            final_batch=True,
            expected_base=resume_base,
        )
        result_bundle["crash_recovery"]["resume_batch"] = resume
        print("\n=== Checkpoint resume batch ===")
        print(json.dumps(resume, indent=2, ensure_ascii=False))

        status, body = http_json(
            port2,
            "POST",
            "/v1/runs/compare",
            {"left_run_id": "full-run", "right_run_id": "crash-run"},
        )
        crash_compare = require_ok(status, body, "full/crash-resume compare")
        result_bundle["comparisons"]["full_vs_crash_resume"] = crash_compare
        print("\n=== Full vs recovered-resume comparison ===")
        print(json.dumps(crash_compare, indent=2, ensure_ascii=False))
        if not crash_compare["identical"]:
            raise AssertionError("recovered run is not bit-for-bit identical to full run")

        status, body = http_json(port2, "GET", "/v1/runs/full-run/window-trace?conn_id=c1")
        window_trace = require_ok(status, body, "window trace")
        result_bundle["window_trace"] = window_trace
        print_trace(window_trace)

        status, body = http_json(port2, "GET", "/v1/runs/full-run/diagnostics")
        diagnostics = require_ok(status, body, "diagnostics")
        result_bundle["diagnostics"] = diagnostics
        print("\n=== Diagnostics ===")
        print(json.dumps(diagnostics, indent=2, ensure_ascii=False))

        print("\n=== Error code catalog ===")
        for item in result_bundle["error_codes"]:
            print(f"{item['code']} (HTTP {item['http_status']}): {item['message']}")

        result_bundle["assertions"] = {
            "full_vs_split_bitwise_identical": split_compare["identical"],
            "full_vs_recovered_bitwise_identical": crash_compare["identical"],
            "concurrent_queries_matched_final_snapshots": True,
            "forced_termination_recovered_from_checkpoint": True,
        }
        ARTIFACT_PATH.write_text(
            json.dumps(result_bundle, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print(f"\nArtifact written: {ARTIFACT_PATH.relative_to(ROOT)}")
        return 0
    finally:
        stop_server(process2, log2)


if __name__ == "__main__":
    raise SystemExit(main())
