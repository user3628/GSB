#!/usr/bin/env python3
"""Reproducible end-to-end demo over the loopback HTTP API."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rateguard.samples import TIERS, generate_all, load_batches  # noqa: E402


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
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base_url + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_ready(base_url: str, process: subprocess.Popen[Any], log_path: Path) -> None:
    deadline = time.time() + 20
    last_error = "timeout"
    while time.time() < deadline:
        if process.poll() is not None:
            break
        try:
            status, payload = http_json(base_url, "GET", "/health", timeout=0.5)
            if status == 200 and payload.get("ok"):
                return
            last_error = f"status={status} payload={payload}"
        except Exception as exc:  # noqa: BLE001 - readiness polling
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.08)
    log = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
    raise RuntimeError(f"server did not become ready ({last_error})\n--- log ---\n{log}")


def start_server(
    *,
    port: int,
    db: Path,
    log_path: Path,
    fault: str | None = None,
) -> subprocess.Popen[Any]:
    env = dict(os.environ)
    if fault:
        env["RATEGUARD_FAULT"] = fault
    else:
        env.pop("RATEGUARD_FAULT", None)
    log = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "rateguard.server",
            "--db",
            str(db),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
    )
    process._rateguard_log = log  # type: ignore[attr-defined]
    return process


def stop_server(process: subprocess.Popen[Any], *, kill: bool = False) -> int | None:
    if process.poll() is None:
        if kill:
            process.kill()
        else:
            process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log = getattr(process, "_rateguard_log", None)
    if log is not None:
        log.close()
    return process.returncode


def checkpoint_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def format_table(windows: list[dict[str, Any]]) -> str:
    header = (
        "  start_ms      end_ms    n   success     p50-NR     p50-LIN      p95-NR"
        "     p95-LIN      p99-NR     p99-LIN   max_delta"
    )
    lines = [header]
    for window in reversed(windows):
        def value(key: str) -> str:
            raw = window.get(key)
            return "-" if raw is None else str(raw)

        lines.append(
            f"  {window['start_ms']} {window['end_ms']} {window['sample_count']:4d} "
            f"{window['success_rate'][:8]:>8} {value('p50_nearest_rank_us'):>10} "
            f"{value('p50_linear_us'):>11} {value('p95_nearest_rank_us'):>11} "
            f"{value('p95_linear_us'):>11} {value('p99_nearest_rank_us'):>11} "
            f"{value('p99_linear_us'):>11} {value('max_method_deviation_us'):>11}"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="artifacts/demo.sqlite3")
    parser.add_argument("--out", default="artifacts")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--keep-db", action="store_true", help="do not reset an existing demo DB")
    args = parser.parse_args()

    out_dir = (ROOT / args.out).resolve() if not Path(args.out).is_absolute() else Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = (ROOT / args.db).resolve() if not Path(args.db).is_absolute() else Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not args.keep_db:
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(db_path) + suffix)
            if candidate.exists():
                candidate.unlink()

    manifest = generate_all(out_dir / "samples")
    port = args.port or free_port()
    base_url = f"http://127.0.0.1:{port}"
    report: dict[str, Any] = {
        "ok": True,
        "python": sys.version.split()[0],
        "database": str(db_path),
        "base_url": base_url,
        "manifest": manifest,
        "tiers": {},
        "recovery": {},
    }
    console: list[str] = [
        "RateGuard deterministic local demo",
        f"python={sys.version.split()[0]} database={db_path}",
    ]

    process = start_server(port=port, db=db_path, log_path=out_dir / "server.log")
    try:
        wait_ready(base_url, process, out_dir / "server.log")
        console.append(f"HTTP server ready at {base_url}")
        console.append("")

        for tier_name, spec in TIERS.items():
            batches = load_batches(out_dir / "samples" / f"{tier_name}.jsonl")
            for batch in batches:
                status, payload = http_json(base_url, "POST", "/v1/events/batches", batch)
                if status != 200 or not payload.get("ok"):
                    raise RuntimeError(
                        f"ingest failed tier={tier_name} batch={batch['batch_id']} status={status} payload={payload}"
                    )

            checkout = f"/{tier_name}/api/checkout"
            status, windows_payload = http_json(
                base_url, "GET", f"/v1/windows?route={urllib.parse.quote(checkout)}&limit=50"
            )
            if status != 200:
                raise RuntimeError(f"window query failed: {windows_payload}")
            windows = windows_payload["windows"]
            state_status, state = http_json(
                base_url, "GET", f"/v1/circuit/state?route={urllib.parse.quote(checkout)}"
            )
            if state_status != 200:
                raise RuntimeError(f"state query failed: {state}")
            transition_status, transitions = http_json(
                base_url,
                "GET",
                f"/v1/circuit/transitions?route={urllib.parse.quote(checkout)}",
            )
            if transition_status != 200:
                raise RuntimeError(f"transition query failed: {transitions}")

            insufficient_window = next(
                (window for window in windows if window["sample_count"] < 100), None
            )
            strict_result: dict[str, Any] | None = None
            if insufficient_window is not None:
                strict_status, strict_payload = http_json(
                    base_url,
                    "GET",
                    f"/v1/windows/stats?route={urllib.parse.quote(checkout)}"
                    f"&window_start_ms={insufficient_window['start_ms']}&strict=P99",
                )
                strict_result = {"http_status": strict_status, "payload": strict_payload}

            decisions: list[dict[str, Any]] = []
            as_of = state["window_end_ms"]
            for concurrency in (8, 32, 60):
                status, payload = http_json(
                    base_url,
                    "POST",
                    "/v1/limit/decision",
                    {
                        "route": checkout,
                        "current_concurrency": concurrency,
                        "budget_watermark": "0.70",
                        "target_latency_us": 50_000,
                        "as_of_ms": as_of,
                    },
                )
                decisions.append({"http_status": status, "payload": payload})

            quota_status, quota_payload = http_json(
                base_url,
                "GET",
                f"/v1/quota/derivation?route={urllib.parse.quote(checkout)}",
            )
            report["tiers"][tier_name] = {
                "metadata": manifest["tiers"][tier_name],
                "windows": windows,
                "circuit_state": state,
                "transitions": transitions["transitions"],
                "strict_insufficient_sample": strict_result,
                "decisions": decisions,
                "quota_derivation": {
                    "http_status": quota_status,
                    "payload": quota_payload,
                },
            }

            console.append(
                f"[{tier_name}] seed={spec.seed} events={manifest['tiers'][tier_name]['event_count']} "
                f"batches={manifest['tiers'][tier_name]['batch_count']} "
                f"checkout_windows={len(windows)} breaker={state['state']}"
            )
            console.append(format_table(windows))
            console.append("  breaker trajectory:")
            if transitions["transitions"]:
                for transition in transitions["transitions"]:
                    console.append(
                        f"    {transition['from_state']} -> {transition['to_state']} "
                        f"at ts={transition['occurred_at_ms']} reason={transition['reason']} "
                        f"composite={transition['composite_version']}"
                    )
            else:
                console.append("    (no state transitions; sample windows remained CLOSED)")
            if strict_result is not None:
                code = strict_result["payload"].get("error", {}).get("code")
                console.append(
                    f"  strict P99 sample floor: http={strict_result['http_status']} code={code} "
                    f"sample_count={insufficient_window['sample_count']}"
                )
            console.append("  quota derivations:")
            for item in decisions:
                if item["http_status"] == 200:
                    payload = item["payload"]
                    console.append(
                        f"    concurrency={payload['decision']['current_concurrency']} "
                        f"decision={payload['decision']['code']} quota={payload['quota']['final_quota']} "
                        f"window_version={payload['window']['version']} "
                        f"breaker_version={payload['breaker']['version']} "
                        f"quota_version={payload['quota']['version']} "
                        f"error_bound={payload['quota']['error_upper_bound_units']}"
                    )
                else:
                    error = item["payload"].get("error", {})
                    console.append(
                        f"    decision_error code={error.get('code')} message={error.get('message')}"
                    )
            console.append("")

        routes = sorted(TIERS)
        before_versions: dict[str, int] = {}
        for tier_name in routes:
            route = f"/{tier_name}/api/checkout"
            status, state = http_json(
                base_url, "GET", f"/v1/circuit/state?route={urllib.parse.quote(route)}"
            )
            if status == 200:
                before_versions[route] = state["composite_version"]

        console.append("Starting forced-crash recovery demonstration...")
        stop_server(process)
        process = None

        crash_port = port
        crash_process = start_server(
            port=crash_port,
            db=db_path,
            log_path=out_dir / "crash-server.log",
            fault="exit_after_intent",
        )
        wait_ready(base_url, crash_process, out_dir / "crash-server.log")
        crash_request = {
            "batch_id": "recovery-crash",
            "sequence": 1,
            "events": [
                {
                    "timestamp_ms": 1_700_000_064_000,
                    "route": "/large/api/checkout",
                    "success": True,
                    "latency_us": 30_000,
                }
            ],
        }
        crash_request_disconnected = False
        try:
            http_json(base_url, "POST", "/v1/events/batches", crash_request, timeout=5)
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
            crash_request_disconnected = True
        crash_exit_code = crash_process.wait(timeout=10)
        log = getattr(crash_process, "_rateguard_log", None)
        if log is not None:
            log.close()

        process = start_server(port=port, db=db_path, log_path=out_dir / "restart-server.log")
        wait_ready(base_url, process, out_dir / "restart-server.log")
        recovery_status, recovery_payload = http_json(
            base_url, "GET", "/v1/recovery/report"
        )
        batch_status, batch_payload = http_json(
            base_url, "GET", "/v1/events/batches/recovery-crash/1"
        )
        after_versions: dict[str, int] = {}
        for tier_name in routes:
            route = f"/{tier_name}/api/checkout"
            status, state = http_json(
                base_url, "GET", f"/v1/circuit/state?route={urllib.parse.quote(route)}"
            )
            if status == 200:
                after_versions[route] = state["composite_version"]

        recovery_report = recovery_payload.get("report", {})
        report["recovery"] = {
            "crash_request_disconnected": crash_request_disconnected,
            "child_exit_code": crash_exit_code,
            "recovery_http_status": recovery_status,
            "recovery_report": recovery_payload,
            "aborted_batch_http_status": batch_status,
            "aborted_batch": batch_payload,
            "composite_versions_before_crash": before_versions,
            "composite_versions_after_restart": after_versions,
        }
        console.append(
            f"  crash_request_disconnected={crash_request_disconnected} "
            f"child_exit_code={crash_exit_code}"
        )
        console.append(
            f"  recovery incomplete_intents={recovery_report.get('incomplete_intents_found')} "
            f"batch_status={batch_payload.get('batch', {}).get('status')} "
            f"abort_reason={batch_payload.get('batch', {}).get('abort_reason')}"
        )
        console.append(
            "  previous_complete_versions_preserved="
            f"{recovery_report.get('integrity', {}).get('previous_complete_versions_preserved')} "
            f"versions_before={before_versions} versions_after={after_versions}"
        )

        report["console"] = console
        (out_dir / "demo_report.json").write_text(
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        (out_dir / "demo_console.txt").write_text("\n".join(console) + "\n", encoding="utf-8")
        print("\n".join(console))
        return 0
    finally:
        if process is not None:
            stop_server(process)
        checkpoint_database(db_path)


if __name__ == "__main__":
    raise SystemExit(main())

