#!/usr/bin/env python3
"""End-to-end HTTP demo including a real forced-termination recovery test."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
DEMO_DATA = ROOT / "data" / "e2e_demo"
ARTIFACTS = ROOT / "artifacts"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def server_env(*, stall_ms: int) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONUTF8": "1",
            "SLO_DATA_DIR": str(DEMO_DATA),
            "SLO_DB_PATH": str(DEMO_DATA / "slo.db"),
            "SLO_WINDOWS": "5m,30m,1h",
            "SLO_TARGET": "0.999",
            "SLO_MINIMUM_SAMPLES": "100",
            "SLO_EXACT_EVENT_LIMIT": "10000",
            "SLO_LATE_TOLERANCE_SECONDS": "600",
            "SLO_WARNING_BURN_RATE": "2.0",
            "SLO_CRITICAL_BURN_RATE": "6.0",
            "SLO_STALL_AFTER_INTENT_MS": str(stall_ms),
        }
    )
    return env


def start_server(*, stall_ms: int = 0) -> tuple[subprocess.Popen, str, Any]:
    port = free_port()
    log_path = DEMO_DATA / f"uvicorn-{port}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "1",
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=server_env(stall_ms=stall_ms),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    base_url = f"http://127.0.0.1:{port}"
    for _ in range(150):
        if process.poll() is not None:
            raise RuntimeError(f"uvicorn exited early; inspect {log_path}")
        try:
            response = httpx.get(f"{base_url}/healthz", timeout=0.5)
            if response.status_code == 200:
                return process, base_url, log_handle
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    process.kill()
    raise RuntimeError(f"uvicorn did not become healthy; inspect {log_path}")


def stop_server(process: subprocess.Popen, log_handle: Any, *, force: bool = False) -> None:
    if process.poll() is None:
        if force:
            process.kill()
        else:
            process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log_handle.close()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def event_chunks(events: list[dict[str, Any]], minutes: int = 5) -> list[list[dict]]:
    chunks: list[list[dict]] = []
    current: list[dict] = []
    current_bucket: int | None = None
    for item in events:
        timestamp = parse_timestamp(item["timestamp"])
        bucket = int(timestamp.timestamp()) // (minutes * 60)
        if current_bucket is None or bucket == current_bucket:
            current_bucket = bucket
            current.append(item)
        else:
            chunks.append(current)
            current = [item]
            current_bucket = bucket
    if current:
        chunks.append(current)
    return chunks


def post_events(client: httpx.Client, events: list[dict[str, Any]], prefix: str) -> dict:
    response = client.post(
        "/v1/events",
        json={
            "batch_id": f"{prefix}-{events[0]['event_id']}",
            "source": "fixed-sample",
            "events": events,
        },
    )
    if response.status_code != 200:
        raise RuntimeError(f"ingest failed {response.status_code}: {response.text}")
    return response.json()


def ingest_file(client: httpx.Client, path: Path, service: str) -> int:
    events = read_jsonl(path)
    version = 0
    for index, chunk in enumerate(event_chunks(events)):
        result = post_events(client, chunk, f"{service}-{index:03d}")
        version = result["version"]
    return version


def compact_window(metric: dict[str, Any]) -> dict[str, Any]:
    sampling = metric.get("sampling", {})
    return {
        "window": metric["window"],
        "status": metric["status"],
        "sli": metric.get("sli"),
        "confidence_interval": metric.get("sli_confidence_interval"),
        "total_events": metric.get("total_events", metric.get("sample_count")),
        "sampling_used": sampling.get("used"),
        "sampling_method": sampling.get("method"),
        "sample_size": sampling.get("sample_size"),
        "sample_rate": sampling.get("sample_rate"),
        "budget_consumption_ratio": (metric.get("error_budget") or {}).get(
            "consumed_fraction"
        ),
        "budget_consumed_event_units": (metric.get("error_budget") or {}).get(
            "observed_bad_events"
        ),
    }


def wait_for_running_intent(timeout: float = 15.0) -> dict[str, Any]:
    db_path = DEMO_DATA / "slo.db"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            connection = sqlite3.connect(str(db_path))
            connection.row_factory = sqlite3.Row
            try:
                row = connection.execute(
                    "SELECT intent_id, target_version, state, batch_id FROM write_intents "
                    "WHERE state='RUNNING' ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            finally:
                connection.close()
            if row is not None:
                return dict(row)
        except sqlite3.Error:
            pass
        time.sleep(0.1)
    raise RuntimeError("did not observe a durable RUNNING write intent")


def db_facts() -> dict[str, Any]:
    connection = sqlite3.connect(str(DEMO_DATA / "slo.db"))
    connection.row_factory = sqlite3.Row
    try:
        version = connection.execute("SELECT COALESCE(MAX(version), 0) AS v FROM versions").fetchone()["v"]
        events = connection.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]
        running = connection.execute(
            "SELECT COUNT(*) AS c FROM write_intents WHERE state='RUNNING'"
        ).fetchone()["c"]
        abandoned = connection.execute(
            "SELECT COUNT(*) AS c FROM write_intents WHERE state='ABANDONED'"
        ).fetchone()["c"]
        return {
            "version": version,
            "events": events,
            "running_intents": running,
            "abandoned_intents": abandoned,
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()

    small_path = SAMPLES / "events_small.jsonl"
    large_path = SAMPLES / "events_large.jsonl"
    if not small_path.exists() or not large_path.exists():
        subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_samples.py")], check=True)
    if DEMO_DATA.exists() and not args.keep_data:
        shutil.rmtree(DEMO_DATA)
    DEMO_DATA.mkdir(parents=True, exist_ok=True)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {"scenarios": {}, "forced_termination": {}}
    process, base_url, log_handle = start_server()
    try:
        with httpx.Client(base_url=base_url, timeout=120.0) as client:
            health0 = client.get("/healthz").json()
            report["initial_health"] = health0
            print(f"server={base_url} initial_version={health0['version']}")

            for key, path, service in (
                ("small", small_path, "checkout-small"),
                ("large", large_path, "checkout-large"),
            ):
                ingest_file(client, path, service)
                health = client.get("/healthz").json()
                windows_response = client.get(
                    f"/v1/services/{service}/windows",
                    params={"windows": "5m,30m,1h", "sli_type": "availability"},
                )
                windows_response.raise_for_status()
                windows = windows_response.json()
                summary = client.get(
                    f"/v1/services/{service}/budget-summary",
                    params={"sli_type": "availability", "include_rolling": "false"},
                )
                summary.raise_for_status()
                summary_payload = summary.json()
                alerts = client.get("/v1/alerts", params={"service": service})
                alerts.raise_for_status()
                scenario = {
                    "version": windows["version"],
                    "service_total_events": summary_payload["service_total"]["total_events"],
                    "health": health,
                    "windows": [compact_window(item) for item in windows["windows"]],
                    "budget_summary": {
                        "service_total": summary_payload["service_total"],
                        "consistency_checks": summary_payload["consistency_checks"],
                        "tumbling_windows": {
                            name: {
                                "slice_count": item["slice_count"],
                                "sum_bad_events": item["sum_bad_events"],
                                "sum_consumed_error_budget": item[
                                    "sum_consumed_error_budget"
                                ],
                                "equals_service_total": item["equals_service_total"],
                            }
                            for name, item in summary_payload["tumbling_windows"].items()
                        },
                    },
                    "alerts": alerts.json()["alerts"],
                }
                report["scenarios"][key] = scenario
                print(
                    f"[{key}] version={scenario['version']} "
                    f"events={scenario['service_total_events']} "
                    f"alert={scenario['alerts'][0]['status']}"
                )
                for item in scenario["windows"]:
                    print(
                        f"  {item['window']:>3} SLI={item['sli']:.6f} "
                        f"burn={item['budget_consumption_ratio']:.2f}x "
                        f"sample={'yes' if item['sampling_used'] else 'no'} "
                        f"rate={item['sample_rate']} ci={item['confidence_interval']}"
                    )
                print(
                    "  consistency="
                    + json.dumps(scenario["budget_summary"]["consistency_checks"], sort_keys=True)
                )

            late_response = client.post(
                "/v1/events",
                json={
                    "batch_id": "demo-late",
                    "events": [
                        {
                            "event_id": "demo-late-000",
                            "timestamp": "2026-01-15T10:00:00.000Z",
                            "service": "checkout-large",
                            "sli_type": "availability",
                            "good": False,
                        }
                    ],
                },
            )
            rejections = client.get(
                "/v1/rejections", params={"service": "checkout-large"}
            )
            rejections.raise_for_status()
            report["late_event_check"] = {
                "http_status": late_response.status_code,
                "response": late_response.json(),
                "rejections": rejections.json(),
            }
            print(
                f"late-event check: HTTP {late_response.status_code} "
                f"code={late_response.json()['error']['code']} "
                f"recorded={rejections.json()['count']}"
            )

            pre_crash = db_facts()
            report["forced_termination"]["pre_crash"] = pre_crash
            print(f"pre-crash facts version={pre_crash['version']} events={pre_crash['events']}")
    finally:
        stop_server(process, log_handle)

    process, base_url, log_handle = start_server(stall_ms=15_000)
    crash_batch = []
    crash_time = datetime(2026, 1, 15, 12, 1, 0, tzinfo=timezone.utc)
    for index in range(5):
        crash_batch.append(
            {
                "event_id": f"forced-crash-{index:03d}",
                "timestamp": (crash_time + timedelta(milliseconds=index * 100)).isoformat(
                    timespec="milliseconds"
                ).replace("+00:00", "Z"),
                "service": "checkout-large",
                "sli_type": "availability",
                "good": True,
            }
        )
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(
        lambda: httpx.post(
            f"{base_url}/v1/events",
            json={"batch_id": "forced-crash", "events": crash_batch},
            timeout=30.0,
        )
    )
    try:
        running = wait_for_running_intent()
        report["forced_termination"]["observed_running_intent"] = running
        print(
            "observed durable RUNNING intent "
            f"id={running['intent_id']} target_version={running['target_version']}"
        )
        process.kill()
        process.wait(timeout=8)
    finally:
        try:
            future.result(timeout=5)
        except Exception as exc:
            report["forced_termination"]["client_error_after_kill"] = type(exc).__name__
        executor.shutdown(wait=False, cancel_futures=True)
        if process.poll() is None:
            process.kill()
    log_handle.close()

    after_kill = db_facts()
    report["forced_termination"]["after_kill"] = after_kill
    if after_kill["version"] != pre_crash["version"]:
        raise RuntimeError("killed process unexpectedly committed a version")

    process, base_url, log_handle = start_server()
    try:
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            recovery_response = client.get("/v1/recovery")
            recovery_response.raise_for_status()
            recovery = recovery_response.json()
            health = client.get("/healthz").json()
            summary = client.get(
                "/v1/services/checkout-large/budget-summary",
                params={"sli_type": "availability", "include_rolling": "false"},
            )
            summary.raise_for_status()
            after_restart = {
                "health": health,
                "service_total_events": summary.json()["service_total"]["total_events"],
                "recovery": recovery,
            }
            report["forced_termination"]["after_restart"] = after_restart
            print(
                "recovery report: "
                f"last_complete_version={recovery['last_complete_version']} "
                f"abandoned={recovery['intents_abandoned']} "
                f"restart_version={health['version']}"
            )
            if recovery["last_complete_version"] != pre_crash["version"]:
                raise RuntimeError("recovery report version does not match last complete version")
            if health["version"] != pre_crash["version"]:
                raise RuntimeError("restart exposed an uncommitted version")
            if health["event_count"] != pre_crash["events"]:
                raise RuntimeError("restart exposed partially committed events")
    finally:
        stop_server(process, log_handle)

    report_path = ARTIFACTS / "e2e_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {report_path}")
    print(
        "forced termination summary: "
        f"version_before={pre_crash['version']} version_after={db_facts()['version']} "
        f"abandoned_intents={db_facts()['abandoned_intents']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
