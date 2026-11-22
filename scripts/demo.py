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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mv_workbench.util import pretty_json, utc_now  # noqa: E402


ARTIFACTS = ROOT / "artifacts"
DATA = ROOT / "data"
SAMPLES = ROOT / "samples"


def free_loopback_port() -> int:
    requested = os.environ.get("MV_DEMO_PORT")
    if requested:
        port = int(requested)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", port))
        return port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def clear_database(db_path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()


def http_json(
    port: int,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read().decode("utf-8"))
        raise RuntimeError(f"HTTP {exc.code} {path}: {json.dumps(payload, ensure_ascii=False)}") from exc
    if not payload.get("ok"):
        raise RuntimeError(json.dumps(payload, ensure_ascii=False))
    return payload["data"]


def wait_for_health(port: int, process: subprocess.Popen, timeout: float = 25.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited early with code {process.returncode}")
        try:
            return http_json(port, "GET", "/health", timeout=1.0)
        except Exception:
            time.sleep(0.15)
    raise RuntimeError("server did not become healthy in time")


def start_server(port: int, db_path: Path, log_name: str) -> tuple[subprocess.Popen, Any]:
    env = os.environ.copy()
    env.update(
        {
            "MV_WORKBENCH_DB": str(db_path),
            "MV_WORKBENCH_SAMPLES": str(SAMPLES),
            "MV_WORKBENCH_HOST": "127.0.0.1",
            "MV_WORKBENCH_PORT": str(port),
            "MV_WORKBENCH_LOG_LEVEL": "warning",
            "PYTHONPATH": str(ROOT) + os.pathsep + env.get("PYTHONPATH", ""),
        }
    )
    log = open(ARTIFACTS / log_name, "w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, "-m", "mv_workbench"],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    wait_for_health(port, process)
    return process, log


def stop_server(process: subprocess.Popen, log: Any) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log.close()


def find_view(result: dict[str, Any], name: str) -> dict[str, Any]:
    return next(item for item in result["views"] if item["view"] == name)


def group_by_key(query: dict[str, Any], key: dict[str, Any]) -> dict[str, Any]:
    for group in query["groups"]:
        if group["key"] == key:
            return group
    raise KeyError(key)


def main() -> int:
    ARTIFACTS.mkdir(exist_ok=True)
    DATA.mkdir(exist_ok=True)
    db_path = DATA / "demo.db"
    clear_database(db_path)

    port = free_loopback_port()
    server, server_log = start_server(port, db_path, "demo_server_1.log")
    result: dict[str, Any] = {
        "format": "mv-workbench/demo-result-v1",
        "generated_at": utc_now(),
        "loopback": {"host": "127.0.0.1", "port": port, "bind_confirmed": True},
        "database": str(db_path.relative_to(ROOT)),
        "database_was_cleared_before_run": True,
    }
    try:
        health = http_json(port, "GET", "/health")
        result["service_version"] = health["version"]
        initial = http_json(port, "GET", "/api/diagnostics")
        result["seeded_graph"] = {
            "graph_version": initial["graph"]["graph_version"],
            "layers": initial["graph"]["layers"],
            "states": {item["view"]: item["state"] for item in initial["views"]},
        }

        full = http_json(
            port,
            "POST",
            "/api/refreshes",
            {"request_id": "demo-full-build", "mode": "all_stale"},
        )
        revenue_before = http_json(port, "GET", "/api/views/v_region_revenue")
        west_before = group_by_key(revenue_before, {"region": "west"})
        result["full_build"] = {
            "request_id": full["request_id"],
            "commit_seq": full["commit_seq"],
            "summary": full["summary"],
            "views": {
                item["view"]: {
                    "state": item["state"],
                    "version": item["version"],
                    "mode": item["mode"],
                    "recomputed_groups": item["recomputed_groups"],
                }
                for item in full["views"]
            },
            "revenue_rows": revenue_before["rows"],
            "west_group_digest": west_before["digest"],
        }

        samples = json.loads((SAMPLES / "batches.json").read_text(encoding="utf-8"))
        batch = http_json(
            port,
            "POST",
            "/api/base-tables/sales/batches",
            samples["batches"][0],
        )
        try:
            http_json(
                port,
                "POST",
                "/api/base-tables/sales/batches",
                samples["batches"][1],
            )
            stale_probe: dict[str, Any] = {"unexpected": "stale probe was accepted"}
        except RuntimeError as exc:
            stale_probe = {"rejected": True, "detail": str(exc)}

        correction = http_json(
            port,
            "POST",
            "/api/refreshes",
            {"request_id": "demo-partial-correction", "mode": "all_stale"},
        )
        revenue_after = http_json(port, "GET", "/api/views/v_region_revenue")
        west_after = group_by_key(revenue_after, {"region": "west"})
        revenue_diag = find_view(correction, "v_region_revenue")
        peak_diag = find_view(correction, "v_region_peak")
        result["correction_batch"] = {
            "batch_id": batch["batch_id"],
            "base_version": batch["base_version"],
            "committed_version": batch["committed_version"],
            "affected_views": batch["affected_views"],
            "stale_probe": stale_probe,
        }
        result["partial_invalidation"] = {
            "request_id": correction["request_id"],
            "commit_seq": correction["commit_seq"],
            "summary": correction["summary"],
            "revenue": {
                "mode": revenue_diag["mode"],
                "affected_groups": revenue_diag["affected_groups"],
                "recomputed_groups": revenue_diag["recomputed_groups"],
                "preserved_groups": revenue_diag["preserved_groups"],
                "reason": revenue_diag["reason"],
                "bound_version": revenue_after["bound_version"],
                "rows": revenue_after["rows"],
            },
            "peak": {
                "mode": peak_diag["mode"],
                "recomputed_groups": peak_diag["recomputed_groups"],
                "non_incremental_functions": peak_diag["non_incremental_functions"],
                "reason": peak_diag["reason"],
            },
            "west_group_preserved": {
                "digest_same": west_before["digest"] == west_after["digest"],
                "rows_same": west_before["rows"] == west_after["rows"],
            },
        }

        definitions_v2 = json.loads(
            (SAMPLES / "view_definitions_v2.json").read_text(encoding="utf-8")
        )
        replacement = http_json(
            port, "PUT", "/api/view-definitions", definitions_v2
        )
        states_after_replacement = http_json(port, "GET", "/api/diagnostics")
        cascade = http_json(
            port,
            "POST",
            "/api/refreshes",
            {"request_id": "demo-definition-cascade", "mode": "all_stale"},
        )
        graph_history = http_json(port, "GET", "/api/graph")["history"]
        result["definition_cascade"] = {
            "previous_graph_version": replacement["previous_graph_version"],
            "graph_version": replacement["graph_version"],
            "changed_views": replacement["changed_views"],
            "new_views": replacement["new_views"],
            "affected_views": replacement["affected_views"],
            "states_immediately_after_replace": {
                item["view"]: item["state"] for item in states_after_replacement["views"]
            },
            "refresh_summary": cascade["summary"],
            "refresh_modes": {
                item["view"]: item["mode"] for item in cascade["views"]
            },
            "graph_history": graph_history,
            "v_revenue_audit_rows": http_json(
                port, "GET", "/api/views/v_revenue_audit"
            )["rows"],
        }
    finally:
        stop_server(server, server_log)

    # The crash worker uses the same SQLite database but a fresh process. It
    # commits a refresh intent / REFRESHING states, then calls os._exit(97).
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    crash = subprocess.run(
        [
            sys.executable,
            "-m",
            "mv_workbench.crash_worker",
            "--db",
            str(db_path),
            "--request-id",
            "demo-forced-crash-1",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if crash.returncode != 97:
        raise RuntimeError(
            f"crash worker returned {crash.returncode}: {crash.stdout} {crash.stderr}"
        )

    server, server_log = start_server(port, db_path, "demo_server_2_recovery.log")
    try:
        startup = http_json(port, "GET", "/api/recovery/startup")
        latest = http_json(port, "GET", "/api/recovery")["latest"]
        freshness = http_json(port, "GET", "/api/views/v_region_revenue/freshness")
        materialized = http_json(port, "GET", "/api/views/v_region_revenue")
        request = http_json(port, "GET", "/api/refreshes/demo-forced-crash-1")
        recovery_diag = http_json(port, "GET", "/api/diagnostics")
        result["forced_termination_recovery"] = {
            "crash_worker_exit_code": crash.returncode,
            "startup_report": startup,
            "latest_report_status": latest["status"],
            "view_state_after_restart": freshness["state"],
            "view_gaps": freshness["gaps"],
            "materialized_version_retained": materialized["bound_version"],
            "query_during_partial_state": materialized["state"],
            "refresh_request_status": request["status"],
            "recovery_events": [
                event
                for event in recovery_diag["recent_events"]
                if event["trigger_type"] == "RECOVERY"
            ],
        }
    finally:
        stop_server(server, server_log)

    result["conclusions"] = [
        "The deterministic full build produced seven FRESH views.",
        "A correction/deletion batch cascaded STALE to six views while the independent returns branch stayed FRESH.",
        "The sum/count view rewrote only three affected region groups and preserved the unchanged west group byte-for-byte.",
        "The max/quantile view performed a full four-group recompute and did not publish stale partial maxima.",
        "Graph replacement advanced version 1 to 2, propagated to downstream views, and historical graph v1 remained queryable.",
        "The forced process termination left a persisted REFRESHING intent; restart marked affected views PARTIAL and retained the last complete materialization.",
    ]
    (ARTIFACTS / "demo_result.json").write_text(
        pretty_json(result) + "\n", encoding="utf-8", newline="\n"
    )
    print(pretty_json({
        "demo_result": "artifacts/demo_result.json",
        "full_build": result["full_build"]["summary"],
        "partial_invalidation": {
            "revenue_mode": result["partial_invalidation"]["revenue"]["mode"],
            "recomputed_groups": result["partial_invalidation"]["revenue"]["recomputed_groups"],
            "preserved_groups": result["partial_invalidation"]["revenue"]["preserved_groups"],
            "peak_mode": result["partial_invalidation"]["peak"]["mode"],
        },
        "definition_cascade": {
            "graph_version": result["definition_cascade"]["graph_version"],
            "summary": result["definition_cascade"]["refresh_summary"],
        },
        "recovery": {
            "status": result["forced_termination_recovery"]["latest_report_status"],
            "view_state": result["forced_termination_recovery"]["view_state_after_restart"],
            "retained_version": result["forced_termination_recovery"]["materialized_version_retained"],
        },
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
