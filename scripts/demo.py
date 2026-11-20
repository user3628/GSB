#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shardroute.datastore import SQLiteDataStore  # noqa: E402
from shardroute.util import atomic_write_json, read_json, utc_now  # noqa: E402


SAMPLES = ROOT / "samples"


def http_json(base_url: str, method: str, path: str, payload: Any = None, timeout: float = 10):
    body = None
    headers: dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(base_url + path, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_for_server(
    process: subprocess.Popen,
    port_file: Path,
    stdout_log: Path,
    stderr_log: Path,
    timeout: float = 20.0,
) -> str:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                "server exited before readiness\n"
                f"exit_code={process.returncode}\n"
                f"stdout={stdout_log.read_text(encoding='utf-8', errors='replace')}\n"
                f"stderr={stderr_log.read_text(encoding='utf-8', errors='replace')}"
            )
        if port_file.exists():
            try:
                descriptor = json.loads(port_file.read_text(encoding="utf-8"))
                base_url = f"http://{descriptor['host']}:{descriptor['port']}"
                status, _ = http_json(base_url, "GET", "/health", timeout=2)
                if status == 200:
                    return base_url
            except Exception as exc:
                last_error = exc
        time.sleep(0.05)
    raise RuntimeError(f"server did not become ready: {last_error}")


def start_server(work_dir: Path, metadata_db: Path, data_dir: Path, label: str):
    port_file = work_dir / f"{label}.port.json"
    port_file.unlink(missing_ok=True)
    stdout_log = work_dir / f"{label}.stdout.log"
    stderr_log = work_dir / f"{label}.stderr.log"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    with stdout_log.open("wb") as stdout_handle, stderr_log.open("wb") as stderr_handle:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "shardroute",
                "serve",
                "--metadata-db",
                str(metadata_db),
                "--data-dir",
                str(data_dir),
                "--host",
                "127.0.0.1",
                "--port",
                "0",
                "--port-file",
                str(port_file),
            ],
            cwd=str(ROOT),
            env=environment,
            stdout=stdout_handle,
            stderr=stderr_handle,
        )
    base_url = wait_for_server(process, port_file, stdout_log, stderr_log)
    return process, base_url, stdout_log, stderr_log


def stop_gracefully(process: subprocess.Popen, base_url: str) -> dict[str, Any] | None:
    if process.poll() is not None:
        return None
    try:
        status, body = http_json(base_url, "POST", "/shutdown", {})
        if status == 200:
            process.wait(timeout=15)
            return body
    except Exception:
        pass
    if process.poll() is None:
        process.kill()
        process.wait(timeout=10)
    return None


def select_route_fields(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_id": result["request_id"],
        "routing_version": result["routing_version"],
        "plan_checksum": result["plan_checksum"],
        "physical_tables": result["physical_tables"],
        "matched_shards": result["matched_shards"],
        "cache": result["cache"],
        "route_trace": result["route_trace"],
        "rows": result["rows"],
        "row_sources": result["row_sources"],
        "conflicts": result["conflicts"],
        "page": result["page"],
    }


def cache_layout(state: dict[str, Any]) -> dict[str, Any]:
    caches = state.get("caches") or {}
    tables = caches.get("tables", {})
    return {
        "routing_version": caches.get("routing_version"),
        "tables": {
            name: {
                "cache_version": entry["cache_version"],
                "algorithm_signature": entry["algorithm_signature"],
                "physical_tables": entry["physical_tables"],
                "boundaries": entry["boundaries"],
            }
            for name, entry in sorted(tables.items())
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the deterministic shard routing demo")
    parser.add_argument("--work-dir", default=str(ROOT / "artifacts" / "demo-work"))
    parser.add_argument(
        "--report", default=str(ROOT / "artifacts" / "demo-report.json")
    )
    parser.add_argument(
        "--recovery-report",
        default=str(ROOT / "artifacts" / "recovery-report.json"),
    )
    args = parser.parse_args(argv)

    work_dir = Path(args.work_dir).resolve()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    data_dir = work_dir / "data"
    metadata_db = work_dir / "metadata.sqlite3"
    data_dir.mkdir(parents=True, exist_ok=True)
    seed_document = read_json(SAMPLES / "seed_data.json")
    snapshot_document = read_json(SAMPLES / "snapshot_v1.json")
    patch_document = read_json(SAMPLES / "patch_v2_orders_hash.json")
    SQLiteDataStore(data_dir).initialize(seed_document)

    report: dict[str, Any] = {
        "generated_at": utc_now(),
        "work_dir": str(work_dir),
        "samples": {
            "snapshot": str(SAMPLES / "snapshot_v1.json"),
            "patch": str(SAMPLES / "patch_v2_orders_hash.json"),
            "seed": str(SAMPLES / "seed_data.json"),
        },
    }
    first_process: subprocess.Popen | None = None
    second_process: subprocess.Popen | None = None
    try:
        first_process, base_url, stdout1, stderr1 = start_server(
            work_dir, metadata_db, data_dir, "server-before-crash"
        )
        report["first_process"] = {"pid": first_process.pid, "base_url": base_url}

        status, load_result = http_json(base_url, "POST", "/snapshot", snapshot_document)
        if status != 200:
            raise RuntimeError(f"full snapshot load failed: {status} {load_result}")
        report["full_snapshot_load"] = load_result

        v1_request = {
            "request_id": "demo-v1-orders-user-7",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
            "routing_version": 1,
            "limit": 10,
            "offset": 0,
        }
        status, v1_result = http_json(base_url, "POST", "/query", v1_request)
        if status != 200:
            raise RuntimeError(f"v1 query failed: {status} {v1_result}")
        report["v1_route"] = select_route_fields(v1_result)

        status, patch_result = http_json(base_url, "POST", "/snapshot/patch", patch_document)
        if status != 200:
            raise RuntimeError(f"patch failed: {status} {patch_result}")
        report["incremental_patch"] = patch_result
        report["cache_after_patch"] = http_json(base_url, "GET", "/cache")[1]

        v2_request = {
            "request_id": "demo-v2-orders-user-7",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
            "routing_version": 2,
            "limit": 10,
            "offset": 0,
        }
        status, v2_result = http_json(base_url, "POST", "/query", v2_request)
        if status != 200:
            raise RuntimeError(f"v2 query failed: {status} {v2_result}")
        report["v2_route"] = select_route_fields(v2_result)

        injection_evidence = []
        for index, injection in enumerate(
            [
                "invalid_shard_key",
                "empty_physical_mapping",
                "version_rollback",
                "range_overlap",
                "execution_interrupt",
            ]
        ):
            status, body = http_json(
                base_url,
                "POST",
                "/injections",
                {
                    "type": injection,
                    "request_id": f"demo-injection-{index}",
                    "logical_table": "orders",
                    "query_type": "scan",
                    "routing_version": 2,
                    "limit": 0,
                },
            )
            injection_evidence.append(
                {
                    "injection": injection,
                    "http_status": status,
                    "business_code": body.get("error", {}).get("code"),
                    "error": body.get("error"),
                }
            )
        report["exception_injections"] = injection_evidence

        status, before_crash_state = http_json(base_url, "GET", "/state")
        if status != 200:
            raise RuntimeError(f"state read failed: {status} {before_crash_state}")
        report["state_before_crash"] = {
            "active_version": before_crash_state["active_version"],
            "active_checksum": before_crash_state["active_checksum"],
            "snapshot_versions": [item["version"] for item in before_crash_state["snapshots"]],
            "cache_layout": cache_layout(before_crash_state),
            "metadata_counts": before_crash_state["metadata_counts"],
        }

        crash_request = {
            "request_id": "demo-crash-mid-merge",
            "logical_table": "orders",
            "query_type": "scan",
            "routing_version": 2,
            "limit": 100,
            "inject": "crash_mid_merge",
        }
        crash_connection_error = None
        try:
            http_json(base_url, "POST", "/query", crash_request, timeout=10)
        except (URLError, ConnectionError, TimeoutError, OSError) as exc:
            crash_connection_error = repr(exc)
        first_process.wait(timeout=15)
        report["forced_termination"] = {
            "request": crash_request,
            "connection_error": crash_connection_error,
            "pid": first_process.pid,
            "exit_code": first_process.returncode,
            "termination_observed": first_process.returncode == 86,
        }
        if first_process.returncode != 86:
            raise RuntimeError(
                f"expected forced termination exit code 86, got {first_process.returncode}; "
                f"stderr={stderr1.read_text(encoding='utf-8', errors='replace')}"
            )

        second_process, second_url, stdout2, stderr2 = start_server(
            work_dir, metadata_db, data_dir, "server-after-recovery"
        )
        report["second_process"] = {"pid": second_process.pid, "base_url": second_url}
        status, recovery_envelope = http_json(second_url, "GET", "/recovery/report")
        if status != 200:
            raise RuntimeError(f"recovery report failed: {status} {recovery_envelope}")
        recovery_report = recovery_envelope["report"]
        status, after_recovery_state = http_json(second_url, "GET", "/state")
        if status != 200:
            raise RuntimeError(f"post-recovery state failed: {status}")
        status, retry_error = http_json(second_url, "POST", "/query", crash_request)
        if status != 503:
            raise RuntimeError(f"crashed request replay did not return 503: {status} {retry_error}")

        before_layout = report["state_before_crash"]["cache_layout"]
        after_layout = cache_layout(after_recovery_state)
        diff = {
            "active_version_before": report["state_before_crash"]["active_version"],
            "active_version_after": after_recovery_state["active_version"],
            "active_checksum_before": report["state_before_crash"]["active_checksum"],
            "active_checksum_after": after_recovery_state["active_checksum"],
            "active_version_restored": (
                report["state_before_crash"]["active_version"]
                == after_recovery_state["active_version"]
            ),
            "snapshot_versions_before": report["state_before_crash"]["snapshot_versions"],
            "snapshot_versions_after": [
                item["version"] for item in after_recovery_state["snapshots"]
            ],
            "cache_layout_equivalent": before_layout["tables"] == after_layout["tables"],
            "running_query_batches_after": after_recovery_state["metadata_counts"][
                "running_query_batches"
            ],
            "aborted_query_request_ids": [
                item["request_id"]
                for item in recovery_report.get("aborted_query_batches", [])
            ],
            "unfinished_intents_aborted": recovery_report.get("aborted_intents", []),
            "crash_window_detected": recovery_report.get("crash_window_detected"),
            "complete_exit_report_available": recovery_report.get(
                "complete_exit_report_available"
            ),
            "retry_business_code": retry_error.get("error", {}).get("code"),
            "retry_http_status": status,
        }
        report["recovery"] = {
            "report": recovery_report,
            "state_after": {
                "active_version": after_recovery_state["active_version"],
                "active_checksum": after_recovery_state["active_checksum"],
                "snapshot_versions": [
                    item["version"] for item in after_recovery_state["snapshots"]
                ],
                "cache_layout": after_layout,
                "metadata_counts": after_recovery_state["metadata_counts"],
            },
            "recovery_diff": diff,
            "retry_error": retry_error,
        }
        report["graceful_shutdown"] = stop_gracefully(second_process, second_url)
        report["second_process_exit_code"] = second_process.returncode
        if second_process.returncode != 0:
            raise RuntimeError(
                f"graceful shutdown failed; stderr={stderr2.read_text(encoding='utf-8', errors='replace')}"
            )

        atomic_write_json(args.report, report)
        atomic_write_json(
            args.recovery_report,
            {
                "generated_at": utc_now(),
                "basis": "persisted SQLite metadata and intent rows after a real forced termination",
                "forced_termination": report["forced_termination"],
                "recovery_report": recovery_report,
                "state_diff": diff,
                "retry_error": retry_error,
            },
        )
    finally:
        if first_process is not None and first_process.poll() is None:
            first_process.kill()
            first_process.wait(timeout=10)
        if second_process is not None and second_process.poll() is None:
            second_process.kill()
            second_process.wait(timeout=10)

    print("=== 1. 全量快照加载 ===")
    print(
        json.dumps(
            {
                "active_version": report["full_snapshot_load"]["active_version"],
                "checksum": report["full_snapshot_load"]["checksum"],
                "rebuilt_tables": report["full_snapshot_load"]["rebuilt_tables"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print("\n=== 2. V1 路由轨迹与命中物理表 ===")
    print(
        json.dumps(
            {
                "physical_tables": report["v1_route"]["physical_tables"],
                "rows": report["v1_route"]["rows"],
                "route_trace": report["v1_route"]["route_trace"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print("\n=== 3. 仅 orders 分片算法的增量提交 ===")
    print(
        json.dumps(
            {
                "base_version": report["incremental_patch"]["base_version"],
                "active_version": report["incremental_patch"]["active_version"],
                "rebuilt_tables": report["incremental_patch"]["rebuilt_tables"],
                "reused_tables": report["incremental_patch"]["reused_tables"],
                "v2_physical_tables": report["v2_route"]["physical_tables"],
                "v2_rows": report["v2_route"]["rows"],
                "v2_route_trace": report["v2_route"]["route_trace"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print("\n=== 4. 异常注入业务码 ===")
    print(
        json.dumps(
            [
                {
                    "injection": item["injection"],
                    "http_status": item["http_status"],
                    "business_code": item["business_code"],
                }
                for item in report["exception_injections"]
            ],
            ensure_ascii=False,
            indent=2,
        )
    )
    print("\n=== 5. 强制终止与恢复差异 ===")
    print(
        json.dumps(
            {
                "forced_termination": report["forced_termination"],
                "recovery_diff": report["recovery"]["recovery_diff"],
                "retry_error": report["recovery"]["retry_error"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"\n完整报告: {Path(args.report).resolve()}")
    print(f"恢复差异报告: {Path(args.recovery_report).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

