from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def api(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: float = 20.0,
) -> tuple[int, dict[str, Any]]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8"))
        finally:
            exc.close()


def wait_health(base_url: str) -> None:
    deadline = time.time() + 10
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            status, payload = api(base_url + "/health", timeout=1)
            if status == 200 and payload.get("ok"):
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.05)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def start_server(db: Path, *, build_delay_ms: int = 0) -> tuple[subprocess.Popen, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    env["JCS_BUILD_DELAY_MS"] = str(build_delay_ms)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "joincost.cli",
            "serve",
            "--db",
            str(db),
            "--workspace",
            str(ROOT),
            "--port",
            "0",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    line = process.stdout.readline()
    if not line:
        stderr = process.stderr.read() if process.stderr else ""
        raise RuntimeError(f"server failed to start: {stderr}")
    startup = json.loads(line)
    wait_health(startup["url"])
    return process, startup["url"]


def stop_server(process: subprocess.Popen, *, kill: bool = False) -> None:
    if process.poll() is not None:
        return
    if kill:
        process.kill()
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def load_predicates() -> list[dict[str, Any]]:
    return json.loads((ROOT / "samples" / "predicates.json").read_text(encoding="utf-8"))


def plan_request(request_id: str, version: int) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "snapshot_version": version,
        "predicates": load_predicates(),
        "options": {"max_relations": 8, "tolerance_ppm": 1000},
    }


def plan_core(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "ordered_relations": plan["ordered_relations"],
        "cost": plan["cost"],
        "steps": plan["plan"]["steps"],
        "predicate_selectivity": plan["predicate_selectivity"],
        "search": plan["search"],
    }


def main() -> int:
    samples = ROOT / "samples"
    required = [
        samples / "stats_v2.bin",
        samples / "stats_v2.jsonl",
        samples / "stats_v1_legacy.bin",
        samples / "stats_v1_legacy.jsonl",
        samples / "stats_v3_unsupported.jsonl",
        samples / "stats_replacement.bin",
        samples / "predicates.json",
    ]
    if not all(path.is_file() for path in required):
        subprocess.run([sys.executable, str(samples / "generate_samples.py")], cwd=ROOT, check=True)

    work = Path(tempfile.mkdtemp(prefix="joincost-demo-"))
    summary: dict[str, Any] = {
        "ok": True,
        "workspace": str(ROOT),
        "temporary_run_directory": str(work),
        "network": {"bind": "127.0.0.1", "external_network": False},
    }
    main_db = work / "main.db"
    process = None
    try:
        process, base = start_server(main_db)
        status, imported_binary = api(
            base + "/v1/snapshots/import", method="POST", body={"path": "samples/stats_v2.bin"}
        )
        assert status == 200, imported_binary
        status, binary_plan = api(
            base + "/v1/plans", method="POST", body=plan_request("demo-binary", 1001)
        )
        assert status == 200, binary_plan

        status, imported_jsonl = api(
            base + "/v1/snapshots/import", method="POST", body={"path": "samples/stats_v2.jsonl"}
        )
        assert status == 200, imported_jsonl
        status, jsonl_plan = api(
            base + "/v1/plans", method="POST", body=plan_request("demo-jsonl", 1001)
        )
        assert status == 200, jsonl_plan
        core_equal = plan_core(binary_plan) == plan_core(jsonl_plan)
        assert core_equal
        summary["interchange"] = {
            "binary_decision": imported_binary["import_report"]["decision"],
            "jsonl_decision": imported_jsonl["import_report"]["decision"],
            "semantic_digest_equal": imported_binary["import_report"]["digest"]
            == imported_jsonl["import_report"]["digest"],
            "duplicate_semantic_import": imported_jsonl["replacement"]["duplicate"],
            "plan_core_bit_equal": core_equal,
            "ordered_relations": binary_plan["ordered_relations"],
            "cost": binary_plan["cost"],
            "predicate_selectivity": binary_plan["predicate_selectivity"],
        }

        corrupt = bytearray((samples / "stats_v2.bin").read_bytes())
        corrupt[-1] ^= 0xFF
        status, corrupt_result = api(
            base + "/v1/snapshots/import",
            method="POST",
            body={
                "content_base64": base64.b64encode(bytes(corrupt)).decode("ascii"),
                "source": "injected-corrupt.bin",
            },
        )
        status_after, snapshot_after = api(base + "/v1/snapshot")
        summary["corruption_injection"] = {
            "http_status": status,
            "error_code": corrupt_result["error"]["code"],
            "current_version_unchanged": snapshot_after["current_snapshot"]["snapshot_version"]
            == 1001,
        }
        assert status == 422 and corrupt_result["error"]["code"] == "E_FORMAT_CORRUPT"
        assert status_after == 200

        status, unsupported = api(
            base + "/v1/snapshots/import",
            method="POST",
            body={"path": "samples/stats_v3_unsupported.jsonl"},
        )
        reports = api(base + "/v1/imports")[1]["imports"]
        summary["unsupported_version"] = {
            "http_status": status,
            "error_code": unsupported["error"]["code"],
            "rejected_report_recorded": any(
                item["code"] == "E_FORMAT_UNSUPPORTED"
                and item["report_json"]["decision"] == "REJECT"
                for item in reports
            ),
        }
        assert status == 422 and unsupported["error"]["code"] == "E_FORMAT_UNSUPPORTED"

        status, replaced = api(
            base + "/v1/snapshots/import",
            method="POST",
            body={"path": "samples/stats_replacement.bin"},
        )
        assert status == 200, replaced
        old_plan_status, old_plan = api(base + "/v1/plans/demo-binary")
        status, new_plan = api(
            base + "/v1/plans", method="POST", body=plan_request("demo-replacement", 1002)
        )
        assert status == 200, new_plan
        summary["snapshot_replacement"] = {
            "previous_version": replaced["replacement"]["previous_version"],
            "current_version": replaced["replacement"]["current_version"],
            "old_plan_still_bound_to": old_plan["snapshot_version"],
            "old_plan_retrievable": old_plan_status == 200,
            "new_plan_bound_to": new_plan["snapshot_version"],
            "old_digest": old_plan["snapshot_digest"],
            "new_digest": new_plan["snapshot_digest"],
        }
        assert old_plan["snapshot_version"] == 1001
        assert new_plan["snapshot_version"] == 1002
        stop_server(process)
        process = None
    finally:
        if process is not None:
            stop_server(process)

    legacy_results = []
    for filename in ("stats_v1_legacy.bin", "stats_v1_legacy.jsonl"):
        legacy_process = None
        try:
            legacy_process, legacy_base = start_server(work / f"legacy-{filename}.db")
            status, result = api(
                legacy_base + "/v1/snapshots/import",
                method="POST",
                body={"path": f"samples/{filename}"},
            )
            assert status == 200, result
            legacy_results.append(
                {
                    "file": filename,
                    "decision": result["import_report"]["decision"],
                    "degradations": result["import_report"]["degradations"],
                }
            )
        finally:
            if legacy_process is not None:
                stop_server(legacy_process)
    summary["legacy_compatibility"] = legacy_results

    crash_db = work / "crash.db"
    crash_process = None
    try:
        crash_process, crash_base = start_server(crash_db, build_delay_ms=5000)
        status, imported = api(
            crash_base + "/v1/snapshots/import",
            method="POST",
            body={"path": "samples/stats_v2.bin"},
        )
        assert status == 200, imported
        holder: dict[str, Any] = {}

        def submit_long_build() -> None:
            try:
                holder["response"] = api(
                    crash_base + "/v1/plans",
                    method="POST",
                    body=plan_request("crash-request", 1001),
                    timeout=30,
                )
            except Exception as exc:
                holder["client_error"] = type(exc).__name__

        thread = threading.Thread(target=submit_long_build, daemon=True)
        thread.start()
        deadline = time.time() + 5
        saw_running = False
        while time.time() < deadline:
            try:
                status, request_state = api(
                    crash_base + "/v1/requests/crash-request", timeout=1
                )
                if status == 200 and request_state["request"]["status"] == "RUNNING":
                    saw_running = True
                    break
            except Exception:
                pass
            time.sleep(0.05)
        if not saw_running:
            raise RuntimeError("crash demo never observed the RUNNING intent")
        stop_server(crash_process, kill=True)
        crash_process = None
        thread.join(timeout=3)
        summary["forced_termination"] = {
            "pending_request_observed": saw_running,
            "process_killed": True,
            "client_disconnected": "client_error" in holder,
        }
    finally:
        if crash_process is not None:
            stop_server(crash_process, kill=True)

    recovered_process = None
    try:
        recovered_process, recovered_base = start_server(crash_db)
        status, recovery = api(recovered_base + "/v1/recovery")
        assert status == 200, recovery
        status, aborted = api(recovered_base + "/v1/plans/crash-request")
        status_snapshot, snapshot = api(recovered_base + "/v1/snapshot")
        assert status == 410, aborted
        summary["recovery"] = {
            "startup_report": recovery["startup_recovery"],
            "latest_report": recovery["latest_recovery_report"],
            "aborted_request_http_status": status,
            "aborted_request_error_code": aborted["error"]["code"],
            "current_snapshot_after_recovery": snapshot["current_snapshot"]["snapshot_version"],
            "snapshot_difference": recovery["latest_recovery_report"]["differences"],
        }
        assert status == 410
        assert aborted["error"]["code"] == "E_REQUEST_ABORTED"
        assert status_snapshot == 200
        assert snapshot["current_snapshot"]["snapshot_version"] == 1001
    finally:
        if recovered_process is not None:
            stop_server(recovered_process)

    print(json.dumps(summary, indent=2, sort_keys=True))
    report_path = ROOT / "artifacts" / "demo_report.json"
    report_path.parent.mkdir(exist_ok=True)
    report_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

