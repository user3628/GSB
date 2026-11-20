from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
ARTIFACTS = ROOT / "artifacts"
DEMO_DIR = ARTIFACTS / "demo"
DB_PATH = DEMO_DIR / "index.db"
MARKER_PATH = DEMO_DIR / "merge-failpoint.json"
REPORT_PATH = ARTIFACTS / "verification_report.json"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_health(client: httpx.Client, process: subprocess.Popen, attempts: int = 100) -> None:
    last_error: Exception | None = None
    for _ in range(attempts):
        if process.poll() is not None:
            raise RuntimeError(f"server exited during startup with code {process.returncode}")
        try:
            response = client.get("/health", timeout=1.0)
            if response.status_code == 200:
                return
        except Exception as exc:  # startup races are expected
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def start_server(port: int, *, failpoint: bool, no_proxy: dict[str, str]) -> tuple[subprocess.Popen, Any]:
    env = os.environ.copy()
    env.update(no_proxy)
    env.update(
        {
            "INDEX_DB_PATH": str(DB_PATH),
            "INDEX_CONFIG_PATH": str(SAMPLES / "index_config.json"),
            "PYTHONPATH": str(ROOT),
        }
    )
    if failpoint:
        env.update(
            {
                "INVERTED_INDEX_FAILPOINT": "merge:after_copy",
                "INVERTED_INDEX_FAILPOINT_HOLD_SECONDS": "60",
                "INVERTED_INDEX_FAILPOINT_MARKER": str(MARKER_PATH),
            }
        )
    log_path = DEMO_DIR / ("server-crash.log" if failpoint else "server.log")
    log_file = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "inverted_index.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    log_file.close()
    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)
    wait_for_health(client, process)
    return process, client


def stop_server(process: subprocess.Popen, client: httpx.Client) -> None:
    client.close()
    if process.poll() is None:
        process.kill()
        process.wait(timeout=10)


def search(client: httpx.Client, version: int, field: str, term: str, limit: int = 100) -> dict[str, Any]:
    response = client.post(
        "/v1/search",
        json={
            "version": version,
            "operator": "AND",
            "clauses": [{"kind": "term", "field": field, "term": term}],
            "limit": limit,
            "offset": 0,
        },
    )
    response.raise_for_status()
    return response.json()


def main() -> int:
    if DEMO_DIR.exists():
        shutil.rmtree(DEMO_DIR)
    DEMO_DIR.mkdir(parents=True)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

    initial = json.loads((SAMPLES / "batches/001_initial.json").read_text(encoding="utf-8"))
    incremental = json.loads((SAMPLES / "batches/002_incremental.json").read_text(encoding="utf-8"))
    port = free_port()
    no_proxy = {"NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"}
    process, client = start_server(port, failpoint=False, no_proxy=no_proxy)
    summary: dict[str, Any] = {
        "started_at": now(),
        "seed": json.loads((SAMPLES / "manifest.json").read_text(encoding="utf-8"))["seed"],
        "checks": {},
    }
    try:
        initial_response = client.post("/v1/batches", json=initial)
        initial_response.raise_for_status()
        old_version = initial_response.json()["version"]
        check(old_version == 1, "initial batch must create version 1")

        old_quantum = search(client, old_version, "title", "quantum")
        old_local = search(client, old_version, "tags", "local")
        old_phrase = client.post(
            "/v1/search",
            json={
                "version": old_version,
                "clauses": [
                    {"kind": "phrase", "field": "body", "terms": ["local", "search", "index"]}
                ],
            },
        ).json()
        check(old_quantum["total"] == 0, "quantum must be absent in version 1")
        check(old_phrase["total"] > 0, "phrase fixture must match in version 1")

        incremental_response = client.post("/v1/batches", json=incremental)
        incremental_response.raise_for_status()
        new_version = incremental_response.json()["version"]
        check(new_version == old_version + 1, "incremental batch must create version 2")

        old_quantum_after = search(client, old_version, "title", "quantum")
        old_local_after = search(client, old_version, "tags", "local")
        new_quantum = search(client, new_version, "title", "quantum")
        migrated = search(client, new_version, "tags", "migrated")
        deleted_at_old = client.get(f"/v1/documents/doc-004", params={"version": old_version})
        deleted_at_new = client.get(f"/v1/documents/doc-004", params={"version": new_version})
        postings = client.get(
            "/v1/postings",
            params={"path": "title", "term": "QUANTUM", "version": new_version},
        )
        postings.raise_for_status()

        check(old_quantum_after == old_quantum, "old quantum result changed after commit")
        check(old_local_after == old_local, "old local result changed after commit")
        check(
            {item["_id"] for item in new_quantum["items"]} == {"doc-002", "doc-013"},
            "new version must include both incremental quantum documents",
        )
        check(
            {item["_id"] for item in migrated["items"]} == {"doc-007"},
            "new version must include the migrated tag",
        )
        check(deleted_at_old.status_code == 200, "deleted document must remain visible at old version")
        check(deleted_at_new.status_code == 404, "deleted document must disappear at new version")
        check(
            {entry["doc_id"] for entry in postings.json()["entries"]} == {"doc-002", "doc-013"},
            "posting lookup must return normalized term entries",
        )

        segments_before = client.get("/v1/index/segments").json()["segments"]
        active_before = [item for item in segments_before if item["status"] == "active"]
        check(len(active_before) == 2, "two committed segments must be active before merge crash")

        summary["versions"] = {"initial": old_version, "incremental": new_version}
        summary["checks"].update(
            {
                "old_query_unchanged_after_commit": True,
                "new_query_sees_incremental_documents": True,
                "deleted_document_visible_only_at_old_version": True,
                "normalized_posting_lookup": True,
                "active_segments_before_merge": [item["segment_id"] for item in active_before],
            }
        )
        summary["old_version_result_ids"] = [item["_id"] for item in old_quantum_after["items"]]
        summary["new_version_quantum_ids"] = [item["_id"] for item in new_quantum["items"]]
    finally:
        stop_server(process, client)

    # Start with a merge failpoint, issue the merge, wait for the persisted
    # in-process marker, then use the OS process kill API.
    if MARKER_PATH.exists():
        MARKER_PATH.unlink()
    crash_port = free_port()
    crash_process, crash_client = start_server(crash_port, failpoint=True, no_proxy=no_proxy)
    request_result: dict[str, Any] = {}

    def request_merge() -> None:
        try:
            response = crash_client.post("/v1/index/merge", json={"source_segment_ids": None}, timeout=30)
            request_result["status_code"] = response.status_code
            request_result["body"] = response.text
        except Exception as exc:
            request_result["exception"] = type(exc).__name__

    thread = threading.Thread(target=request_merge)
    thread.start()
    marker: dict[str, Any] | None = None
    for _ in range(200):
        if MARKER_PATH.exists():
            marker = json.loads(MARKER_PATH.read_text(encoding="utf-8"))
            break
        if crash_process.poll() is not None:
            break
        time.sleep(0.05)
    check(marker is not None, "merge failpoint marker was not observed")
    check(crash_process.poll() is None, "server exited before the external kill")
    crash_process.kill()
    crash_process.wait(timeout=10)
    kill_returncode = crash_process.returncode
    thread.join(timeout=10)
    crash_client.close()
    check(kill_returncode != 0, "force-killed process should have a non-zero return code")

    restart_port = free_port()
    restart_process, restart_client = start_server(restart_port, failpoint=False, no_proxy=no_proxy)
    try:
        recovery = restart_client.get("/v1/recovery/report")
        recovery.raise_for_status()
        recovery_report = recovery.json()
        actions = recovery_report["actions"]
        check(recovery_report["pending_operations_found"] >= 1, "restart must find the pending merge intent")
        check(any(item["decision"] == "discarded-uncommitted-merge" for item in actions), "merge artifact was not discarded")
        check(recovery_report["last_committed_version"] == new_version, "committed version changed after recovery")

        old_after_restart = search(restart_client, old_version, "title", "quantum")
        new_after_restart = search(restart_client, new_version, "title", "quantum")
        segments_after = restart_client.get("/v1/index/segments").json()["segments"]
        active_after = [item for item in segments_after if item["status"] == "active"]
        check(old_after_restart == old_quantum, "old query changed after merge crash recovery")
        check(
            {item["_id"] for item in new_after_restart["items"]} == {"doc-002", "doc-013"},
            "new query changed after merge crash recovery",
        )
        check(len(active_after) == 2, "recovery must leave both source segments active")

        summary["merge_crash"] = {
            "marker": marker,
            "process_returncode": kill_returncode,
            "client_observation": request_result,
            "recovery_report": recovery_report,
            "active_segments_after_restart": [item["segment_id"] for item in active_after],
        }
        summary["checks"].update(
            {
                "merge_intent_persisted_before_kill": True,
                "merge_artifact_discarded_on_restart": True,
                "old_query_unchanged_after_recovery": True,
                "new_query_unchanged_after_recovery": True,
            }
        )
    finally:
        stop_server(restart_process, restart_client)

    summary["completed_at"] = now()
    summary["status"] = "PASS"
    REPORT_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    print(f"Report written to {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
