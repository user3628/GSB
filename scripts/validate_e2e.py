#!/usr/bin/env python3
"""Reproducible end-to-end validation over the local HTTP API.

The script starts uvicorn as a child process, drives deterministic JSON fixtures,
force-kills a second server during a batch transaction, restarts it, and compares
the recovered state with the pre-crash persisted state.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
ARTIFACTS = ROOT / "artifacts"


class APIError(RuntimeError):
    def __init__(self, status: int, payload: dict[str, Any]):
        super().__init__(f"HTTP {status}: {payload}")
        self.status = status
        self.payload = payload


class CheckLog:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def check(self, name: str, condition: bool, detail: Any = None) -> None:
        item = {"name": name, "ok": bool(condition), "detail": detail}
        self.items.append(item)
        if not condition:
            raise AssertionError(f"check failed: {name}; detail={detail!r}")


class HTTPClient:
    def __init__(self, port: int) -> None:
        self.base = f"http://127.0.0.1:{port}"

    def request(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"ok": False, "error": {"code": "E_NON_JSON", "message": raw}}
            raise APIError(exc.code, payload) from exc
        if payload.get("ok") is not True:
            raise APIError(500, payload)
        return payload["data"]

    def error(self, method: str, path: str, body: Any, expected_code: str) -> dict[str, Any]:
        try:
            self.request(method, path, body)
        except APIError as exc:
            code = exc.payload.get("error", {}).get("code")
            if code != expected_code:
                raise AssertionError(f"expected {expected_code}, got {code}: {exc.payload}") from exc
            return exc.payload["error"]
        raise AssertionError(f"expected business error {expected_code}, request succeeded")


def load_sample(name: str) -> dict[str, Any]:
    with (SAMPLES / name).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def remove_sqlite_files(db_path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()


def start_server(db_path: Path, *, crash_enabled: bool, log_path: Path) -> tuple[subprocess.Popen[bytes], int, HTTPClient]:
    port = free_port()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    env["UPLOAD_INDEX_DB"] = str(db_path)
    env["UPLOAD_INDEX_ENABLE_CRASH_DRILL"] = "1" if crash_enabled else "0"
    env["PYTHONUNBUFFERED"] = "1"
    log_handle = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "upload_index.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    log_handle.close()
    client = HTTPClient(port)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            client.request("GET", "/health")
            return process, port, client
        except Exception:
            time.sleep(0.1)
    try:
        process.kill()
    except OSError:
        pass
    log_text = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
    raise RuntimeError(f"server failed to start on port {port}; log:\n{log_text[-4000:]}")


def stop_server(process: subprocess.Popen[bytes]) -> int:
    if process.poll() is None:
        process.terminate()
    try:
        return process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=5)


def normalize_view(view: dict[str, Any]) -> dict[str, Any]:
    return {
        "session": view["session"],
        "parts": view["parts"],
        "index": view["index"],
    }


def run_normal_validation(
    client: HTTPClient, checks: CheckLog, db_path: Path
) -> dict[str, Any]:
    full = load_sample("full_batch.json")
    incremental = load_sample("incremental_batches.json")
    race = load_sample("concurrent_sessions.json")
    gc = load_sample("dedup_gc.json")

    # 1. Full ordered commit with intra-session dedup and atomic merge.
    full_session = client.request(
        "POST",
        "/sessions",
        {
            "session_id": full["session_id"],
            "object_key": full["object_key"],
            "conflict_policy": full["conflict_policy"],
            "lower_bound": full["lower_bound"],
        },
    )
    checks.check("full session generation is 1", full_session["generation"] == 1, full_session)
    full_submit = client.request(
        "POST",
        f"/sessions/{full['session_id']}/parts/batch",
        {"batch_id": full["batch_id"], "parts": full["parts"]},
    )
    full_premerge_version = full_submit["current_version"]
    full_premerge_view = client.request(
        "GET", f"/sessions/{full['session_id']}/view?version={full_premerge_version}"
    )
    checks.check(
        "full batch intervals are contiguous",
        full_premerge_view["index"]["contiguous_ranges"] == [[1, 6]],
        full_premerge_view["index"]["contiguous_ranges"],
    )
    alpha_refs = {
        item["sha256"]: item["ref_count"] for item in full_premerge_view["index"]["dedup_refs"]
    }
    checks.check(
        "same digest is stored once with two references",
        alpha_refs.get(full["parts"][0]["sha256"]) == 2,
        alpha_refs,
    )
    merged = client.request(
        "POST",
        f"/sessions/{full['session_id']}/merge",
        {
            "generation": 1,
            "total_size": full["declared_total_size"],
            "parts": full["parts"],
        },
    )
    checks.check("full merge produced object metadata", merged["part_count"] == 6, merged)
    duplicate_merge_error = client.error(
        "POST",
        f"/sessions/{full['session_id']}/merge",
        {"generation": 1, "total_size": full["declared_total_size"], "parts": full["parts"]},
        "E_OBJECT_EXISTS",
    )
    checks.check(
        "second merge for the same object key is rejected",
        duplicate_merge_error["code"] == "E_OBJECT_EXISTS",
        duplicate_merge_error,
    )

    # 2. Out-of-order incremental batches, idempotent duplicate, and historical reads.
    incremental_session = client.request(
        "POST",
        "/sessions",
        {
            "session_id": incremental["session_id"],
            "object_key": incremental["object_key"],
            "conflict_policy": incremental["conflict_policy"],
            "lower_bound": incremental["lower_bound"],
        },
    )
    incremental_create_version = incremental_session["created_version"]
    first_increment = client.request(
        "POST",
        f"/sessions/{incremental['session_id']}/parts/batch",
        incremental["batches"][0],
    )
    incremental_v1 = first_increment["current_version"]
    first_view = client.request(
        "GET", f"/sessions/{incremental['session_id']}/view?version={incremental_v1}"
    )
    checks.check(
        "out-of-order arrivals form sparse intervals",
        first_view["index"]["contiguous_ranges"] == [[3, 3], [5, 5]],
        first_view["index"]["contiguous_ranges"],
    )
    checks.check(
        "gap detection reports missing ranges",
        first_view["index"]["missing_ranges"] == [[1, 2], [4, 4]],
        first_view["index"]["missing_ranges"],
    )
    duplicate_part = [part for part in first_view["parts"] if part["seq"] == 3][0]
    checks.check(
        "same-seq same-digest resubmission is idempotent",
        duplicate_part["duplicate_count"] == 1 and first_view["session"]["duplicate_commits"] == 1,
        duplicate_part,
    )

    second_increment = client.request(
        "POST",
        f"/sessions/{incremental['session_id']}/parts/batch",
        incremental["batches"][1],
    )
    incremental_v2 = second_increment["current_version"]
    final_view = client.request(
        "GET", f"/sessions/{incremental['session_id']}/view?version={incremental_v2}"
    )
    checks.check(
        "gap-filling increments merge the interval to [1,6]",
        final_view["index"]["contiguous_ranges"] == [[1, 6]],
        final_view["index"]["contiguous_ranges"],
    )
    old_view_after_changes = client.request(
        "GET", f"/sessions/{incremental['session_id']}/view?version={incremental_v1}"
    )
    checks.check(
        "old version still returns its exact pre-gap-fill state",
        old_view_after_changes["parts"] == first_view["parts"],
        [part["seq"] for part in old_view_after_changes["parts"]],
    )
    alpha = client.request(
        "GET", f"/content/{full['parts'][0]['sha256']}?version={incremental_v2}"
    )
    checks.check(
        "cross-session content dedup reference count is 3",
        alpha["content"]["ref_count"] == 3,
        alpha,
    )

    # 3. Physical index-segment compaction with history equivalence.
    segments_before = client.request("GET", f"/sessions/{incremental['session_id']}/segments")
    views_before_compaction = {
        incremental_v1: normalize_view(first_view),
        incremental_v2: normalize_view(final_view),
    }
    compaction = client.request(
        "POST", "/maintenance/compact", {"session_id": incremental["session_id"]}
    )
    segments_after = client.request("GET", f"/sessions/{incremental['session_id']}/segments")
    checks.check(
        "compaction reduces physical index segments",
        compaction["compacted"] and segments_after["segment_count"] < segments_before["segment_count"],
        {"before": segments_before, "after": segments_after, "compaction": compaction},
    )
    for version, expected in views_before_compaction.items():
        actual = client.request(
            "GET", f"/sessions/{incremental['session_id']}/view?version={version}"
        )
        checks.check(
            f"version {version} query semantics survive compaction",
            normalize_view(actual) == expected,
            version,
        )

    # 4. Same-key concurrent session generations and stale merge handling.
    race_sessions = []
    for index, session_spec in enumerate(race["sessions"]):
        created = client.request(
            "POST",
            "/sessions",
            {
                "session_id": race["session_ids"][index],
                "object_key": race["object_key"],
                "conflict_policy": race["conflict_policy"],
            },
        )
        race_sessions.append(created)
        client.request(
            "POST",
            f"/sessions/{race['session_ids'][index]}/parts/batch",
            {"batch_id": f"race-{index}", "parts": session_spec["parts"]},
        )
    checks.check(
        "same-key sessions coexist with monotonically increasing generations",
        [item["generation"] for item in race_sessions] == [1, 2],
        race_sessions,
    )
    stale = client.error(
        "POST",
        f"/sessions/{race['session_ids'][0]}/merge",
        {"generation": 1, "total_size": 70, "parts": race["sessions"][0]["parts"]},
        "E_SESSION_STALE",
    )
    checks.check(
        "older generation merge reports the current generation",
        stale["current_generation"] == 2,
        stale,
    )
    race_merged = client.request(
        "POST",
        f"/sessions/{race['session_ids'][1]}/merge",
        {"generation": 2, "total_size": 80, "parts": race["sessions"][1]["parts"]},
    )
    checks.check("newest generation wins the merge", race_merged["generation"] == 2, race_merged)
    client.error(
        "POST",
        f"/sessions/{race['session_ids'][1]}/merge",
        {"generation": 2, "total_size": 80, "parts": race["sessions"][1]["parts"]},
        "E_OBJECT_EXISTS",
    )
    client.request("DELETE", f"/sessions/{race['session_ids'][0]}")
    race_object = client.request(
        "GET", f"/objects/view?object_key={urllib.parse.quote(race['object_key'], safe='')}"
    )
    statuses = {
        item["session_id"]: item["status"]
        for item in race_object["generation_index"]["sessions"]
    }
    checks.check(
        "generation index tracks completed and cancelled statuses",
        statuses == {"sess-race-old": "cancelled", "sess-race-new": "completed"},
        statuses,
    )

    # 5. Dedup refcount -> pending_gc -> physical removal, with historical visibility.
    for index, session_spec in enumerate(gc["sessions"]):
        client.request(
            "POST",
            "/sessions",
            {
                "session_id": gc["session_ids"][index],
                "object_key": session_spec["object_key"],
                "conflict_policy": "reject",
            },
        )
        client.request(
            "POST",
            f"/sessions/{gc['session_ids'][index]}/parts",
            session_spec["part"],
        )
    shared_before = client.request("GET", f"/content/{gc['shared_sha256']}")
    checks.check(
        "shared content starts with two references",
        shared_before["content"]["ref_count"] == 2,
        shared_before,
    )
    client.request("DELETE", f"/sessions/{gc['session_ids'][0]}")
    shared_middle = client.request("GET", f"/content/{gc['shared_sha256']}")
    checks.check(
        "cancelling one session decrements to one reference",
        shared_middle["content"]["ref_count"] == 1 and shared_middle["content"]["state"] == "active",
        shared_middle,
    )
    client.request("DELETE", f"/sessions/{gc['session_ids'][1]}")
    shared_pending = client.request("GET", f"/content/{gc['shared_sha256']}")
    pending_version = shared_pending["version"]
    checks.check(
        "zero-reference content moves to pending_gc before removal",
        shared_pending["content"]["state"] == "pending_gc"
        and shared_pending["content"]["ref_count"] == 0,
        shared_pending,
    )
    purge = client.request("POST", "/maintenance/gc")
    shared_removed = client.request("GET", f"/content/{gc['shared_sha256']}")
    checks.check(
        "GC physically removes the content record",
        shared_removed["content"]["state"] == "removed"
        and shared_removed["physical_record_exists_now"] is False,
        shared_removed,
    )
    shared_pending_history = client.request(
        "GET", f"/content/{gc['shared_sha256']}?version={pending_version}"
    )
    checks.check(
        "historical query still sees pending_gc at the earlier version",
        shared_pending_history["content"]["state"] == "pending_gc",
        shared_pending_history,
    )

    # 6. Version leases and retirement boundary.
    lease = client.request("POST", "/query-leases", {"version": incremental_v1, "ttl_ms": 60_000})
    retire_protected = client.request(
        "POST", "/versions/retire", {"min_active_version": incremental_v2}
    )
    checks.check(
        "retirement is bounded by the minimum active query version",
        retire_protected["retention_floor"] == incremental_v1,
        retire_protected,
    )
    retired_creation = client.error(
        "GET",
        f"/sessions/{incremental['session_id']}/view?version={incremental_create_version}",
        None,
        "E_VERSION_RETIRED",
    )
    checks.check(
        "retired version query returns E_VERSION_RETIRED",
        retired_creation["retention_floor"] == incremental_v1,
        retired_creation,
    )
    protected_view = client.request(
        "GET", f"/sessions/{incremental['session_id']}/view?version={incremental_v1}"
    )
    checks.check(
        "leased active version remains queryable",
        protected_view["index"]["contiguous_ranges"] == [[3, 3], [5, 5]],
        protected_view["index"]["contiguous_ranges"],
    )
    client.request("DELETE", f"/query-leases/{lease['lease_id']}")
    retire_all_old = client.request(
        "POST", "/versions/retire", {"min_active_version": incremental_v2}
    )
    client.error(
        "GET",
        f"/sessions/{incremental['session_id']}/view?version={incremental_v1}",
        None,
        "E_VERSION_RETIRED",
    )
    retained_view = client.request(
        "GET", f"/sessions/{incremental['session_id']}/view?version={incremental_v2}"
    )
    checks.check(
        "retention floor version remains reconstructable from checkpoint + later events",
        retained_view["parts"] == final_view["parts"],
        retire_all_old,
    )

    trace = client.request("GET", f"/versions?since={incremental_v2}&limit=200")
    current = client.request("GET", "/versions/current")
    return {
        "database": str(db_path),
        "full_commit": {
            "session": full_session,
            "premerge_version": full_premerge_version,
            "premerge_intervals": full_premerge_view["index"]["contiguous_ranges"],
            "dedup_refs": full_premerge_view["index"]["dedup_refs"],
            "merged_object": merged,
            "duplicate_merge_error": duplicate_merge_error,
        },
        "incremental": {
            "first_version": incremental_v1,
            "first_intervals": first_view["index"]["contiguous_ranges"],
            "first_missing_ranges": first_view["index"]["missing_ranges"],
            "final_version": incremental_v2,
            "final_intervals": final_view["index"]["contiguous_ranges"],
            "old_version_parts_after_new_commits": [
                part["seq"] for part in old_view_after_changes["parts"]
            ],
            "duplicate_commits": final_view["session"]["duplicate_commits"],
            "alpha_ref_count": alpha["content"]["ref_count"],
        },
        "compaction": {
            "before": segments_before,
            "after": segments_after,
            "result": compaction,
            "historical_equivalence": True,
        },
        "generations": {
            "sessions": race_sessions,
            "stale_error": stale,
            "winner": race_merged,
            "statuses": statuses,
        },
        "dedup_gc": {
            "before": shared_before["content"],
            "after_one_cancel": shared_middle["content"],
            "pending": shared_pending["content"],
            "purge": purge,
            "removed": shared_removed["content"],
        },
        "retirement": {
            "protected": retire_protected,
            "protected_query_intervals": protected_view["index"]["contiguous_ranges"],
            "final": retire_all_old,
            "retained_version": incremental_v2,
        },
        "version_trace_after_retirement": trace["events"][:20],
        "current": current,
    }


def run_force_termination_validation(
    checks: CheckLog, db_path: Path, normal_log: Path
) -> dict[str, Any]:
    plan = load_sample("forced_termination_plan.json")
    process, _port, client = start_server(db_path, crash_enabled=True, log_path=normal_log)
    try:
        session = client.request(
            "POST",
            "/sessions",
            {
                "session_id": plan["session_id"],
                "object_key": plan["object_key"],
                "conflict_policy": plan["conflict_policy"],
            },
        )
        base = client.request(
            "POST",
            f"/sessions/{plan['session_id']}/parts/batch",
            {"batch_id": plan["base_batch_id"], "parts": plan["base_parts"]},
        )
        before_version = base["current_version"]
        before_view = client.request("GET", f"/sessions/{plan['session_id']}/view")
        before_segments = client.request("GET", f"/sessions/{plan['session_id']}/segments")

        crash_request_sent = True
        crashed_by_connection_drop = False
        try:
            client.request(
                "POST",
                "/admin/crash-drill",
                {
                    "kind": "batch",
                    "session_id": plan["session_id"],
                    "batch_id": plan["crash_batch_id"],
                    "parts": plan["crash_parts"],
                    "crash_point": plan["crash_point"],
                },
            )
        except APIError:
            raise
        except Exception:
            crashed_by_connection_drop = True
        return_code = process.wait(timeout=15)
        checks.check("crash drill process exited abruptly", return_code != 0, return_code)
        checks.check(
            "client observed a connection drop during forced termination",
            crash_request_sent and crashed_by_connection_drop,
            {"request_sent": crash_request_sent, "connection_drop": crashed_by_connection_drop},
        )
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    restarted, _new_port, recovered_client = start_server(
        db_path, crash_enabled=True, log_path=normal_log
    )
    try:
        report = recovered_client.request("GET", "/recovery/report")
        current = recovered_client.request("GET", "/versions/current")
        after_view = recovered_client.request("GET", f"/sessions/{plan['session_id']}/view")
        after_segments = recovered_client.request(
            "GET", f"/sessions/{plan['session_id']}/segments"
        )
        checks.check(
            "recovery report is based on persisted facts and detects unclean shutdown",
            report["forced_termination_detected"] is True
            and report["recovery_source"] == "sqlite_persisted_facts_only"
            and report["clean_exit_report_available"] is False,
            report,
        )
        checks.check(
            "pending batch intent is recovered as aborted",
            report["pending_intents_found"] >= 1
            and report["recovered_intents"][0]["decision"] == "aborted_rolled_back",
            report["recovered_intents"],
        )
        checks.check(
            "current version rolled back to the last complete version",
            before_version == current["current_version"] == report["last_complete_version"],
            {"before": before_version, "after": current, "report": report["last_complete_version"]},
        )
        checks.check(
            "uncommitted crash batch parts are absent after recovery",
            [part["seq"] for part in after_view["parts"]]
            == [part["seq"] for part in before_view["parts"]],
            [part["seq"] for part in after_view["parts"]],
        )
        checks.check(
            "active index segments are identical to the pre-crash persisted state",
            after_segments == before_segments,
            {"before": before_segments, "after": after_segments},
        )

        recovery_diff = {
            "version_before": before_version,
            "version_after": current["current_version"],
            "version_delta": current["current_version"] - before_version,
            "parts_before": [part["seq"] for part in before_view["parts"]],
            "parts_after": [part["seq"] for part in after_view["parts"]],
            "segments_before": before_segments,
            "segments_after": after_segments,
            "recovery_report": report,
        }

        retry = recovered_client.request(
            "POST",
            f"/sessions/{plan['session_id']}/parts/batch",
            {"batch_id": "crash-recovered-retry", "parts": plan["crash_parts"]},
        )
        recovered_view = recovered_client.request(
            "GET", f"/sessions/{plan['session_id']}/view?version={retry['current_version']}"
        )
        checks.check(
            "restarted service accepts a new batch and closes the gaps contiguously",
            recovered_view["index"]["contiguous_ranges"] == [[1, 6]],
            recovered_view["index"]["contiguous_ranges"],
        )
        recovery_diff["retry_after_recovery"] = {
            "versions": retry["versions"],
            "intervals": recovered_view["index"]["contiguous_ranges"],
        }

        compact_version = retry["current_version"]
        compact_segments_before = recovered_client.request(
            "GET", f"/sessions/{plan['session_id']}/segments"
        )
        compact_connection_drop = False
        try:
            recovered_client.request(
                "POST",
                "/admin/crash-drill",
                {
                    "kind": "compact",
                    "session_id": plan["session_id"],
                    "crash_point": "after_partial_writes",
                },
            )
        except APIError:
            raise
        except Exception:
            compact_connection_drop = True
        compact_return_code = restarted.wait(timeout=15)
        checks.check(
            "compaction crash drill exits abruptly and drops the client connection",
            compact_return_code != 0 and compact_connection_drop,
            {"return_code": compact_return_code, "connection_drop": compact_connection_drop},
        )

        compact_recovered, _third_port, compact_client = start_server(
            db_path, crash_enabled=False, log_path=normal_log
        )
        try:
            compact_report = compact_client.request("GET", "/recovery/report")
            compact_current = compact_client.request("GET", "/versions/current")
            compact_segments_after = compact_client.request(
                "GET", f"/sessions/{plan['session_id']}/segments"
            )
            checks.check(
                "compaction recovery reports an aborted pending intent",
                compact_report["pending_intents_found"] == 1
                and compact_report["recovered_intents"][0]["kind"] == "compact"
                and compact_report["recovered_intents"][0]["decision"]
                == "aborted_rolled_back",
                compact_report["recovered_intents"],
            )
            checks.check(
                "compaction crash preserves the complete version and source segments",
                compact_current["current_version"] == compact_version
                and compact_segments_after == compact_segments_before,
                {
                    "version_before": compact_version,
                    "version_after": compact_current["current_version"],
                    "segments_before": compact_segments_before,
                    "segments_after": compact_segments_after,
                },
            )
            recovery_diff["compaction_crash"] = {
                "version_before": compact_version,
                "version_after": compact_current["current_version"],
                "segments_before": compact_segments_before,
                "segments_after": compact_segments_after,
                "recovery_report": compact_report,
            }
        finally:
            stop_server(compact_recovered)
        return recovery_diff
    finally:
        stop_server(restarted)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    args = parser.parse_args()
    data_dir = Path(args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

    normal_db = data_dir / "validation.db"
    crash_db = data_dir / "force_termination.db"
    remove_sqlite_files(normal_db)
    remove_sqlite_files(crash_db)
    normal_log = data_dir / "validation-server.log"
    crash_log = data_dir / "force-termination-server.log"
    normal_log.write_text("", encoding="utf-8")
    crash_log.write_text("", encoding="utf-8")

    checks = CheckLog()
    process, _port, client = start_server(normal_db, crash_enabled=False, log_path=normal_log)
    try:
        normal_result = run_normal_validation(client, checks, normal_db)
    finally:
        stop_server(process)

    crash_result = run_force_termination_validation(checks, crash_db, crash_log)
    result = {
        "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "normal_validation": normal_result,
        "force_termination": crash_result,
        "check_count": len(checks.items),
        "checks": checks.items,
    }
    (ARTIFACTS / "validation_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (ARTIFACTS / "force_termination_recovery.json").write_text(
        json.dumps(crash_result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    incremental = normal_result["incremental"]
    compaction = normal_result["compaction"]
    generation = normal_result["generations"]
    gc_result = normal_result["dedup_gc"]
    retirement = normal_result["retirement"]
    checksum = checks.items

    print("=== 分段上传元数据索引：本地可复现验证 ===")
    print(f"数据库: {normal_db}")
    print(
        "1) 全量提交: "
        f"premerge_version={normal_result['full_commit']['premerge_version']} "
        f"intervals={normal_result['full_commit']['premerge_intervals']} "
        f"object_sha256={normal_result['full_commit']['merged_object']['object_sha256']}"
    )
    print(
        "2) 乱序/重复增量: "
        f"v{incremental['first_version']} intervals={incremental['first_intervals']} "
        f"missing={incremental['first_missing_ranges']} -> "
        f"v{incremental['final_version']} intervals={incremental['final_intervals']} "
        f"duplicate_commits={incremental['duplicate_commits']} alpha_refs={incremental['alpha_ref_count']}"
    )
    print(
        "3) 索引段压缩: "
        f"{compaction['before']['segment_count']} -> {compaction['after']['segment_count']} 段; "
        "旧版本查询逐字段一致=" + str(compaction["historical_equivalence"])
    )
    print(
        "4) 同键代际: "
        f"generations={[item['generation'] for item in generation['sessions']]} "
        f"stale={generation['stale_error']['code']} current_generation="
        f"{generation['stale_error']['current_generation']} statuses={generation['statuses']}"
    )
    print(
        "5) 去重回收: "
        f"refs {gc_result['before']['ref_count']} -> {gc_result['after_one_cancel']['ref_count']} "
        f"-> {gc_result['pending']['state']} -> {gc_result['removed']['state']}; "
        f"purged={gc_result['purge']['purged_count']}"
    )
    print(
        "6) 版本回收: "
        f"protected_floor={retirement['protected']['retention_floor']} "
        f"final_floor={retirement['final']['retention_floor']} "
        f"retained_v{retirement['retained_version']} ok"
    )
    print(
        "7) 批次强制终止恢复: "
        f"version {crash_result['version_before']} -> {crash_result['version_after']} "
        f"delta={crash_result['version_delta']} "
        f"parts_after={crash_result['parts_after']} "
        f"pending_intents={crash_result['recovery_report']['pending_intents_found']} "
        f"decision={crash_result['recovery_report']['recovered_intents'][0]['decision']}"
    )
    print(
        "8) 压缩强制终止恢复: "
        f"version {crash_result['compaction_crash']['version_before']} -> "
        f"{crash_result['compaction_crash']['version_after']} "
        f"segments {crash_result['compaction_crash']['segments_before']['segment_count']} -> "
        f"{crash_result['compaction_crash']['segments_after']['segment_count']} "
        f"decision={crash_result['compaction_crash']['recovery_report']['recovered_intents'][0]['decision']}"
    )
    print(f"校验项: {len(checksum)} 项全部通过")
    print(f"详细结果: {ARTIFACTS / 'validation_result.json'}")
    print(f"恢复差异: {ARTIFACTS / 'force_termination_recovery.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

