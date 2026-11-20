#!/usr/bin/env python3
"""Kill the service from outside, restart it, and verify intent-based recovery."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mvcc_engine.client import MVCCClient  # noqa: E402
from scripts.demo_common import (  # noqa: E402
    reset_directory,
    start_server,
    stop_handle,
)


def run_force_termination_demo(work_dir: Path, params_path: Path | None = None) -> dict:
    data_dir = work_dir / "data"
    reset_directory(work_dir)
    params_path = params_path or ROOT / "samples" / "demo_params.json"
    first = start_server(data_dir, params_path, port=0)
    base_url = f"http://{first['info']['host']}:{first['info']['port']}"
    client = MVCCClient(base_url)
    try:
        survivor_txn = client.begin("READ_COMMITTED", "crash-survivor")["txn_id"]
        survivor_edit = client.open_edit(survivor_txn, "documents", "DOC-002")
        survivor_save = client.save(
            survivor_txn,
            survivor_edit["session_id"],
            survivor_edit["row_version"],
            {
                "title": "Committed before forced termination",
                "state": "approved",
                "amount": 841,
                "assignee": "bob",
            },
        )
        survivor_commit = client.commit(
            survivor_txn,
            {"request_id": "survivor-commit"},
        )

        active_txn = client.begin("REPEATABLE_READ", "crash-active")["txn_id"]
        client.read(active_txn, "documents", "DOC-003")
        active_edit = client.open_edit(active_txn, "documents", "DOC-003")
        client.save(
            active_txn,
            active_edit["session_id"],
            active_edit["row_version"],
            {
                "title": "UNCOMMITTED crash write must disappear",
                "state": "draft",
                "amount": -1,
                "assignee": "nobody",
            },
        )
        client.savepoint(active_txn, "after-first-crash-write")
        second_edit = client.open_edit(active_txn, "accounts", "A-1002")
        client.save(
            active_txn,
            second_edit["session_id"],
            second_edit["row_version"],
            {"name": "UNCOMMITTED account write", "balance": -22},
        )
        idle_edit = client.open_edit(active_txn, "accounts", "A-1004")
        stats_before = client.request("GET", "/stats")
        locks_before = client.request("GET", "/locks")
        pid = int(first["info"]["pid"])
        shutdown_report_before_kill = (data_dir / "shutdown-report.json").exists()
    finally:
        # This is the required external forced termination: no application
        # shutdown endpoint, no source changes, and no recovery hook runs.
        killed_code = stop_handle(first, force=True)

    shutdown_report_after_kill = (data_dir / "shutdown-report.json").exists()
    forced_stdout = "\n".join(first["stdout_lines"])
    second = start_server(data_dir, params_path, port=0)
    restarted_base = f"http://{second['info']['host']}:{second['info']['port']}"
    restarted = MVCCClient(restarted_base)
    try:
        report = restarted.recovery_report()
        observer = restarted.begin("READ_COMMITTED", "post-recovery-observer")["txn_id"]
        committed_row = restarted.read(observer, "documents", "DOC-002")["row"]
        reverted_document = restarted.read(observer, "documents", "DOC-003")["row"]
        reverted_account = restarted.read(observer, "accounts", "A-1002")["row"]
        idle_row = restarted.read(observer, "accounts", "A-1004")["row"]
        restarted.commit(observer)
        recovered_txn = restarted.request("GET", f"/txns/{active_txn}")
    finally:
        # Avoid manufacturing a graceful-exit report for the forced scenario.
        stop_handle(second, force=True)

    checks = {
        "committed_survives_forced_termination": (
            committed_row["data"]["title"] == "Committed before forced termination"
            and committed_row["row_version"] == survivor_save["new_row_version"]
            and committed_row["commit_seq"] == survivor_commit["commit_seq"]
        ),
        "first_uncommitted_write_reverted": (
            reverted_document["row_version"] == 1
            and reverted_document["data"]["title"] == "Audit trail extract"
        ),
        "second_uncommitted_write_reverted": (
            reverted_account["row_version"] == 1
            and reverted_account["data"]["name"] == "Contoso Retail"
        ),
        "idle_edit_session_row_unchanged": (
            idle_row["row_version"] == 1 and idle_row["data"]["name"] == "Tailspin Logistics"
        ),
        "active_txn_marked_recovered_aborted": (
            recovered_txn["status"] == "recovered_aborted"
            and recovered_txn["abort_reason"] == "startup_recovery"
        ),
        "recovery_report_based_on_persisted_intents": (
            report["report_kind"] == "startup_recovery"
            and report["status"] == "recovered_unfinished_transactions"
            and active_txn in [item["txn_id"] for item in report["recovered_txns"]]
            and report["uncommitted_versions_removed"] >= 2
        ),
        "no_complete_graceful_exit_report_claimed": (
            report["graceful_exit_report_available"] is False
            and not shutdown_report_before_kill
            and not shutdown_report_after_kill
            and "SERVER_STOPPED" not in forced_stdout
        ),
        "recovery_report_file_exists": (data_dir / "recovery-report.json").exists(),
        "intent_log_file_exists": (data_dir / "intent-log.jsonl").exists(),
    }
    if not all(checks.values()):
        raise RuntimeError(
            "forced-termination recovery checks failed: "
            + json.dumps(checks, ensure_ascii=False, indent=2)
        )
    return {
        "ok": True,
        "scenario": "external_forced_termination_then_restart",
        "kill_method": (
            "TerminateProcess via subprocess.Popen.kill"
            if platform.system() == "Windows"
            else "SIGKILL via subprocess.Popen.kill"
        ),
        "killed_pid": pid,
        "killed_return_code": killed_code,
        "shutdown_report_before_kill": shutdown_report_before_kill,
        "shutdown_report_after_kill": shutdown_report_after_kill,
        "pre_kill": {
            "stats": stats_before,
            "locks": locks_before,
            "committed_txn": survivor_commit,
            "active_txn": active_txn,
            "active_snapshot_id": active_edit["snapshot_id"],
            "idle_session_id": idle_edit["session_id"],
        },
        "recovery_report": report,
        "post_restart_rows": {
            "committed_document": committed_row,
            "reverted_document": reverted_document,
            "reverted_account": reverted_account,
        },
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", default=str(ROOT / "runtime" / "force_termination"))
    parser.add_argument("--params", default=str(ROOT / "samples" / "demo_params.json"))
    args = parser.parse_args()
    result = run_force_termination_demo(Path(args.work_dir), Path(args.params))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

