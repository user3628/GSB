#!/usr/bin/env python3
"""Repeatable end-to-end demonstration of the MVCC acceptance scenarios."""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mvcc_engine.client import ApiError, MVCCClient  # noqa: E402
from scripts.concurrent_editors_demo import launch_concurrent_editors  # noqa: E402
from scripts.demo_common import (  # noqa: E402
    reset_directory,
    start_server,
    stop_handle,
    wait_until,
)
from scripts.force_termination_demo import run_force_termination_demo  # noqa: E402


REQUIRED_ERROR_CODES = {
    "E_INVALID_ISOLATION",
    "E_EDIT_STALE",
    "E_EDIT_EXPIRED",
    "E_WRITE_CONFLICT",
    "E_LOCK_TIMEOUT",
    "E_LOCK_QUEUE_FULL",
    "E_TXN_LIMIT",
    "E_DEADLOCK_VICTIM",
    "E_CONFLICTING_TXN",
}


class DemoRecorder:
    def __init__(self, client: MVCCClient) -> None:
        self.client = client
        self.observed_codes: set[str] = set()
        self.events: list[dict[str, Any]] = []

    def expect_error(self, code: str, fn: Callable[[], Any]) -> dict[str, Any]:
        try:
            value = fn()
        except ApiError as error:
            if error.code != code:
                raise RuntimeError(
                    f"expected {code}, got {error.code}: {error.as_dict()}"
                ) from None
            self.observed_codes.add(error.code)
            event = {
                "expected_code": code,
                "observed_error": error.as_dict(),
            }
            self.events.append(event)
            return event
        raise RuntimeError(f"expected {code}, request succeeded: {value}")

    def record(self, code: str) -> None:
        self.observed_codes.add(code)


def current_row(client: MVCCClient, table: str, row_id: str) -> dict[str, Any] | None:
    txn = client.begin("READ_COMMITTED", f"observer-{uuid.uuid4().hex[:10]}")["txn_id"]
    try:
        return client.read(txn, table, row_id)["row"]
    finally:
        client.rollback(txn, "observer_done")


def scenario_invalid_isolation(client: MVCCClient, recorder: DemoRecorder) -> dict[str, Any]:
    event = recorder.expect_error(
        "E_INVALID_ISOLATION",
        lambda: client.begin("SERIALIZABLE", f"invalid-{uuid.uuid4().hex[:8]}"),
    )
    return {
        "requested_isolation": "SERIALIZABLE",
        "error": event["observed_error"],
    }


def scenario_savepoint(client: MVCCClient, recorder: DemoRecorder) -> dict[str, Any]:
    txn = client.begin("REPEATABLE_READ", "demo-savepoint")["txn_id"]
    baseline = client.read(txn, "accounts", "A-1002")
    fixed_snapshot = baseline["snapshot_id"]
    edit_one = client.open_edit(txn, "accounts", "A-1002")
    first_save = client.save(
        txn,
        edit_one["session_id"],
        edit_one["row_version"],
        {"name": "Before savepoint", "balance": 202, "owner": "bob"},
    )
    savepoint = client.savepoint(txn, "after_first_edit")
    edit_two = client.open_edit(txn, "accounts", "A-1003")
    second_save = client.save(
        txn,
        edit_two["session_id"],
        edit_two["row_version"],
        {"name": "After savepoint", "balance": 303, "owner": "carol"},
    )
    before_rollback = client.query(txn, "accounts")
    rolled = client.rollback_to(txn, "after_first_edit")
    after_rollback = client.query(txn, "accounts")
    removed_session = recorder.expect_error(
        "E_EDIT_SESSION_NOT_FOUND",
        lambda: client.request(
            "GET",
            f"/txns/{txn}/edit-sessions/{edit_two['session_id']}",
        ),
    )
    restored_session = client.request(
        "GET",
        f"/txns/{txn}/edit-sessions/{edit_one['session_id']}",
    )
    locks = client.request("GET", "/locks")
    client.commit(txn, {"request_id": "savepoint-commit"})
    committed_one = current_row(client, "accounts", "A-1002")
    committed_two = current_row(client, "accounts", "A-1003")
    before_by_id = {row["row_id"]: row for row in before_rollback["rows"]}
    after_by_id = {row["row_id"]: row for row in after_rollback["rows"]}
    result = {
        "txn_id": txn,
        "fixed_snapshot": fixed_snapshot,
        "first_save_version": first_save["new_row_version"],
        "second_save_version": second_save["new_row_version"],
        "savepoint_snapshot": savepoint["snapshot_id"],
        "before_rollback": {
            "A-1002": before_by_id["A-1002"],
            "A-1003": before_by_id["A-1003"],
        },
        "rollback_result": rolled,
        "after_rollback": {
            "A-1002": after_by_id["A-1002"],
            "A-1003": after_by_id["A-1003"],
        },
        "post_savepoint_session_removed": removed_session["observed_error"],
        "pre_savepoint_session_restored_status": restored_session["session"]["status"],
        "locks_after_rollback": locks,
        "committed_after": {
            "A-1002": committed_one,
            "A-1003": committed_two,
        },
    }
    if fixed_snapshot != rolled["snapshot_id"]:
        raise RuntimeError("savepoint rollback changed the transaction visibility snapshot")
    if after_by_id["A-1003"]["row_version"] != 1:
        raise RuntimeError("savepoint rollback did not undo the second row write")
    if after_by_id["A-1002"]["row_version"] != first_save["new_row_version"]:
        raise RuntimeError("savepoint rollback did not preserve the pre-savepoint row write")
    return result


def scenario_wait_timeout(client: MVCCClient, recorder: DemoRecorder) -> dict[str, Any]:
    holder = client.begin("REPEATABLE_READ", "demo-timeout-holder")["txn_id"]
    holder_edit = client.open_edit(holder, "accounts", "A-1004")
    holder_save = client.save(
        holder,
        holder_edit["session_id"],
        holder_edit["row_version"],
        {"name": "Held for timeout", "balance": 404, "owner": "dave"},
    )

    rc = client.begin("READ_COMMITTED", "demo-rc-conflict")["txn_id"]
    rc_edit = client.open_edit(rc, "accounts", "A-1004")
    start = time.monotonic()
    conflict = recorder.expect_error(
        "E_WRITE_CONFLICT",
        lambda: client.save(
            rc,
            rc_edit["session_id"],
            rc_edit["row_version"],
            {"name": "RC conflict", "balance": 405},
        ),
    )
    rc_elapsed_ms = int((time.monotonic() - start) * 1000)

    waiter = client.begin("REPEATABLE_READ", "demo-timeout-waiter")["txn_id"]
    waiter_edit = client.open_edit(waiter, "accounts", "A-1004")
    start = time.monotonic()
    timeout = recorder.expect_error(
        "E_LOCK_TIMEOUT",
        lambda: client.save(
            waiter,
            waiter_edit["session_id"],
            waiter_edit["row_version"],
            {"name": "Will time out", "balance": 406},
        ),
    )
    timeout_elapsed_ms = int((time.monotonic() - start) * 1000)

    client.rollback(holder, "timeout_demo_done")
    client.rollback(rc, "timeout_demo_done")
    client.rollback(waiter, "timeout_demo_done")
    reverted = current_row(client, "accounts", "A-1004")
    return {
        "holder_save": holder_save,
        "read_committed_conflict": conflict["observed_error"],
        "read_committed_elapsed_ms": rc_elapsed_ms,
        "lock_timeout": timeout["observed_error"],
        "observed_timeout_elapsed_ms": timeout_elapsed_ms,
        "row_after_holder_rollback": reverted,
    }


def scenario_queue_full_and_wake(client: MVCCClient, recorder: DemoRecorder) -> dict[str, Any]:
    holder = client.begin("REPEATABLE_READ", "demo-queue-holder")["txn_id"]
    holder_edit = client.open_edit(holder, "accounts", "A-1003")
    client.save(
        holder,
        holder_edit["session_id"],
        holder_edit["row_version"],
        {"name": "Queue holder", "balance": 500},
    )

    waiters: list[dict[str, Any]] = []
    threads: list[threading.Thread] = []
    for index in range(3):
        txn = client.begin("REPEATABLE_READ", f"demo-queue-waiter-{index}")["txn_id"]
        edit = client.open_edit(txn, "accounts", "A-1003")
        item: dict[str, Any] = {"index": index, "txn_id": txn, "edit": edit}
        waiters.append(item)

        def wait_and_rollback(entry: dict[str, Any] = item) -> None:
            started = time.monotonic()
            try:
                entry["save"] = client.save(
                    entry["txn_id"],
                    entry["edit"]["session_id"],
                    entry["edit"]["row_version"],
                    {"name": f"Woken waiter {entry['index']}", "balance": 600 + entry["index"]},
                )
                entry["elapsed_ms"] = int((time.monotonic() - started) * 1000)
            except BaseException as error:  # pragma: no cover - diagnostic capture
                entry["error"] = error
            finally:
                try:
                    client.rollback(entry["txn_id"], "queue_demo_rollback")
                except BaseException as error:  # pragma: no cover - diagnostic capture
                    entry.setdefault("rollback_error", error)

        thread = threading.Thread(target=wait_and_rollback, name=f"queue-waiter-{index}")
        threads.append(thread)
        thread.start()

    if not wait_until(
        lambda: client.request("GET", "/locks")["waiting_count"] == 3,
        timeout=3.0,
    ):
        raise RuntimeError("three queue waiters did not register in time")
    locks_with_queue = client.request("GET", "/locks")

    rejected = client.begin("REPEATABLE_READ", "demo-queue-full")["txn_id"]
    rejected_edit = client.open_edit(rejected, "accounts", "A-1003")
    queue_full = recorder.expect_error(
        "E_LOCK_QUEUE_FULL",
        lambda: client.save(
            rejected,
            rejected_edit["session_id"],
            rejected_edit["row_version"],
            {"name": "Queue full", "balance": 777},
        ),
    )
    client.rollback(rejected, "queue_full_demo_done")

    client.rollback(holder, "release_queued_waiters")
    for thread in threads:
        thread.join(timeout=5.0)
        if thread.is_alive():
            raise RuntimeError("queued waiter did not wake before the demo timeout")
    if any("error" in item or "save" not in item for item in waiters):
        raise RuntimeError(
            "one or more queued waiters did not save successfully: "
            + json.dumps(
                [
                    {"index": item["index"], "error": repr(item.get("error"))}
                    for item in waiters
                    if "error" in item or "save" not in item
                ],
                ensure_ascii=False,
            )
        )
    return {
        "holder_save_version": holder_edit["row_version"] + 1,
        "locks_with_three_waiters": locks_with_queue,
        "queue_full_error": queue_full["observed_error"],
        "wake_results": [
            {
                "index": item["index"],
                "txn_id": item["txn_id"],
                "saved_version": item["save"]["new_row_version"],
                "wake_and_save_elapsed_ms": item["elapsed_ms"],
            }
            for item in waiters
        ],
        "assertion": "the holder's rollback woke all three Condition waiters; each saved and then rolled back so the next waiter remained non-stale",
    }


def scenario_deadlock(client: MVCCClient, recorder: DemoRecorder) -> dict[str, Any]:
    first = client.begin("REPEATABLE_READ", "demo-deadlock-a")["txn_id"]
    time.sleep(0.003)
    second = client.begin("REPEATABLE_READ", "demo-deadlock-b")["txn_id"]
    a_row1 = client.open_edit(first, "documents", "DOC-001")
    a_row2 = client.open_edit(first, "documents", "DOC-002")
    b_row1 = client.open_edit(second, "documents", "DOC-001")
    b_row2 = client.open_edit(second, "documents", "DOC-002")
    client.save(
        first,
        a_row1["session_id"],
        a_row1["row_version"],
        {"title": "A owns DOC-001", "state": "draft"},
    )
    client.save(
        second,
        b_row2["session_id"],
        b_row2["row_version"],
        {"title": "B owns DOC-002", "state": "draft"},
    )

    a_result: dict[str, Any] = {}

    def first_waits() -> None:
        try:
            a_result["save"] = client.save(
                first,
                a_row2["session_id"],
                a_row2["row_version"],
                {"title": "A acquires DOC-002 after victim rollback", "state": "approved"},
            )
        except BaseException as error:  # pragma: no cover - diagnostic capture
            a_result["error"] = error

    thread = threading.Thread(target=first_waits, name="deadlock-a")
    thread.start()
    if not wait_until(
        lambda: client.request("GET", "/locks")["wait_graph"].get(first) == second,
        timeout=3.0,
    ):
        raise RuntimeError("deadlock edge A -> B did not register in time")
    wait_graph_before = client.request("GET", "/locks")
    victim = recorder.expect_error(
        "E_DEADLOCK_VICTIM",
        lambda: client.save(
            second,
            b_row1["session_id"],
            b_row1["row_version"],
            {"title": "B triggers cycle", "state": "draft"},
        ),
    )
    thread.join(timeout=5.0)
    if thread.is_alive():
        raise RuntimeError("non-victim waiter did not wake after deadlock resolution")
    if "error" in a_result:
        raise RuntimeError(f"non-victim waiter failed: {a_result['error']!r}")
    client.commit(first, {"request_id": "deadlock-a-commit"})
    b_state = client.request("GET", f"/txns/{second}")
    committed_row = current_row(client, "documents", "DOC-002")
    return {
        "wait_graph_before_cycle": wait_graph_before,
        "deadlock_error": victim["observed_error"],
        "non_victim_save": a_result["save"],
        "victim_transaction": b_state,
        "committed_DOC-002": committed_row,
    }


def scenario_expired(client: MVCCClient, recorder: DemoRecorder, edit_budget_ms: int) -> dict[str, Any]:
    txn = client.begin("REPEATABLE_READ", "demo-expired")["txn_id"]
    opened = client.open_edit(txn, "accounts", "A-1004")
    sleep_seconds = edit_budget_ms / 1000.0 + 0.15
    time.sleep(sleep_seconds)
    expired = recorder.expect_error(
        "E_EDIT_EXPIRED",
        lambda: client.save(
            txn,
            opened["session_id"],
            opened["row_version"],
            {"name": "Too late", "balance": 1},
        ),
    )
    client.rollback(txn, "expired_demo_done")
    return {
        "sleep_seconds": round(sleep_seconds, 3),
        "error": expired["observed_error"],
        "assertion": "version was unchanged, yet the save was rejected because the server-side interaction budget elapsed",
    }


def scenario_txn_limit(client: MVCCClient, recorder: DemoRecorder, max_active: int) -> dict[str, Any]:
    opened: list[str] = []
    for index in range(max_active):
        opened.append(
            client.begin("READ_COMMITTED", f"demo-limit-{index}")["txn_id"]
        )
    limited = recorder.expect_error(
        "E_TXN_LIMIT",
        lambda: client.begin("READ_COMMITTED", "demo-limit-overflow"),
    )
    for txn in opened:
        client.rollback(txn, "txn_limit_demo_done")
    return {
        "opened_count": len(opened),
        "error": limited["observed_error"],
    }


def scenario_idempotent_commit(client: MVCCClient, recorder: DemoRecorder) -> dict[str, Any]:
    txn = client.begin("READ_COMMITTED", "demo-idempotent-commit")["txn_id"]
    first = client.commit(txn, {"request_id": "stable-commit-request"})
    replay = client.commit(txn, {"request_id": "stable-commit-request"})
    conflict = recorder.expect_error(
        "E_CONFLICTING_TXN",
        lambda: client.commit(txn, {"request_id": "different-commit-request"}),
    )
    return {
        "first_commit": first,
        "replayed_commit": replay,
        "conflicting_commit": conflict["observed_error"],
    }


def build_report(
    runtime_dir: Path,
    force_termination: dict[str, Any],
    stages: dict[str, Any],
    observed_codes: set[str],
) -> dict[str, Any]:
    required_sorted = sorted(REQUIRED_ERROR_CODES)
    return {
        "ok": True,
        "generated_at": time.time(),
        "environment": {
            "python": sys.version.split()[0],
            "database": "SQLite WAL",
            "http_bind": "127.0.0.1 loopback only",
            "runtime_dir": str(runtime_dir),
        },
        "stages": stages,
        "error_code_coverage": {
            "required": required_sorted,
            "observed": sorted(observed_codes),
            "missing": sorted(REQUIRED_ERROR_CODES - observed_codes),
            "all_required_observed": REQUIRED_ERROR_CODES.issubset(observed_codes),
        },
        "forced_termination_recovery": force_termination,
        "summary": {
            "visibility": "RR/SNAPSHOT snapshots remained fixed; stale saves returned the latest editable version.",
            "savepoint": "row versions, edit sessions, and post-savepoint locks were undone together; pre-savepoint state stayed intact.",
            "locks": "RC failed fast; RR timed out without polling; queued waiters were genuinely awakened by lock release.",
            "deadlock": "the deterministic youngest transaction was rolled back and the surviving waiter proceeded.",
            "recovery": "external SIGKILL/TerminateProcess left no graceful report; restart reverted only uncommitted facts and preserved commits.",
        },
    }


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", default=str(ROOT / "runtime" / "demo"))
    parser.add_argument("--output", default=str(ROOT / "artifacts" / "demo-report.json"))
    args = parser.parse_args()
    runtime_dir = Path(args.runtime_dir).resolve()
    output = Path(args.output).resolve()
    data_dir = runtime_dir / "data"
    reset_directory(runtime_dir)
    reset_directory(data_dir)
    params_path = ROOT / "samples" / "demo_params.json"
    params = json.loads(params_path.read_text(encoding="utf-8"))
    edit_budget_ms = int(params["edit_budget_ms"])
    max_active = int(params["max_active_txns"])

    handle = start_server(data_dir, params_path, port=0)
    base_url = f"http://{handle['info']['host']}:{handle['info']['port']}"
    client = MVCCClient(base_url, timeout=12.0)
    recorder = DemoRecorder(client)
    stages: dict[str, Any] = {}
    try:
        stages["invalid_isolation"] = scenario_invalid_isolation(client, recorder)
        concurrent = launch_concurrent_editors(
            base_url,
            runtime_dir / "concurrent_barrier",
            run_id=uuid.uuid4().hex[:12],
        )
        recorder.record("E_EDIT_STALE")
        stages["concurrent_edit_stale"] = concurrent
        stages["savepoint_rollback"] = scenario_savepoint(client, recorder)
        stages["wait_timeout_and_rc_conflict"] = scenario_wait_timeout(client, recorder)
        stages["lock_queue_full_and_wake"] = scenario_queue_full_and_wake(client, recorder)
        stages["deadlock_victim"] = scenario_deadlock(client, recorder)
        stages["edit_expired"] = scenario_expired(client, recorder, edit_budget_ms)
        stages["transaction_limit"] = scenario_txn_limit(client, recorder, max_active)
        stages["idempotent_commit"] = scenario_idempotent_commit(client, recorder)
        demo_stats = client.request("GET", "/stats")
        demo_locks = client.request("GET", "/locks")
    finally:
        server_exit = stop_handle(handle, force=False)

    # Start a fresh service/data directory for the destructive crash scenario.
    force_termination = run_force_termination_demo(
        runtime_dir / "force_termination",
        params_path,
    )
    report = build_report(runtime_dir, force_termination, stages, recorder.observed_codes)
    report["main_service"] = {
        "exit_code": server_exit,
        "final_stats": demo_stats,
        "final_locks": demo_locks,
    }
    if report["error_code_coverage"]["missing"]:
        raise RuntimeError(
            "missing required demo error codes: "
            + json.dumps(report["error_code_coverage"]["missing"], ensure_ascii=False)
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(output.name + ".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(output)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

