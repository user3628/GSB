#!/usr/bin/env python3
"""Reproduce the acceptance scenarios against the fixed local samples.

The script starts only loopback servers, uses a fresh SQLite DB for each
scenario, and writes a machine-readable verification report. It never contacts
an external network.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from werkzeug.serving import make_server

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mqmw.api import create_app  # noqa: E402
from mqmw.samples import load_samples  # noqa: E402
from mqmw.store import SQLiteStore  # noqa: E402
from tools.test_consumer import create_consumer_app  # noqa: E402


def start_server(app):
    server = make_server("127.0.0.1", 0, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


def stop_server(server, thread):
    server.shutdown()
    thread.join(timeout=5)


@contextmanager
def middleware_stack(db_path: Path):
    app = create_app(str(db_path), ack_base_url="http://127.0.0.1:8080")
    server, thread, base = start_server(app)
    app.config["MQMW_ACK_BASE_URL"] = base
    try:
        yield {"base": base, "store": app.extensions["mqmw"]["store"], "db": str(db_path)}
    finally:
        stop_server(server, thread)


def api_post(base: str, path: str, body: dict[str, Any], timeout: float = 30.0):
    response = requests.post(base + path, json=body, timeout=timeout)
    try:
        payload = response.json()
    except ValueError:
        payload = {"text": response.text}
    return response.status_code, payload


def load_fixed_samples(store: SQLiteStore, runtime_dir: Path, consumer_base: str):
    source = json.loads((ROOT / "samples" / "consumers.json").read_text(encoding="utf-8"))
    for consumer in source["consumers"]:
        consumer["endpoint"] = consumer["endpoint"].replace(
            "http://127.0.0.1:8091", consumer_base
        )
    consumers_path = runtime_dir / "consumers_runtime.json"
    consumers_path.write_text(
        json.dumps(source, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return load_samples(
        store,
        queues_path=str(ROOT / "samples" / "queues.json"),
        consumers_path=str(consumers_path),
        messages_path=str(ROOT / "samples" / "messages.json"),
    )


def scenario_exactly_once(runtime_dir: Path, consumer_base: str):
    db = runtime_dir / "exactly_once.db"
    with middleware_stack(db) as mw:
        store = mw["store"]
        load_fixed_samples(store, runtime_dir, consumer_base)
        status, result = api_post(
            mw["base"],
            "/v1/rounds",
            {
                "request_id": "verify-exact-round",
                "mode": "exactly_once",
                "consumer_id": "payments-worker",
                "config": {
                    "global_concurrency": 2,
                    "per_queue_inflight": 2,
                    "time_budget_ms": 700,
                    "dispatch_timeout_ms": 350,
                    "wait_queue_capacity": 8,
                },
            },
        )
        assert status == 200, (status, result)
        assert result["http_accepted"] == 3
        assert result["acks_observed"] == 3
        snapshot = store.snapshot("payments-exact")
        delivery = snapshot["deliveries"][0]
        dup_status, dup = api_post(
            mw["base"],
            "/v1/acks",
            {
                "request_id": "verify-exact-duplicate-ack",
                "delivery_id": delivery["delivery_id"],
                "lease_token": delivery["lease_token"],
                "consumer_id": "payments-worker",
                "mode": "exactly_once",
            },
        )
        assert dup_status == 409 and dup["error"]["code"] == "E_DUPLICATE_ACK"
        summary = store.get_queue_summary("payments-exact")
        assert summary["active_deliveries"] == 0
        assert summary["status_counts"]["ACKED"] == 3
        return {
            "ok": True,
            "round_status": result["status"],
            "delivered": result["http_accepted"],
            "acked": result["acks_observed"],
            "duplicate_ack": {"http_status": dup_status, "code": dup["error"]["code"]},
            "active_deliveries_after_duplicate": summary["active_deliveries"],
            "group_offset": summary["group_offsets"],
            "trace_events": sorted({t["event"] for t in result["traces"]}),
        }


def scenario_at_least_once(runtime_dir: Path, consumer_base: str):
    db = runtime_dir / "at_least_once.db"
    with middleware_stack(db) as mw:
        store = mw["store"]
        load_fixed_samples(store, runtime_dir, consumer_base)
        config = {
            "global_concurrency": 1,
            "per_queue_inflight": 1,
            "time_budget_ms": 400,
            "dispatch_timeout_ms": 200,
            "wait_queue_capacity": 3,
        }
        status1, first = api_post(
            mw["base"],
            "/v1/rounds",
            {
                "request_id": "verify-alo-round-1",
                "mode": "at_least_once",
                "consumer_id": "orders-worker",
                "config": config,
            },
        )
        assert status1 == 200
        assert first["claimed"] == 1
        assert first["http_accepted"] == 1
        time.sleep(0.35)
        reap_status, reap = api_post(
            mw["base"], "/v1/reap", {"request_id": "verify-alo-reap"}
        )
        assert reap_status == 200
        redelivered = store.get_message("orders-hot", "order-001")
        assert redelivered["status"] == "REDELIVERED"
        assert redelivered["redelivery_count"] == 1
        status2, second = api_post(
            mw["base"],
            "/v1/rounds",
            {
                "request_id": "verify-alo-round-2",
                "mode": "at_least_once",
                "consumer_id": "orders-worker",
                "config": config,
            },
        )
        assert status2 == 200
        assert second["redelivered_attempts"] == 1
        assert second["per_queue"]["orders-hot"]["claimed"] == 1
        snap = store.snapshot("orders-hot")
        attempt2 = max(snap["deliveries"], key=lambda d: d["attempt"])
        assert attempt2["attempt"] == 2
        ack_status, ack = api_post(
            mw["base"],
            "/v1/acks",
            {
                "request_id": "verify-alo-ack-attempt2",
                "delivery_id": attempt2["delivery_id"],
                "lease_token": attempt2["lease_token"],
                "consumer_id": "orders-worker",
                "mode": "at_least_once",
            },
        )
        assert ack_status == 200
        summary = store.get_queue_summary("orders-hot")
        offset = next(o for o in summary["group_offsets"] if o["group_name"] == "orders-group")
        assert offset["next_seq"] == 2
        assert store.get_message("orders-hot", "order-002")["status"] == "PENDING"
        return {
            "ok": True,
            "first_round": {
                "claimed": first["claimed"],
                "accepted": first["http_accepted"],
            },
            "reap_actions": reap["actions"],
            "redelivery": {
                "status": redelivered["status"],
                "redelivery_count": redelivered["redelivery_count"],
                "second_round_redelivered_attempts": second["redelivered_attempts"],
                "attempt": attempt2["attempt"],
            },
            "group_offset_after_ack": offset["next_seq"],
            "successor_order_002_still_pending": True,
        }


def scenario_timeout_and_mixed_queues(runtime_dir: Path, consumer_base: str):
    db = runtime_dir / "mixed_timeout.db"
    with middleware_stack(db) as mw:
        store = mw["store"]
        load_fixed_samples(store, runtime_dir, consumer_base)
        started = time.monotonic()
        status, result = api_post(
            mw["base"],
            "/v1/rounds",
            {
                "request_id": "verify-mixed-round",
                "mode": "at_least_once",
                "config": {
                    "global_concurrency": 2,
                    "per_queue_inflight": 1,
                    "time_budget_ms": 1000,
                    "dispatch_timeout_ms": 180,
                    "wait_queue_capacity": 24,
                },
            },
        )
        elapsed = time.monotonic() - started
        assert status == 200
        assert result["dispatch_timeouts"] >= 1
        assert result["http_accepted"] >= 1
        assert result["dead_lettered"] == 2
        assert elapsed < 1.2
        timeout_out = next(o for o in result["outcomes"] if o["outcome"] == "TIMED_OUT")
        assert timeout_out["connection_closed"] is True
        assert store.get_message("hang-queue", "hang-001")["status"] == "REDELIVERED"
        return {
            "ok": True,
            "elapsed_seconds": round(elapsed, 3),
            "claimed": result["claimed"],
            "accepted": result["http_accepted"],
            "timeouts": result["dispatch_timeouts"],
            "dead_lettered": result["dead_lettered"],
            "timeout_connection_closed": timeout_out["connection_closed"],
            "hang_message_state": store.get_message("hang-queue", "hang-001")["status"],
            "error_codes": sorted(set(result["error_codes"])),
        }


def _budget_round(db: Path, runtime_dir: Path, consumer_base: str, request_id: str):
    with middleware_stack(db) as mw:
        store = mw["store"]
        load_fixed_samples(store, runtime_dir, consumer_base)
        status, result = api_post(
            mw["base"],
            "/v1/rounds",
            {
                "request_id": request_id,
                "mode": "at_least_once",
                "consumer_id": "budget-worker",
                "config": {
                    "global_concurrency": 1,
                    "per_queue_inflight": 1,
                    "time_budget_ms": 600,
                    "dispatch_timeout_ms": 200,
                    "wait_queue_capacity": 1,
                    "max_scan_messages": 100,
                    "strict_wait_queue": False,
                },
            },
        )
        assert status == 200
        skipped = [
            (s["queue_name"], s["message_id"], s["reason"])
            for s in result["degradation_order"]
        ]
        pending = [
            m["message_id"]
            for m in store.snapshot("budget-longtail")["messages"]
            if m["status"] == "PENDING"
        ]
        assert pending
        return {
            "request_id": request_id,
            "claimed": result["claimed"],
            "skipped": skipped,
            "skip_order": [s["message_id"] for s in result["degradation_order"]],
            "pending_messages": pending,
            "wait_queue_overflow": result["wait_queue_overflow"],
            "error_codes": result["error_codes"],
            "queue_order": result["queue_order"],
        }


def scenario_budget_degradation(runtime_dir: Path, consumer_base: str):
    strict_db = runtime_dir / "budget_strict.db"
    with middleware_stack(strict_db) as mw:
        store = mw["store"]
        load_fixed_samples(store, runtime_dir, consumer_base)
        status, strict_result = api_post(
            mw["base"],
            "/v1/rounds",
            {
                "request_id": "verify-budget-strict",
                "mode": "at_least_once",
                "consumer_id": "budget-worker",
                "config": {
                    "global_concurrency": 1,
                    "per_queue_inflight": 1,
                    "time_budget_ms": 600,
                    "dispatch_timeout_ms": 200,
                    "wait_queue_capacity": 1,
                    "strict_wait_queue": True,
                },
            },
        )
        assert status == 429
        assert strict_result["error"]["code"] == "E_QUEUE_FULL"
        assert len(store.snapshot("budget-longtail")["deliveries"]) == 0

    first = _budget_round(
        runtime_dir / "budget_nonstrict_a.db",
        runtime_dir,
        consumer_base,
        "verify-budget-a",
    )
    second = _budget_round(
        runtime_dir / "budget_nonstrict_b.db",
        runtime_dir,
        consumer_base,
        "verify-budget-b",
    )
    assert first["skip_order"] == second["skip_order"]
    assert first["skipped"] == second["skipped"]
    return {
        "ok": True,
        "strict_response": {
            "http_status": status,
            "code": strict_result["error"]["code"],
            "dispatch_attempts": strict_result["dispatch_attempts"],
        },
        "deterministic_run_a": first,
        "deterministic_run_b": {
            "skip_order": second["skip_order"],
            "claimed": second["claimed"],
        },
        "identical_degradation_across_fresh_databases": True,
    }


def scenario_claim_and_request_idempotency(runtime_dir: Path, consumer_base: str):
    db = runtime_dir / "concurrency.db"
    with middleware_stack(db) as mw:
        store = mw["store"]
        store.register_queue("lease-q", 1)
        store.register_consumer(
            "lease-c", "lease-g", consumer_base + "/consume?behavior=success", ["lease-q"]
        )
        store.enqueue(
            "lease-q",
            [
                {
                    "message_id": "single-lease",
                    "body": {"n": 1},
                    "visibility_timeout_ms": 10_000,
                    "max_retries": 2,
                }
            ],
        )
        barrier = threading.Barrier(4)

        def claim(index):
            barrier.wait(timeout=5)
            status, body = api_post(
                mw["base"],
                "/v1/claims",
                {
                    "request_id": f"concurrent-claim-{index}",
                    "queue_name": "lease-q",
                    "consumer_id": "lease-c",
                    "mode": "at_least_once",
                },
            )
            return status, body

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(claim, range(4)))
        winners = [body for status, body in results if body.get("delivery")]
        assert len(winners) == 1
        assert len(store.get_message("lease-q", "single-lease")["deliveries"]) == 1

        store.register_queue("dedupe-q", 1)
        store.register_consumer(
            "dedupe-c", "dedupe-g", consumer_base + "/consume?behavior=success", ["dedupe-q"]
        )
        barrier2 = threading.Barrier(4)
        payload = {
            "request_id": "same-enqueue-request",
            "queue_name": "dedupe-q",
            "messages": [
                {
                    "message_id": "same-message",
                    "body": {"n": 1},
                    "visibility_timeout_ms": 10_000,
                    "max_retries": 1,
                }
            ],
        }

        def enqueue_same(_index):
            barrier2.wait(timeout=5)
            return api_post(mw["base"], "/v1/enqueue", payload)

        with ThreadPoolExecutor(max_workers=4) as pool:
            enqueued = list(pool.map(enqueue_same, range(4)))
        seqs = {body["messages"][0]["seq"] for status, body in enqueued if status == 200}
        assert seqs == {1}
        assert store.get_queue_summary("dedupe-q")["next_seq"] == 2
        return {
            "ok": True,
            "concurrent_claim_winners": len(winners),
            "lease_delivery_rows": len(
                store.get_message("lease-q", "single-lease")["deliveries"]
            ),
            "same_request_responses": len(enqueued),
            "same_sequence_returned_by_all": sorted(seqs),
            "dedupe_queue_next_seq": store.get_queue_summary("dedupe-q")["next_seq"],
        }


def scenario_forced_termination_recovery(runtime_dir: Path, _consumer_base: str = ""):
    helper = ROOT / "tests" / "crash_helper.py"
    results = {}
    for mode in ("dispatch", "ack"):
        db = runtime_dir / f"crash_{mode}.db"
        env = os.environ.copy()
        env.pop("MQMW_FAULT_POINT", None)
        process = subprocess.run(
            [sys.executable, str(helper), str(db), mode],
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert process.returncode == 97, (process.returncode, process.stdout, process.stderr)
        store = SQLiteStore(str(db))
        message_id = "crash-dispatch" if mode == "dispatch" else "crash-ack"
        before = store.get_message("crash-q", message_id)
        report = store.recover()
        after = store.get_message("crash-q", message_id)
        if mode == "dispatch":
            assert before["status"] == "DISPATCHED"
            assert after["status"] == "PENDING"
            assert report["dispatch_rollbacks"]
        else:
            assert before["status"] == "ACKED"
            assert after["status"] == "DISPATCHED"
            assert report["ack_rollbacks"]
        results[mode] = {
            "forced_exit_code": process.returncode,
            "before_state": before["status"],
            "after_state": after["status"],
            "report_action": (
                report["dispatch_rollbacks"] or report["ack_rollbacks"]
            )[0]["action"],
            "durable_facts": report["durable_facts_after_recovery"],
        }
    return {"ok": True, **results}


def run_pytest() -> dict[str, Any]:
    process = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=180,
    )
    return {
        "returncode": process.returncode,
        "passed": process.returncode == 0,
        "summary": process.stdout.strip().splitlines()[-1] if process.stdout.strip() else "",
        "stdout_tail": "\n".join(process.stdout.strip().splitlines()[-8:]),
        "stderr_tail": "\n".join(process.stderr.strip().splitlines()[-8:]),
    }


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="artifacts/verification_report.json")
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    runtime_dir = ROOT / "artifacts" / "runtime" / f"verification_{stamp}"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    consumer_app = create_consumer_app()
    consumer_server, consumer_thread, consumer_base = start_server(consumer_app)
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "loopback_only": True,
            "containerized": False,
        },
        "samples": {
            "queues_sha256": sha256(ROOT / "samples" / "queues.json"),
            "consumers_sha256": sha256(ROOT / "samples" / "consumers.json"),
            "messages_sha256": sha256(ROOT / "samples" / "messages.json"),
            "rounds_sha256": sha256(ROOT / "samples" / "rounds.json"),
        },
        "runtime_dir": str(runtime_dir),
        "consumer_endpoint": consumer_base,
        "scenarios": {},
    }
    scenario_functions = {
        "exactly_once_auto_ack_and_duplicate_ack": scenario_exactly_once,
        "at_least_once_visibility_redelivery": scenario_at_least_once,
        "parallel_timeout_does_not_block": scenario_timeout_and_mixed_queues,
        "deterministic_budget_degradation": scenario_budget_degradation,
        "concurrent_lease_and_request_idempotency": scenario_claim_and_request_idempotency,
        "forced_termination_recovery": scenario_forced_termination_recovery,
    }
    overall_ok = True
    try:
        for name, function in scenario_functions.items():
            try:
                result = function(runtime_dir, consumer_base)
                report["scenarios"][name] = result
            except BaseException as exc:
                overall_ok = False
                report["scenarios"][name] = {
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(),
                }
        if not args.skip_tests:
            report["pytest"] = run_pytest()
            overall_ok = overall_ok and report["pytest"]["passed"]
    finally:
        stop_server(consumer_server, consumer_thread)

    report["ok"] = overall_ok
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"ok": overall_ok, "output": str(output), "scenarios": {
        name: item.get("ok") for name, item in report["scenarios"].items()
    }, "pytest": report.get("pytest", {}).get("passed")}, ensure_ascii=False, indent=2))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
