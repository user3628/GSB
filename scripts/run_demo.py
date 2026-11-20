#!/usr/bin/env python
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gradsync.samples import (  # noqa: E402
    load_sample_manifest,
    manifest_to_rounds,
    write_sample_manifest,
)

ARTIFACTS = ROOT / "artifacts"
LOGS = ARTIFACTS / "logs"
SAMPLES_PATH = ROOT / "data" / "samples.json"
REPORT_PATH = ARTIFACTS / "demo_report.json"
WORKER_IDS = ("worker-a", "worker-b", "worker-c")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def clean_file(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)


class ManagedProcess:
    def __init__(self, command: list[str], *, log_path: Path, env: dict[str, str] | None = None) -> None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path = log_path
        self._log_handle = log_path.open("w", encoding="utf-8", newline="\n")
        merged_env = os.environ.copy()
        merged_env["PYTHONUNBUFFERED"] = "1"
        merged_env["PYTHONPATH"] = str(ROOT)
        if env:
            merged_env.update(env)
        self.process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=merged_env,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def tail(self, limit: int = 3000) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8")[-limit:]
        except OSError:
            return ""

    def stop(self, timeout: float = 8.0) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=timeout)
        self._log_handle.close()


def wait_http(base_url: str, process: ManagedProcess, *, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not started"
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise RuntimeError(
                f"process exited early ({process.process.returncode})\n{process.tail()}"
            )
        try:
            response = httpx.get(f"{base_url}/health", timeout=0.8)
            if response.is_success:
                return
            last_error = f"HTTP {response.status_code}: {response.text[:300]}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.1)
    raise RuntimeError(f"service did not become healthy: {base_url}: {last_error}\n{process.tail()}")


def call(method: str, url: str, *, json_body: Any = None, timeout: float = 15.0) -> dict[str, Any]:
    try:
        response = httpx.request(method, url, json=json_body, timeout=timeout)
        try:
            body = response.json()
        except Exception:
            body = {"text": response.text[:1000]}
        return {"http_status": response.status_code, "body": body}
    except Exception as exc:
        return {
            "http_status": None,
            "body": {"code": None, "error": f"{type(exc).__name__}: {exc}"},
        }


def start_scheduler(port: int, db_path: Path, *, crash: bool = False) -> ManagedProcess:
    env: dict[str, str] = {}
    if crash:
        env["GRADSYNC_CRASH_AT"] = "before_aggregated_commit"
        env["GRADSYNC_CRASH_MARKER"] = str(ARTIFACTS / "crash_marker.json")
    process = ManagedProcess(
        [
            sys.executable,
            "-m",
            "gradsync",
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--db",
            str(db_path),
            "--log-level",
            "warning",
        ],
        log_path=LOGS / ("scheduler-crash.log" if crash else "scheduler.log"),
        env=env,
    )
    wait_http(f"http://127.0.0.1:{port}", process)
    return process


def start_workers() -> tuple[dict[str, str], list[ManagedProcess]]:
    endpoints: dict[str, str] = {}
    processes: list[ManagedProcess] = []
    for node_id in WORKER_IDS:
        port = free_port()
        process = ManagedProcess(
            [
                sys.executable,
                "-m",
                "gradsync",
                "worker",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--name",
                node_id,
                "--log-level",
                "warning",
            ],
            log_path=LOGS / f"{node_id}.log",
        )
        endpoint = f"http://127.0.0.1:{port}"
        wait_http(endpoint, process)
        endpoints[node_id] = endpoint
        processes.append(process)
    return endpoints, processes


def create_round(
    scheduler_url: str,
    sample: Any,
    endpoints: dict[str, str],
    *,
    collect_seconds: float | None = None,
) -> dict[str, Any]:
    payload = sample.create_request(
        worker_endpoints=endpoints,
        collect_seconds=collect_seconds,
    )
    response = call("POST", f"{scheduler_url}/rounds", json_body=payload)
    assert response["http_status"] == 201, response
    return response


def dispatch_batch(
    worker_url: str,
    scheduler_url: str,
    round_id: str,
    items: list[dict[str, Any]],
    *,
    timeout: float = 20.0,
) -> dict[str, Any]:
    response = call(
        "POST",
        f"{worker_url}/run",
        json_body={
            "scheduler_url": scheduler_url,
            "round_id": round_id,
            "submissions": items,
        },
        timeout=timeout,
    )
    return response


def wait_state(
    scheduler_url: str,
    round_id: str,
    desired: str,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = call("GET", f"{scheduler_url}/rounds/{round_id}")
        if last.get("http_status") == 200 and last["body"].get("state") == desired:
            return last["body"]
        time.sleep(0.05)
    raise RuntimeError(f"round {round_id} did not reach {desired}; last={last}")


def event_trace(events: list[dict[str, Any]]) -> list[str]:
    return [
        f"{event['event_type']}[{event.get('from_state') or '-'}->{event.get('to_state') or '-'}]"
        for event in events
    ]


def result_summary(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": result.get("code"),
        "state": result.get("state"),
        "degraded": result.get("degraded"),
        "missing_nodes": result.get("missing_nodes", []),
        "digest_sha256": result.get("digest_sha256"),
        "element_sum": result.get("element_sum"),
        "tensor_preview": result.get("tensor_preview"),
        "snapshot_hash": result.get("snapshot_hash"),
    }


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    write_sample_manifest(SAMPLES_PATH)
    samples = manifest_to_rounds(load_sample_manifest(SAMPLES_PATH))

    normal_db = ARTIFACTS / "demo_scheduler.db"
    crash_db = ARTIFACTS / "demo_crash.db"
    marker_path = ARTIFACTS / "crash_marker.json"
    for path in (normal_db, crash_db):
        clean_file(path)
    marker_path.unlink(missing_ok=True)
    REPORT_PATH.unlink(missing_ok=True)

    report: dict[str, Any] = {
        "ok": False,
        "generated_at": time.time(),
        "python": sys.version,
        "samples_file": str(SAMPLES_PATH),
        "scenarios": {},
        "checks": {},
    }
    scheduler_process: ManagedProcess | None = None
    crash_process: ManagedProcess | None = None
    worker_processes: list[ManagedProcess] = []
    try:
        worker_endpoints, worker_processes = start_workers()
        scheduler_port = free_port()
        scheduler_url = f"http://127.0.0.1:{scheduler_port}"
        scheduler_process = start_scheduler(scheduler_port, normal_db)

        # 1. Complete round, delivered to every local worker.
        complete = samples["demo-complete"]
        created = create_round(scheduler_url, complete, worker_endpoints, collect_seconds=8.0)
        dispatch_results = []
        for delay, node_id in zip((0.0, 0.06, 0.12), complete.worker_ids):
            dispatch_results.append(
                dispatch_batch(
                    worker_endpoints[node_id],
                    scheduler_url,
                    complete.round_id,
                    complete.submissions_for(node_id, delay_seconds=delay),
                )
            )
        assert_ok(all(item["http_status"] == 200 for item in dispatch_results), "complete dispatch failed")
        ready = wait_state(scheduler_url, complete.round_id, "READY")
        finalize = call(
            "POST",
            f"{scheduler_url}/rounds/{complete.round_id}/finalize",
            json_body={"deliver": True},
        )
        assert_ok(finalize["http_status"] == 200, f"complete finalize failed: {finalize}")
        complete_result = finalize["body"]
        assert_ok(complete_result["code"] == "OK", "complete round was not OK")
        assert_ok(
            complete_result["digest_sha256"] == complete.expected_digest(),
            "complete round digest mismatch",
        )
        delivery_ok = finalize["body"].get("delivery", {}).get("ok")
        assert_ok(bool(delivery_ok), "complete result delivery failed")
        delivered_states = {}
        for node_id, endpoint in worker_endpoints.items():
            worker_state = call("GET", f"{endpoint}/results")["body"]
            delivered_states[node_id] = bool(worker_state.get("results", {}).get(complete.round_id))
        assert_ok(all(delivered_states.values()), "worker did not store delivered result")
        complete_status = call("GET", f"{scheduler_url}/rounds/{complete.round_id}")["body"]
        report["scenarios"]["complete"] = {
            **result_summary(complete_result),
            "expected_digest_sha256": complete.expected_digest(),
            "deadline": created["body"]["collect_deadline_at"],
            "event_trace": event_trace(complete_status["events"]),
            "delivered_to_workers": delivered_states,
        }

        # 2. Strict late round: early finalize is E_BUSY, late blocks still participate.
        strict = samples["demo-strict-late"]
        created = create_round(scheduler_url, strict, worker_endpoints, collect_seconds=0.7)
        for node_id in ("worker-a", "worker-b"):
            response = dispatch_batch(
                worker_endpoints[node_id],
                scheduler_url,
                strict.round_id,
                strict.submissions_for(node_id),
            )
            assert_ok(response["http_status"] == 200, f"strict early dispatch failed: {response}")
        early = call("POST", f"{scheduler_url}/rounds/{strict.round_id}/finalize", json_body={})
        assert_ok(early["body"].get("code") == "E_BUSY", f"expected E_BUSY, got {early}")
        deadline = float(created["body"]["collect_deadline_at"])
        time.sleep(max(0.0, deadline - time.time() + 0.2))
        late_response = dispatch_batch(
            worker_endpoints["worker-c"],
            scheduler_url,
            strict.round_id,
            strict.submissions_for("worker-c"),
        )
        assert_ok(late_response["http_status"] == 200, f"strict late dispatch failed: {late_response}")
        late_flags = [
            entry["response"].get("late")
            for entry in late_response["body"]["submissions"]
        ]
        assert_ok(all(late_flags), f"strict late blocks were not marked late: {late_flags}")
        ready = wait_state(scheduler_url, strict.round_id, "READY")
        strict_final = call(
            "POST",
            f"{scheduler_url}/rounds/{strict.round_id}/finalize",
            json_body={"deliver": False},
        )
        assert_ok(strict_final["http_status"] == 200, f"strict finalize failed: {strict_final}")
        strict_result = strict_final["body"]
        assert_ok(
            strict_result["digest_sha256"] == strict.expected_digest(),
            "strict late round omitted late blocks",
        )
        strict_status = call("GET", f"{scheduler_url}/rounds/{strict.round_id}")["body"]
        report["scenarios"]["strict_late"] = {
            **result_summary(strict_result),
            "early_finalize_code": early["body"].get("code"),
            "late_blocks_marked": late_flags,
            "expected_digest_sha256": strict.expected_digest(),
            "event_trace": event_trace(strict_status["events"]),
        }

        # 3. Degraded round: late blocks enter the late queue but remain out of the snapshot.
        degraded = samples["demo-degraded"]
        created = create_round(scheduler_url, degraded, worker_endpoints, collect_seconds=0.5)
        for node_id in ("worker-a", "worker-b"):
            response = dispatch_batch(
                worker_endpoints[node_id],
                scheduler_url,
                degraded.round_id,
                degraded.submissions_for(node_id),
            )
            assert_ok(response["http_status"] == 200, f"degraded early dispatch failed: {response}")
        deadline = float(created["body"]["collect_deadline_at"])
        time.sleep(max(0.0, deadline - time.time() + 0.2))
        late_response = dispatch_batch(
            worker_endpoints["worker-c"],
            scheduler_url,
            degraded.round_id,
            degraded.submissions_for("worker-c"),
        )
        assert_ok(late_response["http_status"] == 200, f"degraded late dispatch failed: {late_response}")
        before_final = call("GET", f"{scheduler_url}/rounds/{degraded.round_id}")["body"]
        degraded_final = call(
            "POST",
            f"{scheduler_url}/rounds/{degraded.round_id}/finalize",
            json_body={"mode": "DEGRADED", "deliver": True},
        )
        assert_ok(degraded_final["http_status"] == 200, f"degraded finalize failed: {degraded_final}")
        degraded_result = degraded_final["body"]
        assert_ok(degraded_result["code"] == "E_DEGRADED_ROUND", "degraded code missing")
        assert_ok(degraded_result["missing_nodes"] == ["worker-c"], "degraded missing nodes mismatch")
        assert_ok(
            degraded_result["digest_sha256"] == degraded.expected_digest(("worker-a", "worker-b")),
            "degraded snapshot included late block",
        )
        degraded_status = call("GET", f"{scheduler_url}/rounds/{degraded.round_id}")["body"]
        report["scenarios"]["degraded"] = {
            **result_summary(degraded_result),
            "expected_digest_sha256": degraded.expected_digest(("worker-a", "worker-b")),
            "late_queue_before_finalize": before_final.get("late_blocks", []),
            "event_trace_before_finalize": event_trace(before_final.get("events", [])),
            "event_trace": event_trace(degraded_status["events"]),
        }

        # 4. Duplicate, conflicting retry, and arrival-order independence.
        idem_a = samples["demo-idempotent-a"]
        idem_b = samples["demo-idempotent-b"]
        create_round(scheduler_url, idem_a, worker_endpoints, collect_seconds=8.0)
        create_round(scheduler_url, idem_b, worker_endpoints, collect_seconds=8.0)
        for node_id, delay in (("worker-a", 0.0), ("worker-b", 0.03), ("worker-c", 0.06)):
            response = dispatch_batch(
                worker_endpoints[node_id], scheduler_url, idem_a.round_id,
                idem_a.submissions_for(node_id, delay_seconds=delay),
            )
            assert_ok(response["http_status"] == 200, f"idempotent-a dispatch failed: {response}")
        for node_id, delay in (("worker-c", 0.0), ("worker-b", 0.03), ("worker-a", 0.06)):
            response = dispatch_batch(
                worker_endpoints[node_id], scheduler_url, idem_b.round_id,
                idem_b.submissions_for(node_id, delay_seconds=delay),
            )
            assert_ok(response["http_status"] == 200, f"idempotent-b dispatch failed: {response}")

        first_item = idem_a.submissions_for("worker-a")[0]
        duplicate = dispatch_batch(
            worker_endpoints["worker-a"],
            scheduler_url,
            idem_a.round_id,
            [dict(first_item, delay_seconds=0.0)],
        )
        duplicate_code = duplicate["body"]["submissions"][0]["response"].get("outcome")
        varied = dict(first_item, delay_seconds=0.0)
        varied["gradient"] = list(first_item["gradient"])
        varied["gradient"][0] += 0.25
        conflict = dispatch_batch(
            worker_endpoints["worker-a"],
            scheduler_url,
            idem_a.round_id,
            [varied],
        )
        conflict_code = conflict["body"]["submissions"][0]["response"].get("code")
        assert_ok(duplicate_code == "DUPLICATE", f"duplicate not deduplicated: {duplicate}")
        assert_ok(conflict_code == "E_CONFLICTING_REPORT", f"conflict code missing: {conflict}")
        idem_a_final = call("POST", f"{scheduler_url}/rounds/{idem_a.round_id}/finalize", json_body={})
        idem_b_final = call("POST", f"{scheduler_url}/rounds/{idem_b.round_id}/finalize", json_body={})
        assert_ok(idem_a_final["http_status"] == 200, f"idempotent-a finalize failed: {idem_a_final}")
        assert_ok(idem_b_final["http_status"] == 200, f"idempotent-b finalize failed: {idem_b_final}")
        assert_ok(
            idem_a_final["body"]["digest_sha256"] == idem_b_final["body"]["digest_sha256"],
            "arrival order changed aggregate digest",
        )
        idem_status = call("GET", f"{scheduler_url}/rounds/{idem_a.round_id}")["body"]
        submission_count = len(idem_status["submissions"])
        assert_ok(submission_count == idem_a.shard_count * len(idem_a.worker_ids), "duplicate was counted")
        report["scenarios"]["idempotent_order"] = {
            "duplicate_outcome": duplicate_code,
            "conflict_code": conflict_code,
            "submission_count_after_retries": submission_count,
            "round_a_digest": idem_a_final["body"]["digest_sha256"],
            "round_b_digest": idem_b_final["body"]["digest_sha256"],
            "digests_equal": idem_a_final["body"]["digest_sha256"] == idem_b_final["body"]["digest_sha256"],
            "event_trace_round_a": event_trace(idem_status["events"]),
        }

        # 5. Zero weight is a business error, not a divide-by-zero crash.
        zero = samples["demo-zero-weight"]
        create_round(scheduler_url, zero, worker_endpoints, collect_seconds=8.0)
        for node_id in zero.worker_ids:
            response = dispatch_batch(
                worker_endpoints[node_id], scheduler_url, zero.round_id, zero.submissions_for(node_id)
            )
            assert_ok(response["http_status"] == 200, f"zero-weight dispatch failed: {response}")
        zero_response = call("POST", f"{scheduler_url}/rounds/{zero.round_id}/finalize", json_body={})
        assert_ok(zero_response["http_status"] == 422, f"zero weight status mismatch: {zero_response}")
        assert_ok(zero_response["body"].get("code") == "E_ZERO_WEIGHT", "zero weight code missing")
        zero_status = call("GET", f"{scheduler_url}/rounds/{zero.round_id}")["body"]

        # 6. Invalid transition and unknown round are non-mutating errors.
        invalid = call("POST", f"{scheduler_url}/rounds/{complete.round_id}/abort", json_body={})
        unknown = call("GET", f"{scheduler_url}/rounds/does-not-exist")
        assert_ok(invalid["body"].get("code") == "E_INVALID_TRANSITION", f"invalid code: {invalid}")
        assert_ok(unknown["body"].get("code") == "E_ROUND_NOT_FOUND", f"unknown code: {unknown}")
        report["scenarios"]["business_errors"] = {
            "zero_weight": {
                "http_status": zero_response["http_status"],
                "code": zero_response["body"].get("code"),
                "message": zero_response["body"].get("message"),
                "event_trace": event_trace(zero_status.get("events", [])),
            },
            "invalid_transition": {
                "http_status": invalid["http_status"],
                "code": invalid["body"].get("code"),
            },
            "round_not_found": {
                "http_status": unknown["http_status"],
                "code": unknown["body"].get("code"),
            },
        }

        scheduler_process.stop()
        scheduler_process = None

        # 7. Real forced termination after intent commit and before AGGREGATED commit.
        crash_port = free_port()
        crash_url = f"http://127.0.0.1:{crash_port}"
        crash_process = start_scheduler(crash_port, crash_db, crash=True)
        crash_sample = samples["demo-crash"]
        create_round(crash_url, crash_sample, worker_endpoints, collect_seconds=8.0)
        for delay, node_id in zip((0.0, 0.03, 0.06), crash_sample.worker_ids):
            response = dispatch_batch(
                worker_endpoints[node_id],
                crash_url,
                crash_sample.round_id,
                crash_sample.submissions_for(node_id, delay_seconds=delay),
            )
            assert_ok(response["http_status"] == 200, f"crash dispatch failed: {response}")
        wait_state(crash_url, crash_sample.round_id, "READY")
        crash_request = call(
            "POST",
            f"{crash_url}/rounds/{crash_sample.round_id}/finalize",
            json_body={},
            timeout=15.0,
        )
        crash_exit_code = crash_process.process.wait(timeout=15.0)
        crash_process.stop()
        crash_process = None
        assert_ok(crash_exit_code == 91, f"unexpected crash exit code: {crash_exit_code}")
        assert_ok(marker_path.exists(), "crash marker was not written")

        with sqlite3.connect(str(crash_db)) as conn:
            conn.row_factory = sqlite3.Row
            pre_round = conn.execute(
                "SELECT status FROM rounds WHERE round_id=?", (crash_sample.round_id,)
            ).fetchone()
            pre_intent = conn.execute(
                "SELECT status,error_code FROM intents WHERE round_id=? ORDER BY created_at DESC LIMIT 1",
                (crash_sample.round_id,),
            ).fetchone()
            result_count = conn.execute(
                "SELECT COUNT(*) FROM aggregate_results WHERE round_id=?",
                (crash_sample.round_id,),
            ).fetchone()[0]
        assert_ok(pre_round["status"] == "READY", f"pre-restart state: {dict(pre_round)}")
        assert_ok(pre_intent["status"] == "PENDING", f"pre-restart intent: {dict(pre_intent)}")
        assert_ok(result_count == 0, "crash happened after aggregate result commit")

        restarted = start_scheduler(crash_port, crash_db, crash=False)
        try:
            recovery = call("GET", f"{crash_url}/recovery/report")["body"]
            recovered_status = call("GET", f"{crash_url}/rounds/{crash_sample.round_id}")["body"]
            recovered_result = call("GET", f"{crash_url}/rounds/{crash_sample.round_id}/result")["body"]
            assert_ok(recovered_status["state"] == "AGGREGATED", "recovery did not aggregate")
            assert_ok(
                recovered_result["digest_sha256"] == crash_sample.expected_digest(),
                "recovered digest mismatch",
            )
            assert_ok(
                any(entry["outcome"] == "RECOVERED" for entry in recovery.get("entries", [])),
                "recovery report has no RECOVERED entry",
            )
            delivered = call("POST", f"{crash_url}/rounds/{crash_sample.round_id}/deliver")["body"]
            report["scenarios"]["crash_recovery"] = {
                "crash_exit_code": crash_exit_code,
                "crash_request_error": crash_request["body"].get("error"),
                "marker": json.loads(marker_path.read_text(encoding="utf-8")),
                "pre_restart": {
                    "round_state": pre_round["status"],
                    "intent_status": pre_intent["status"],
                    "aggregate_result_count": result_count,
                },
                "post_restart": {
                    "round_state": recovered_status["state"],
                    "intent_status": next(
                        event["details"].get("intent_id")
                        for event in recovered_status["events"]
                        if event["event_type"] == "ROUND_AGGREGATED"
                    ),
                    "digest_sha256": recovered_result["digest_sha256"],
                    "element_sum": recovered_result["element_sum"],
                    "expected_digest_sha256": crash_sample.expected_digest(),
                    "delivery_ok": delivered.get("ok"),
                    "event_trace": event_trace(recovered_status["events"]),
                },
                "recovery_report": recovery,
            }
        finally:
            restarted.stop()

        report["ok"] = True
        report["finished_at"] = time.time()
        report["checks"] = {
            "complete_allreduce": True,
            "strict_late_included": True,
            "degraded_missing_nodes": True,
            "idempotent_duplicate_and_conflict": True,
            "arrival_order_independent": True,
            "zero_weight_business_code": True,
            "invalid_transition_and_unknown_round": True,
            "forced_termination_recovered_from_sqlite": True,
        }
        REPORT_PATH.write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )

        print("GradSync 本地确定性演示通过")
        for name in ("complete", "strict_late", "degraded", "idempotent_order", "business_errors"):
            scenario = report["scenarios"][name]
            if "digest_sha256" in scenario:
                print(
                    f"[{name}] code={scenario['code']} missing={scenario['missing_nodes']} "
                    f"digest={scenario['digest_sha256']} sum={scenario['element_sum']}"
                )
            else:
                print(f"[{name}] {json.dumps(scenario, ensure_ascii=False, sort_keys=True)}")
        print(
            "[crash_recovery] "
            f"pre={report['scenarios']['crash_recovery']['pre_restart']} "
            f"digest={report['scenarios']['crash_recovery']['post_restart']['digest_sha256']}"
        )
        print(
            "state_trace complete: "
            + " -> ".join(report["scenarios"]["complete"]["event_trace"])
        )
        print(
            "state_trace degraded: "
            + " -> ".join(report["scenarios"]["degraded"]["event_trace"])
        )
        print(f"artifact: {REPORT_PATH}")
        return 0
    except Exception as exc:
        report["ok"] = False
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["finished_at"] = time.time()
        REPORT_PATH.write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        print(f"DEMO FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        print(f"partial report: {REPORT_PATH}", file=sys.stderr)
        return 1
    finally:
        for process in (scheduler_process, crash_process):
            if process is not None:
                process.stop()
        for process in worker_processes:
            process.stop()


if __name__ == "__main__":
    raise SystemExit(main())

