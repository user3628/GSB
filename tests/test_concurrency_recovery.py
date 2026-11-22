from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from quota_scheduler.service import QuotaScheduler

from conftest import make_service, request_payload, write_bundle


def test_readers_never_see_half_committed_batching(tmp_path) -> None:
    service = make_service(tmp_path)
    service.submit_request(request_payload("r1", memory="2Gi", samples=2), auto_admit=True)
    entered = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []
    original_failpoint = service.db._maybe_failpoint

    def pause_before_commit(kind: str) -> None:
        if kind == "ORGANIZE_BATCHES":
            entered.set()
            assert release.wait(5)
            return
        original_failpoint(kind)

    service.db._maybe_failpoint = pause_before_commit  # type: ignore[method-assign]

    def writer() -> None:
        try:
            service.organize_batches(strategy="greedy")
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    assert entered.wait(5)
    during = service.get_request("r1")
    assert during["status"] == "ADMITTED"
    assert during["reservation"]["active"] is True
    assert during["batches"] == []
    assert service.list_batches()["count"] == 0
    release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    after = service.get_request("r1")
    assert after["status"] == "BATCHED"
    assert after["reservation"]["active"] is True
    assert len(after["batches"]) == 1
    assert service.state_summary()["invariants"]["ok"] is True


def test_killed_batch_transaction_recovers_to_last_complete_version(tmp_path) -> None:
    service = make_service(tmp_path)
    service.submit_request(request_payload("r1", memory="2Gi", samples=2), auto_admit=True)
    env = os.environ.copy()
    env["QUOTA_SCHEDULER_CRASH_AT"] = "ORGANIZE_BATCHES:before_commit"
    proc = subprocess.run(
        [sys.executable, "-m", "quota_scheduler", "--db", service.database_path, "organize"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert proc.returncode != 0
    journal_path = Path(service.database_path + ".intents.jsonl")
    journal_before = journal_path.read_text(encoding="utf-8")
    assert '"event":"PREPARED"' in journal_before
    assert "ORGANIZE_BATCHES" in journal_before

    restarted = QuotaScheduler(service.database_path)
    prepared = restarted.list_intents("PREPARED")["intents"]
    assert len(prepared) == 1
    assert prepared[0]["kind"] == "ORGANIZE_BATCHES"
    request = restarted.get_request("r1")
    assert request["status"] == "ADMITTED"
    assert request["batches"] == []
    assert restarted.list_batches()["count"] == 0
    assert restarted.quota_snapshot()["nodes"]["n1"]["allocated_bytes"] == 0

    report = restarted.recover(abort_running=False)
    assert report["prepared_intent_count"] == 1
    assert report["resolved_intents"][0]["resolution"] == "ABORTED"
    assert restarted.list_intents("PREPARED")["count"] == 0
    assert restarted.recovery_status()["exists"] is True
    journal_after = journal_path.read_text(encoding="utf-8")
    assert '"event":"ABORTED"' in journal_after
    assert restarted.state_summary()["invariants"]["ok"] is True


def test_killed_admission_leaves_no_reservation(tmp_path) -> None:
    service = make_service(tmp_path)
    service.submit_request(request_payload("r1", memory="2Gi"), auto_admit=False)
    env = os.environ.copy()
    env["QUOTA_SCHEDULER_CRASH_AT"] = "ADMIT_REQUEST:before_commit"
    proc = subprocess.run(
        [sys.executable, "-m", "quota_scheduler", "--db", service.database_path, "admit", "r1"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert proc.returncode != 0
    restarted = QuotaScheduler(service.database_path)
    pending = restarted.get_request("r1")
    assert pending["status"] == "PENDING"
    assert pending["reservation"] is None
    report = restarted.recover()
    assert report["prepared_intent_count"] == 1
    assert restarted.quota_snapshot()["quota_groups"]["g1"]["reserved_bytes"] == 0


def test_recovery_aborts_persisted_running_batches(tmp_path) -> None:
    service = make_service(tmp_path)
    service.submit_request(request_payload("r1", memory="2Gi", samples=2), auto_admit=True)
    service.organize_batches(strategy="greedy")
    scheduled = service.schedule_round(max_parallel_batches=1)
    batch_id = scheduled["selected"][0]["batch_id"]
    assert service.get_batch(batch_id)["status"] == "RUNNING"

    restarted = QuotaScheduler(service.database_path)
    report = restarted.recover(abort_running=True)
    assert report["aborted_running_batches"]
    assert restarted.get_batch(batch_id)["status"] == "ABORTED"
    request = restarted.get_request("r1")
    assert request["status"] == "PREEMPTED"
    assert request["reservation"]["active"] is False
    assert restarted.quota_snapshot()["nodes"]["n1"]["allocated_bytes"] == 0
    assert restarted.state_summary()["invariants"]["ok"] is True


def test_persistence_across_service_instances(tmp_path) -> None:
    service = make_service(tmp_path)
    service.submit_request(request_payload("r1"), auto_admit=True)
    service.organize_batches(strategy="greedy")
    restarted = QuotaScheduler(service.database_path)
    request = restarted.get_request("r1")
    assert request["status"] == "BATCHED"
    assert restarted.list_batches()["count"] == 1
    assert restarted.state_summary()["invariants"]["ok"] is True

