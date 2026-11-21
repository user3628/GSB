from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from delay_queue.config import Settings
from delay_queue.service import DelayQueueService
from delay_queue.storage import Storage
from delay_queue.verify import validate_round

from .conftest import task


def round_body(**overrides):
    body = {
        "max_concurrency": 2,
        "time_budget_ms": 800,
        "max_task_resource": 100,
        "waiting_queue_capacity": 20,
        "dispatch_timeout_ms": 200,
    }
    body.update(overrides)
    return body


def test_timeout_retry_backoff_dead_letter_and_verifier(service, endpoint, settings):
    _, state = endpoint
    service.submit_tasks(
        "q",
        [task("hang-task", mode="hang", hang_seconds=5, retries=1, priority=0)],
    )
    started = time.monotonic()
    first = asyncio.run(
        service.trigger_round(
            "q", "timeout1", round_body(dispatch_timeout_ms=60)
        )
    )
    elapsed = time.monotonic() - started
    assert elapsed < 1.5
    item = first["dispatched"][0]
    assert item["outcome"] == "TIMEOUT"
    assert item["error_code"] == "E_DISPATCH_TIMEOUT"
    assert item["http_status"] is None
    assert item["state_after"] == "RETRY_WAITING"
    assert service.storage.get_task("q", "hang-task")["remaining_retries"] == 0

    immediate = asyncio.run(service.trigger_round("q", "too-early", round_body()))
    assert immediate["counts"]["dispatched"] == 0
    assert immediate["counts"]["due_candidates"] == 0

    time.sleep(settings.retry_backoff_base_ms / 1_000 + 0.02)
    second = asyncio.run(service.trigger_round("q", "timeout2", round_body(dispatch_timeout_ms=50)))
    assert second["dispatched"][0]["outcome"] == "TIMEOUT"
    assert second["dispatched"][0]["state_after"] == "DEAD_LETTERED"
    assert service.storage.get_task("q", "hang-task")["state"] == "DEAD_LETTERED"
    assert state.snapshot()["total_requests"] == 2
    assert validate_round(settings.db_path, "timeout1", settings.retry_backoff_base_ms)["ok"]
    assert validate_round(settings.db_path, "timeout2", settings.retry_backoff_base_ms)["ok"]


def test_flaky_retry_then_success(service, settings):
    service.submit_tasks("q", [task("flaky", mode="flaky", retries=2)])
    first = asyncio.run(service.trigger_round("q", "flaky1", round_body(dispatch_timeout_ms=200)))
    assert first["dispatched"][0]["state_after"] == "RETRY_WAITING"
    first_attempt = service.storage.get_attempts_for_round("flaky1")[0]
    transition = service.storage.get_transitions_for_attempt(first_attempt["attempt_id"])[-1]
    assert transition["to_state"] == "RETRY_WAITING"
    assert transition["metadata"]["backoff_ms"] == settings.retry_backoff_base_ms

    time.sleep(settings.retry_backoff_base_ms / 1_000 + 0.02)
    second = asyncio.run(service.trigger_round("q", "flaky2", round_body(dispatch_timeout_ms=200)))
    assert second["dispatched"][0]["outcome"] == "SUCCEEDED"
    assert second["dispatched"][0]["state_after"] == "COMPLETED"
    trajectory = service.get_task_trajectory("q", "flaky")
    assert trajectory["task"]["attempt_count"] == 2
    assert [attempt["outcome"] for attempt in trajectory["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert [item["state"] for item in trajectory["intents"]] == ["CLOSED", "CLOSED"]
    assert validate_round(settings.db_path, "flaky1", settings.retry_backoff_base_ms)["ok"]
    assert validate_round(settings.db_path, "flaky2", settings.retry_backoff_base_ms)["ok"]


def test_sigkill_recovery_rolls_back_uncommitted_delivery(service, settings, endpoint):
    base_url, _ = endpoint
    service.submit_tasks(
        "crashq",
        [task("crash-task", mode="hang", hang_seconds=30, retries=1, resource=1)],
    )
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(root)
    process = subprocess.Popen(
        [
            sys.executable,
            str(root / "scripts" / "crash_round.py"),
            "--db",
            str(settings.db_path),
            "--intent-dir",
            str(settings.intent_log_dir),
            "--queue",
            "crashq",
            "--round-id",
            "crash-round",
            "--endpoint",
            base_url,
            "--hang-ms",
            "30000",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    observer = Storage(settings.db_path)
    try:
        deadline = time.time() + 10
        ready = False
        while time.time() < deadline:
            snapshot = observer.queue_snapshot("crashq")
            if snapshot["state_counts"]["DISPATCHED"] == 1 and snapshot["open_intents"] == 1:
                ready = True
                break
            if process.poll() is not None:
                break
            time.sleep(0.02)
        assert ready, process.communicate(timeout=1)
        if os.name == "nt":
            process.kill()
        else:
            process.send_signal(signal.SIGKILL)
        process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()

    restarted = DelayQueueService(settings)
    report = restarted.startup()
    assert report["counts"]["open_intents_scanned"] == 1
    assert report["counts"]["rolled_back_to_ready"] == 1
    assert report["counts"]["rounds_marked_interrupted"] == 1
    assert report["graceful_exit_report_available_for_prior_process"] is False
    assert report["actions"][0]["wire_outcome_unknown"] is True
    assert report["actions"][0]["wire_result_persisted"] is False

    trajectory = restarted.get_task_trajectory("crashq", "crash-task")
    assert trajectory["task"]["state"] == "READY"
    assert trajectory["attempts"][0]["outcome"] == "INTERRUPTED"
    assert trajectory["attempts"][0]["duration_ms"] is None
    assert trajectory["attempts"][0]["http_status"] is None
    assert trajectory["intents"][0]["state"] == "CLOSED"
    assert trajectory["intents"][0]["resolution"] == "RECOVERED_ROLLBACK_TO_READY"
    round_row = restarted.storage.get_round("crash-round")
    assert round_row["status"] == "INTERRUPTED"
    assert restarted.get_recovery_report("crashq")["report"]["report_id"] == report["report_id"]
    assert validate_round(
        settings.db_path, "crash-round", settings.retry_backoff_base_ms
    )["ok"]


def test_recovery_report_contains_only_persisted_facts(service, settings):
    report = service.get_recovery_report("never-seen")["report"]
    assert report["report_id"] is None
    assert report["open_intents_scanned"] == 0
    latest = service.get_recovery_report()["report"]
    assert latest["source"] == "STARTUP_RECOVERY"
    assert latest["graceful_exit_report_available_for_prior_process"] is False

