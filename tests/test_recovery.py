from __future__ import annotations

import json
import pathlib
import sqlite3
import subprocess
import sys

import pytest

from gateway_middleware.errors import E_INVALID_TRANSITION
from gateway_middleware.sample_data import generate_small, seed_dataset
from gateway_middleware.service import GatewayService


class ForcedStop(RuntimeError):
    pass


def test_interrupted_intent_is_rolled_back_to_last_complete_version(settings):
    service = GatewayService(settings)
    dataset = generate_small(settings)
    seeded_version = seed_dataset(service, dataset)
    context_holder = {}

    def stop(context):
        context_holder.update(context)
        raise ForcedStop("simulated process termination after durable intent")

    crashing = GatewayService(settings, crash_hook=stop)
    with pytest.raises(ForcedStop):
        crashing.submit_batch(dataset.batch("interrupted-batch"))

    assert context_holder["phase"] == "after_intent_commit"
    with sqlite3.connect(settings.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM intents WHERE status='PENDING'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM batches WHERE status='PENDING'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM window_events").fetchone()[0] == 0

    restarted = GatewayService(settings)
    report = restarted.recover()
    assert report.scanned_intents == 1
    assert report.rolled_back_batches == 1
    assert report.state_version == seeded_version
    assert report.complete_exit_report is False
    assert restarted.snapshot_info()["version"] == seeded_version
    assert restarted.latest_recovery_report().report_id == report.report_id

    with pytest.raises(Exception) as error:
        restarted.get_batch("interrupted-batch")
    assert getattr(error.value, "code", None) == E_INVALID_TRANSITION

    with sqlite3.connect(settings.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM window_events").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM batch_events").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM intents WHERE status='ROLLED_BACK'").fetchone()[0] == 1


def test_real_forced_process_termination_then_recovery(settings, tmp_path: pathlib.Path):
    service = GatewayService(settings)
    dataset = generate_small(settings)
    seeded_version = seed_dataset(service, dataset)
    marker = tmp_path / "crash-marker.json"

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "gateway_middleware.crash_worker",
            "--db",
            str(settings.db_path),
            "--marker",
            str(marker),
            "--batch-id",
            "forced-kill-batch",
        ],
        cwd=str(pathlib.Path(__file__).resolve().parents[1]),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 91, completed.stderr
    context = json.loads(marker.read_text(encoding="utf-8"))
    assert context["forced_termination"] is True
    assert context["base_version"] == seeded_version

    restarted = GatewayService(settings)
    report = restarted.recover()
    assert report.scanned_intents == 1
    assert report.rolled_back_batches == 1
    assert report.details["state_version_unchanged"] == seeded_version
    assert restarted.snapshot_info()["version"] == seeded_version
    persisted = restarted.latest_recovery_report()
    assert persisted.report_id == report.report_id
    assert persisted.complete_exit_report is False
