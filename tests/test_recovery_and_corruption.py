from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from capacity_ledger.errors import CapacityError
from capacity_ledger.service import CapacityLedger

from conftest import ROOT, load_json


def prepare_v3(ledger: CapacityLedger, sample_root: Path) -> dict:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    ledger.commit_increment(load_json(sample_root / "increments" / "01_auth_price_increment.json"))
    ledger.commit_increment(load_json(sample_root / "increments" / "02_catalog_cores_increment.json"))
    return ledger.get_current()


def test_forced_termination_recovery_restores_last_complete_version(
    sample_root: Path, tmp_path: Path
) -> None:
    db_path = tmp_path / "ledger.sqlite3"
    intent_dir = tmp_path / "intents"
    ledger = CapacityLedger(db_path, intent_dir)
    before = prepare_v3(ledger, sample_root)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    crash = subprocess.run(
        [
            sys.executable,
            "-m",
            "capacity_ledger.crash_worker",
            "--db",
            str(db_path),
            "--intent-dir",
            str(intent_dir),
            "--increment",
            str(sample_root / "increments" / "04_cart_force_terminate.json"),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert crash.returncode == 86, crash.stdout + crash.stderr
    restarted = CapacityLedger(db_path, intent_dir)
    report = restarted.recover()
    assert report["report_basis"] == "PERSISTED_FACTS_ONLY_AFTER_RESTART"
    assert report["forced_termination_exit_report_available"] is False
    assert report["aborted_count"] == 1
    assert report["aborted_intents"][0]["failure_code"] == "E_PROCESS_TERMINATED"
    assert report["before"] == report["after"]
    assert report["difference"]["state_unchanged"] is True
    current = restarted.get_current()
    assert current["version"] == 3
    assert current["ledger_checksum"] == before["ledger_checksum"]
    assert restarted.verify()["status"] == "VALID"


def test_ledger_corruption_is_detected(sample_root: Path, tmp_path: Path) -> None:
    db_path = tmp_path / "ledger.sqlite3"
    ledger = CapacityLedger(db_path, tmp_path / "intents")
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    with sqlite3.connect(db_path) as connection:
        metrics_json = connection.execute(
            "SELECT metrics_json FROM ledger_versions WHERE version = 1"
        ).fetchone()[0]
        tampered = metrics_json.replace('"total_cost_cents_per_hour":1866', '"total_cost_cents_per_hour":1')
        assert tampered != metrics_json
        connection.execute(
            "UPDATE ledger_versions SET metrics_json = ? WHERE version = 1", (tampered,)
        )
        connection.commit()
    with pytest.raises(CapacityError) as exc_info:
        ledger.verify()
    assert exc_info.value.code == "E_LEDGER_CORRUPT"
    with pytest.raises(CapacityError) as exc_info:
        ledger.recover()
    assert exc_info.value.code == "E_LEDGER_CORRUPT"


def test_unfinished_intent_with_committed_ledger_is_reconciled(
    sample_root: Path, tmp_path: Path
) -> None:
    ledger = CapacityLedger(tmp_path / "ledger.sqlite3", tmp_path / "intents")
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    with ledger.store.write_transaction() as connection:
        connection.execute(
            """
            INSERT INTO intents (
                intent_id, commit_id, kind, base_version, target_version, state,
                payload_json, created_at, updated_at
            ) VALUES (
                'simulated-desync', 'commit-v1-baseline', 'snapshot', 0, 1,
                'PREPARED', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
            )
            """
        )
    report = ledger.recover()
    assert report["reconciled_committed"] == ["simulated-desync"]
    assert report["aborted_count"] == 0
    intents = ledger.list_intents()
    assert intents[0]["state"] == "COMMITTED"

