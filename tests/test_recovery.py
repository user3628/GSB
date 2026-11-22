from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

from app.config import Settings
from app.engine import RuntimeEngine
from app.errors import E_RECOVERED_INTERRUPTED
from tests.conftest import wait_for


def _read_call(db_path: Path, call_id: str):
    connection = sqlite3.connect(db_path, timeout=2)
    connection.row_factory = sqlite3.Row
    try:
        try:
            row = connection.execute(
                "SELECT * FROM calls WHERE call_id = ?", (call_id,)
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        return dict(row) if row else None
    finally:
        connection.close()


@pytest.mark.parametrize("scenario", ["execute", "create"])
def test_force_kill_recovery_uses_persisted_facts(
    tmp_path: Path, endpoint_server, scenario: str
):
    db_path = tmp_path / f"crash-{scenario}.sqlite3"
    spec_path = tmp_path / f"spec-{scenario}.json"
    function_id = f"crash-{scenario}"
    spec_path.write_text(
        json.dumps(
            {
                "functions": [
                    {
                        "function_id": function_id,
                        "runtime": "python3.11",
                        "init_us": 10_000,
                        "memory_bytes": 16_777_216,
                        "per_instance_concurrency": 1,
                        "function_concurrency_limit": 2,
                        "reserved_quota": 1,
                        "init_concurrency": 1,
                        "prewarm_instances": 0,
                        "queue_capacity": 2,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    call_id = f"killed-{scenario}"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tools.crash_worker",
            "--db",
            str(db_path),
            "--endpoint",
            endpoint_server.base_url,
            "--specs",
            str(spec_path),
            "--scenario",
            scenario,
            "--call-id",
            call_id,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        cwd=Path(__file__).resolve().parent.parent,
    )
    try:
        if scenario == "execute":
            wait_for(
                lambda: (_read_call(db_path, call_id) or {}).get("status") == "EXECUTING",
                timeout=8.0,
            )
        else:
            wait_for(
                lambda: (_read_call(db_path, call_id) or {}).get("status") == "CREATING",
                timeout=8.0,
            )
        process.kill()
        process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert process.returncode != 0

    settings = Settings(
        db_path=db_path,
        log_path=tmp_path / "recovery.log",
        endpoint_base=endpoint_server.base_url,
        start_timeout_s=0.2,
        exec_timeout_s=0.2,
        max_queue_wait_s=2.0,
    )
    recovered = RuntimeEngine(settings)
    report = recovered.latest_recovery_report()
    assert report["mode"] == "STARTUP_RECOVERY"
    assert report["persisted_facts_only"] is True
    assert report["exit_report_available"] is False
    assert report["pending_intents_found"] >= 1
    assert any(call["call_id"] == call_id for call in report["recovered_calls"])
    assert report["terminated_instances"]
    released = next(
        row for row in report["quota_released"] if row["function_id"] == function_id
    )
    assert released["on_demand_released"] == 1
    assert released["on_demand_used_after"] == 0
    assert released["reserved_used_after"] == 0
    call = recovered.get_call(call_id)
    assert call["business_code"] == E_RECOVERED_INTERRUPTED
    assert call["status"] == "FAILED"
    assert recovered.quota(function_id)[0]["on_demand_used"] == 0
    assert recovered.audit_invariants()["ok"] is True
    assert all(
        instance["occupied_slots"] == 0
        for instance in recovered.list_instances(function_id)
    )

    second_restart = RuntimeEngine(settings)
    second_report = second_restart.latest_recovery_report()
    assert second_report["recovered_calls"] == []
    assert second_report["pending_intents_found"] == 0

