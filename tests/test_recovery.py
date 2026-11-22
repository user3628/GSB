from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

from svc_registry.db import Database
from svc_registry.recovery import RecoveryManager
from svc_registry.service import RegistryService


ROOT = Path(__file__).resolve().parent.parent


def _run_child(script: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )


@pytest.mark.skipif(os.name == "nt", reason="SIGKILL semantics are verified on POSIX CI")
def test_probe_round_forced_kill_rolls_back_uncommitted_attempts() -> None:
    path = tempfile.mktemp(suffix=".db")
    child = _run_child(
        f"""
        import asyncio, os
        from svc_registry.models import NormalizedRegistration
        from svc_registry.scheduler import ProbeScheduler
        from svc_registry.service import RegistryService
        service = RegistryService({path!r})
        record = NormalizedRegistration(
            'svc', 'i', 'http://127.0.0.1:28765/health/ok', 500, 30000,
            'http', {{'timeout_ms': 500, 'expected_status': 200, 'expected_body_contains': 'OK'}},
            1700000000000, 1, 400, 1700000000000,
        )
        asyncio.run(service.register(record))
        os.environ['REGISTRY_FAULT_POINT'] = 'probe_after_attempts'
        scheduler = ProbeScheduler(service.db, max_concurrency=1)
        asyncio.run(scheduler.run_round(budget_ms=3000))
        """
    )
    assert child.returncode != 0, child.stderr
    assert child.returncode in (-9, 137)

    database = Database(path)
    database.initialize()
    before = database.get_instance("svc", "i")
    assert before is not None
    assert before.health_status == "UNKNOWN"
    report = RecoveryManager(database).run()
    assert report["facts_only"] is True
    assert report["scanned_intents"] == 1
    assert report["probe"]["instance_health_changes"] == 0
    assert len(report["probe"]["rolled_back_attempts"]) == 1
    assert report["imports"]["instance_records_changed"] == 0
    after = database.get_instance("svc", "i")
    assert after.health_status == before.health_status
    assert after.record_version == before.record_version
    with database.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM probe_history").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM intent_log WHERE status='STARTED'").fetchone()[0] == 0
        attempt = connection.execute("SELECT state, result_json FROM probe_attempts").fetchone()
        assert attempt["state"] == "ROLLED_BACK"
        assert json.loads(attempt["result_json"])["next_state"] == "PENDING_FOR_RETRY"


@pytest.mark.skipif(os.name == "nt", reason="SIGKILL semantics are verified on POSIX CI")
def test_import_forced_kill_aborts_staging_and_preserves_old_state() -> None:
    path = tempfile.mktemp(suffix=".db")
    child = _run_child(
        f"""
        import asyncio, os
        from pathlib import Path
        from svc_registry.importer import ImportInput, SnapshotImporter
        from svc_registry.policies import TimePolicy, WeightPolicy
        from svc_registry.service import RegistryService
        service = RegistryService({path!r})
        importer = SnapshotImporter(service.db)
        os.environ['REGISTRY_FAULT_POINT'] = 'import_after_stage'
        data = Path('samples/legacy_v1.jsonl').read_bytes()
        asyncio.run(importer.import_snapshot(ImportInput(
            batch_id='crash-import',
            data=data,
            weight_policy=WeightPolicy(source_scale='ratio', rounding_rule='half_up', declared=True),
            time_policy=TimePolicy(truncation_rule='floor', declared=True),
        )))
        """
    )
    assert child.returncode != 0, child.stderr
    assert child.returncode in (-9, 137)

    database = Database(path)
    database.initialize()
    assert database.get_instance("catalog", "catalog-1") is None
    report = RecoveryManager(database).run()
    assert report["scanned_intents"] == 1
    assert report["imports"]["instance_records_changed"] == 0
    assert len(report["imports"]["aborted_batches"]) == 1
    assert report["imports"]["aborted_batches"][0]["batch_id"] == "crash-import"
    with database.read() as connection:
        batch = connection.execute(
            "SELECT status, report_json FROM import_batches WHERE batch_id='crash-import'"
        ).fetchone()
        assert batch["status"] == "ABORTED"
        assert json.loads(batch["report_json"])["atomic_rollback"] is True
        assert connection.execute("SELECT COUNT(*) FROM intent_log WHERE status='STARTED'").fetchone()[0] == 0
    assert database.get_instance("catalog", "catalog-1") is None
