from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from replication_guard.api import create_app
from replication_guard.errors import AppError
from replication_guard.formats import encode_binary
from replication_guard.models import normalize_policy
from replication_guard.service import ReplicationService


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def test_api_unified_errors_and_required_codes(tmp_path: Path) -> None:
    app = create_app(tmp_path / "api.sqlite3")
    with TestClient(app) as client:
        invalid = client.post(
            "/api/policies",
            json={
                "slave_id": "bad",
                "replication_group": "g",
                "delay_threshold_ms": 10,
                "tolerable_delay_ms": 9,
                "expected_version": "8.0.1",
            },
        )
        assert invalid.status_code == 422
        assert invalid.json()["code"] == "E_POLICY_INVALID"

        malformed = client.post(
            "/api/policies",
            json={
                "slave_id": "bad",
                "replication_group": "g",
                "delay_threshold_ms": 10,
                "tolerable_delay_ms": 20,
                "expected_version": "eight",
            },
        )
        assert malformed.status_code == 422
        assert malformed.json()["code"] == "E_VERSION_MALFORMED"

        future = client.post(
            "/api/policies/import?format=jsonl&batch_id=api-future",
            content=(SAMPLES / "policies_future_v2.jsonl").read_bytes(),
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert future.status_code == 409
        assert future.json()["code"] == "E_FORMAT_INCOMPATIBLE"


def test_api_roundtrip_recalc_edit_replay_and_stale_base(tmp_path: Path) -> None:
    app = create_app(tmp_path / "api2.sqlite3")
    with TestClient(app) as client:
        imported = client.post(
            "/api/policies/import?format=jsonl&batch_id=api-import",
            content=(SAMPLES / "policies.jsonl").read_bytes(),
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert imported.status_code == 200
        assert imported.json()["negotiation"]["decision"] == "ACCEPT"
        dataset_version = imported.json()["datasetVersion"]

        duplicate = client.post(
            "/api/policies/import?format=jsonl&batch_id=api-import",
            content=(SAMPLES / "policies.jsonl").read_bytes(),
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert duplicate.json()["deduplicated"] is True

        exported = client.get("/api/policies/export?format=binary&version=2")
        assert exported.status_code == 200
        assert exported.content.startswith(b"RGPB")

        observations = json.loads((SAMPLES / "observations.json").read_text("utf-8"))
        assert client.post("/api/observations", json=observations).status_code == 200
        recalculated = client.post(
            "/api/recalculate",
            json={
                "batchId": "api-recalc",
                "observationBatchId": observations["batchId"],
                "windowSize": 2,
            },
        )
        assert recalculated.status_code == 200
        body = recalculated.json()
        assert body["businessCode"] == "E_VERSION_AHEAD"
        recalc_id = body["recalcId"]

        replayed = client.post(f"/api/verdicts/{recalc_id}/replay")
        assert replayed.json()["deterministicMatch"] is True

        edited = client.patch(
            f"/api/verdicts/{recalc_id}/rows/replica-east-01",
            json={"effectiveAction": "FAILOVER", "note": "operator override"},
        )
        assert edited.status_code == 200
        fetched = client.get(f"/api/verdicts/{recalc_id}").json()
        east = next(row for row in fetched["result"]["results"] if row["slaveId"] == "replica-east-01")
        assert east["effectiveAction"] == "FAILOVER"
        assert east["computedEffectiveAction"] == "THROTTLE_WRITES"

        current = client.get("/api/policies/replica-east-01").json()["policy"]
        stale = client.put(
            "/api/policies/replica-east-01",
            json={"policy": current, "baseDatasetVersion": dataset_version - 1},
        )
        assert stale.status_code == 409
        assert stale.json()["code"] == "E_STALE_BASE"

        invalid = client.post(
            "/api/states/replica-north-03/transition",
            json={"targetState": "HEALTHY"},
        )
        assert invalid.status_code == 409
        assert invalid.json()["code"] == "E_INVALID_TRANSITION"


def test_binary_export_reimport_into_empty_service(tmp_path: Path) -> None:
    source = ReplicationService(tmp_path / "source.sqlite3")
    target = ReplicationService(tmp_path / "target.sqlite3")
    source.startup()
    target.startup()
    source.import_policies(
        (SAMPLES / "policies.jsonl").read_bytes(), format_hint="jsonl", batch_id="source-import"
    )
    binary = source.export_policies("binary", version=2)[0]
    target.import_policies(binary, format_hint="binary", batch_id="binary-import")
    assert source.list_policies()["policies"] == target.list_policies()["policies"]


def test_forced_termination_recovery_marks_batch_aborted(tmp_path: Path) -> None:
    db_path = tmp_path / "crash.sqlite3"
    baseline = ReplicationService(db_path)
    baseline.startup()
    baseline.import_policies(
        (SAMPLES / "policies.jsonl").read_bytes(), format_hint="jsonl", batch_id="baseline"
    )
    before = baseline.list_policies()

    script = (
        "from pathlib import Path;"
        "from replication_guard.service import ReplicationService;"
        f"svc=ReplicationService(Path(r'{db_path}'));"
        "svc.startup();"
        f"svc.import_policies(Path(r'{SAMPLES / 'policies.jsonl'}').read_bytes(),"
        "format_hint='jsonl',batch_id='crash-batch')"
    )
    env = os.environ.copy()
    env["RG_DB_PATH"] = str(db_path)
    env["RG_CRASH_POINT"] = "before_commit"
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=env,
        capture_output=True,
        timeout=20,
    )
    assert completed.returncode == 92

    recovered = ReplicationService(db_path)
    recovered.startup()
    after = recovered.list_policies()
    assert after == before
    report = recovered.recovery_report()
    assert "crash-batch" in report["abortedBatches"]
    assert report["scope"] == "PERSISTED_FACTS_ONLY"
    assert report["forcedTerminationExitReportAvailable"] is False
    assert report["integrity"]["currentDatasetVersion"] == 2
    with pytest.raises(AppError) as captured:
        recovered.import_policies(
            (SAMPLES / "policies.jsonl").read_bytes(), format_hint="jsonl", batch_id="crash-batch"
        )
    assert captured.value.code == "E_BATCH_ABORTED"
    assert captured.value.details["deduplicated"] is True


def test_recovery_restores_last_complete_snapshot_on_policy_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "integrity.sqlite3"
    baseline = ReplicationService(db_path)
    baseline.startup()
    imported = baseline.import_policies(
        (SAMPLES / "policies.jsonl").read_bytes(), format_hint="jsonl", batch_id="integrity-base"
    )
    original = baseline.get_policy("replica-east-01")["policy"]
    assert imported["datasetVersion"] == 2

    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "UPDATE policies SET delay_threshold_ms = 99999 WHERE slave_id = 'replica-east-01'"
        )
        connection.commit()
    finally:
        connection.close()

    recovered = ReplicationService(db_path)
    recovered.startup()
    restored = recovered.get_policy("replica-east-01")["policy"]
    assert restored["delay_threshold_ms"] == original["delay_threshold_ms"]
    report = recovered.recovery_report()
    assert report["integrity"]["restoredFromSnapshot"] is True
    assert report["integrity"]["currentDatasetVersion"] == 2

