from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from checkpoint_recovery import CheckpointManager, ManagerConfig
from checkpoint_recovery.api import create_app


def test_http_contract_errors_busy_and_recovery(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(data_dir=tmp_path / "data", max_writing=1, writer_delay_seconds=0.35)
    manager = CheckpointManager(config)
    app = create_app(manager=manager, config=config)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200

        first_payload = payload_factory(1)
        first = client.post("/checkpoints", json=first_payload)
        assert first.status_code == 202
        assert first.json()["status"] == "WRITING"
        checkpoint_id = first.json()["checkpoint_id"]
        immediate = client.get(f"/checkpoints/{checkpoint_id}")
        assert immediate.json()["checkpoint"]["status"] == "WRITING"

        busy = client.post("/checkpoints", json=payload_factory(2))
        assert busy.status_code == 429
        assert busy.json()["code"] == "E_WRITER_BUSY"

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            current = client.get(f"/checkpoints/{checkpoint_id}").json()["checkpoint"]
            if current["status"] == "COMMITTED":
                break
            time.sleep(0.05)
        assert current["status"] == "COMMITTED"

        bad = payload_factory(3)
        bad["optimizer"][0]["sha256"] = "f" * 64
        checksum = client.post("/checkpoints", json=bad, params={"wait": "true"})
        assert checksum.status_code == 409
        assert checksum.json()["code"] == "E_CHECKSUM_MISMATCH"

        invalid_transition = client.post(
            f"/checkpoints/{checkpoint_id}/transition",
            json={"target_state": "PRUNED", "reason": "not_allowed_via_manual_endpoint"},
        )
        assert invalid_transition.status_code == 409
        assert invalid_transition.json()["code"] == "E_INVALID_TRANSITION"

        missing = client.get("/checkpoints/ckpt-does-not-exist")
        assert missing.status_code == 404
        assert missing.json()["code"] == "E_CHECKPOINT_NOT_FOUND"

        prune_conflict = client.post("/prune", json={"checkpoint_ids": [checkpoint_id]})
        assert prune_conflict.status_code == 409
        assert prune_conflict.json()["code"] == "E_PRUNE_CONFLICT"

        baseline = client.get("/recovery/baseline")
        assert baseline.status_code == 200
        assert baseline.json()["checkpoint"]["status"] == "COMMITTED"
        report = client.get("/recovery/report", params={"target_round": 3})
        assert report.status_code == 200
        assert report.json()["report"]["difference_report"]["rounds_to_replay"] == 2


def test_http_idempotent_wait_reuses_existing_result(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(data_dir=tmp_path / "data", max_writing=1, writer_delay_seconds=0.05)
    manager = CheckpointManager(config)
    with TestClient(create_app(manager=manager, config=config)) as client:
        payload = payload_factory(1, request_id="http-idempotent")
        first = client.post("/checkpoints", json=payload, params={"wait": "true"})
        second = client.post("/checkpoints", json=payload, params={"wait": "true"})
        assert first.status_code == second.status_code == 200
        assert first.json()["checkpoint_id"] == second.json()["checkpoint_id"]
        assert second.json()["reused"] is True
        assert client.get("/checkpoints").json()["count"] == 1

