from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.api import create_app
from app.service import TieringService

from .conftest import SAMPLES


def test_http_flow_and_error_shapes(tmp_path: Path):
    db = tmp_path / "api.db"
    service = TieringService(db, samples_dir=SAMPLES)
    app = create_app(service=service)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        occupancy = client.get("/v1/layers/occupancy")
        assert occupancy.status_code == 200
        snapshot_version = occupancy.json()["snapshot_version"]

        preview = client.post("/v1/batches/preview", json={"snapshot_version": snapshot_version})
        assert preview.status_code == 200
        assert preview.json()["selected_count"] == 9

        batch = client.post(
            "/v1/batches",
            json={"batch_id": "api-batch", "snapshot_version": snapshot_version},
        )
        assert batch.status_code == 200
        assert batch.json()["state"] == "COMMITTED"

        illegal = client.post(
            "/v1/state-machine/validate",
            json={"from_state": "COMMITTED", "to_state": "PENDING"},
        )
        assert illegal.status_code == 409
        assert illegal.json()["error"]["code"] == "E_INVALID_TRANSITION"
        assert illegal.json()["error"]["context"]["persisted_state_changed"] is False

        stale = client.post("/v1/batches", json={"batch_id": "stale-batch", "snapshot_version": 99})
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "E_STALE_SNAPSHOT"

        report = client.get("/v1/recovery/report")
        assert report.status_code == 200
        assert report.json()["persisted_only"] is True

        safe = client.post("/v1/admin/safe-shutdown", json={"reason": "api_test"})
        assert safe.status_code == 200
        assert safe.json()["report_type"] == "SAFE_SHUTDOWN"


def test_invalid_request_has_business_error(tmp_path: Path):
    service = TieringService(tmp_path / "api2.db", samples_dir=SAMPLES)
    with TestClient(create_app(service=service)) as client:
        response = client.post("/v1/batches", json={"batch_id": "bad id with spaces"})
        assert response.status_code == 422
        body = response.json()
        assert body["error"]["code"] == "E_INVALID_REQUEST"
        assert "context" in body["error"]

        missing = client.get("/does-not-exist")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_NOT_FOUND"
