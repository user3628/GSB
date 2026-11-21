from __future__ import annotations

from fastapi.testclient import TestClient

from config_coordinator.api import create_app
from config_coordinator.codec import compute_snapshot_checksum


def test_unified_api_error_body_for_checksum_and_stale(tmp_path) -> None:
    app = create_app(str(tmp_path / "api.db"), recover_on_start=True)
    with TestClient(app) as client:
        bad = {
            "snapshot": {
                "version": 1,
                "generated_at": "2026-01-01T00:00:00Z",
                "configs": {"a": 1},
                "checksum": "sha256:broken",
            },
            "components": [],
        }
        response = client.post("/snapshots", json=bad)
        assert response.status_code == 422
        assert response.json()["code"] == "E_SNAPSHOT_INVALID"
        assert set(response.json()) == {"code", "message", "details"}

        configs = {"a": 1}
        checksum = compute_snapshot_checksum(1, "2026-01-01T00:00:00Z", configs)
        good = {
            "snapshot": {
                "version": 1,
                "generated_at": "2026-01-01T00:00:00Z",
                "configs": configs,
                "checksum": checksum,
            },
            "components": [],
        }
        assert client.post("/snapshots", json=good).status_code == 200
        response = client.post("/snapshots", json=good)
        assert response.status_code == 409
        assert response.json()["code"] == "E_STALE_SNAPSHOT"
        assert response.json()["details"]["current_version"] == 1

        increment = {"base_version": 0, "config_set": {"a": 2}}
        response = client.post("/increments", json=increment)
        assert response.status_code == 409
        assert response.json()["code"] == "E_STALE_BASE"

