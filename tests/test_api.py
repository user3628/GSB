from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from vgw.app import AppSettings, create_app

from .conftest import make_snapshot, make_topology


def test_http_surface_and_error_envelope(tmp_path: Path) -> None:
    topology = make_topology()
    topology_path = tmp_path / "topology.json"
    topology_path.write_text(
        json.dumps(topology.as_dict(), ensure_ascii=False), encoding="utf-8"
    )
    settings = AppSettings(
        db_path=tmp_path / "api.db",
        topology_path=topology_path,
        auto_load_sample=False,
    )
    app = create_app(settings)
    snapshot = make_snapshot(topology)
    with TestClient(app) as client:
        health = client.get("/healthz")
        assert health.status_code == 200
        assert health.json()["ok"] is True
        assert health.json()["snapshot_version"] is None

        loaded = client.post("/api/v1/snapshots", json=snapshot)
        assert loaded.status_code == 200, loaded.text
        assert loaded.json()["result"]["version"] == 1

        accepted = client.post(
            "/api/v1/frames",
            json={
                "frame_id": "api-alpha-1",
                "session_id": "alpha",
                "sequence": 1,
                "payload": {"op": "echo", "body": "api"},
            },
        )
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["result"]["response"]["endpoint"] == "decap-east"

        gap = client.post(
            "/api/v1/frames",
            json={
                "frame_id": "api-alpha-3",
                "session_id": "alpha",
                "sequence": 3,
                "payload": {"op": "echo"},
            },
        )
        assert gap.status_code == 400
        assert gap.json()["error"]["code"] == "E_FRAME_INVALID"
        assert gap.json()["error"]["context"]["expected_sequence"] == 2

        conflicting = client.post(
            "/api/v1/frames",
            json={
                "frame_id": "api-alpha-1",
                "session_id": "alpha",
                "sequence": 1,
                "payload": {"different": True},
            },
        )
        assert conflicting.status_code == 409
        assert conflicting.json()["error"]["code"] == "E_CONFLICTING_FRAME"

        overview = client.get("/api/v1/overview").json()
        assert overview["state"]["version"] == 1
        assert overview["state"]["sessions"]["alpha"]["last_acked_seq"] == 1
        assert overview["network"]["carriers"]

        traces = client.get("/api/v1/traces").json()["traces"]
        assert any(item["kind"] == "frame_acked" for item in traces)

        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "Loopback Tunnel Gateway" in dashboard.text

