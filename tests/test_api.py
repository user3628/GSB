from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from h2workbench.api import create_app

from .conftest import ROOT


def client(tmp_path: Path) -> TestClient:
    return TestClient(create_app(tmp_path / "api.db"))


def read_sample(name: str) -> dict:
    return json.loads((ROOT / "samples" / name).read_text(encoding="utf-8"))


def test_full_api_flow_and_error_bodies(tmp_path: Path):
    http = client(tmp_path)
    assert http.get("/api/health").json()["status"] == "ok"
    dashboard = http.get("/")
    assert dashboard.status_code == 200
    assert "HTTP/2 多路复用诊断工作台" in dashboard.text

    sample = read_sample("normal_multiplex.json")
    submitted = http.post("/api/samples", json=sample)
    assert submitted.status_code == 200
    assert submitted.json()["queued_data"]["bytes"] == 12_500
    scheduled = http.post("/api/samples/demo-normal-multiplex/schedule", json={})
    assert scheduled.status_code == 200
    assert scheduled.json()["status"] == "COMPLETED"
    assert scheduled.json()["trace"]

    connection = http.get("/api/connections/demo-normal").json()
    assert connection["connection_window"] == 53_035
    stream = http.get("/api/connections/demo-normal/streams/3").json()
    assert stream["stream"]["state"] == "HALF_CLOSED_LOCAL"
    trace = http.get("/api/connections/demo-normal/trace").json()
    assert trace["runs"][0]["trace"] == scheduled.json()["trace"]

    blocked = read_sample("blocked_window.json")
    assert http.post("/api/samples", json=blocked).status_code == 200
    response = http.post("/api/samples/demo-window-blocked/schedule", json={})
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "E_FLOW_CONTROL_BLOCKED"
    assert error["connection_id"] == "demo-blocked"
    assert error["context"]["streams"][0]["required_bytes"] > 0
    blocked_trace = http.get("/api/samples/demo-window-blocked/trace").json()
    assert blocked_trace["status"] == "BLOCKED"
    assert blocked_trace["blocked"]

    missing = http.get("/api/samples/not-there")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "E_SAMPLE_NOT_FOUND"

    conflict = read_sample("normal_multiplex.json")
    conflict["frames"][-1]["payload_bytes"] += 1
    conflict_response = http.post("/api/samples", json=conflict)
    assert conflict_response.status_code == 409
    assert conflict_response.json()["error"]["code"] == "E_CONFLICTING_SAMPLE"

    invalid = {"sample_id": "bad", "connection_id": "bad", "frames": [{"stream_id": 1}]}
    invalid_response = http.post("/api/samples", json=invalid)
    assert invalid_response.status_code == 400
    assert invalid_response.json()["error"]["code"] == "E_FRAME_INVALID"


def test_window_update_close_and_recovery_endpoints(tmp_path: Path):
    http = client(tmp_path)
    sample = read_sample("window_update_cap.json")
    assert http.post("/api/samples", json=sample).status_code == 200
    run = http.post("/api/samples/demo-window-update-cap/schedule", json={})
    assert run.status_code == 200
    conn = http.get("/api/connections/demo-window-cap").json()
    assert conn["protocol_violations"] == 2
    assert conn["connection_window"] == 500
    assert conn["streams"][0]["window"] == 400

    update = http.post(
        "/api/connections/demo-window-cap/window-update",
        json={"stream_id": 0, "increment": 500},
    )
    assert update.status_code == 200
    assert update.json()["connection"]["connection_window"] == 1000

    closed = http.post("/api/connections/demo-window-cap/close")
    assert closed.status_code == 200
    assert closed.json()["closed"] is True
    assert closed.json()["streams"][0]["state"] == "CLOSED"

    recovery = http.get("/api/recovery")
    assert recovery.status_code == 200
    assert recovery.json()["scope"] == "persisted_facts_only"
