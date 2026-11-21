from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from cabin_voice.api import create_app


def test_http_workbench_end_to_end(tmp_path: Path) -> None:
    app = create_app(tmp_path / "http-workspace")
    client = TestClient(app)

    root = client.get("/")
    assert root.status_code == 200
    assert "车内多音区语音仲裁工作台" in root.text

    initialized = client.post("/api/demo/init", json={"seed": 20261122, "reset": True})
    assert initialized.status_code == 200
    assert initialized.json()["shard_count"] if "shard_count" in initialized.json() else len(initialized.json()["shards"]) == 3

    run = client.post("/api/run", json={"stage": "all", "force_full": False, "verify": True})
    assert run.status_code == 200
    run_body = run.json()
    assert run_body["recomputed_stages"] == ["vad", "arbitrate"]
    assert run_body["result_digest"] == run_body["verified_full_digest"]

    status = client.get("/api/status").json()
    assert status["stages"]["vad"]["fresh"] is True
    edit_version = status["edit_version"]

    query = client.get(
        "/api/query/active",
        params={"shard_id": "shard-001", "time_ms": 350, "budget_ms": 500},
    )
    assert query.status_code == 200
    query_body = query.json()
    assert query_body["result"]["active_zones"] == ["driver", "passenger"]
    assert query_body["cache_hit"] is False
    cached = client.get(
        "/api/query/active",
        params={"shard_id": "shard-001", "time_ms": 350, "budget_ms": 500},
    ).json()
    assert cached["cache_hit"] is True
    assert cached["result_digest"] == query_body["result_digest"]

    latency = client.get(
        "/api/query/active",
        params={"shard_id": "shard-001", "time_ms": 350, "budget_ms": 0},
    )
    assert latency.status_code == 504
    latency_body = latency.json()
    assert latency_body["ok"] is False
    assert latency_body["error"]["code"] == "E_LATENCY_BUDGET"
    assert "query_id" in latency_body["error"]["details"]
    assert "elapsed_ms" in latency_body["error"]["details"]

    switched = client.post(
        "/api/edits/direct",
        json={
            "operation": "select_scope",
            "parameters": {},
            "expected_version": edit_version,
            "shard_id": "shard-003",
        },
    )
    assert switched.status_code == 200
    priority = client.post(
        "/api/edits/direct",
        json={
            "operation": "set_priority",
            "parameters": {"zone_id": "rear_right", "priority": 1},
            "expected_version": switched.json()["edit_version"],
            "shard_id": "shard-003",
        },
    )
    assert priority.status_code == 200
    stale = client.post(
        "/api/edits/direct",
        json={
            "operation": "set_priority",
            "parameters": {"zone_id": "rear_left", "priority": 2},
            "expected_version": edit_version,
            "shard_id": "shard-003",
        },
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STALE_VERSION"
    assert stale.json()["error"]["details"]["pending_status"] == "stale"

    incremental = client.post("/api/run", json={"stage": "all", "verify": True}).json()
    assert incremental["reused_stages"] == ["vad"]
    assert incremental["recomputed_stages"] == ["arbitrate"]
    assignment = client.get(
        "/api/query/assignment",
        params={"shard_id": "shard-003", "time_ms": 900, "budget_ms": 500},
    ).json()
    assert assignment["result"]["events"][0]["responsible_zone"] == "rear_right"

    snapshots = client.post("/api/snapshots/verify", json={})
    assert snapshots.status_code == 200
    assert snapshots.json()["status"] == "verified"

    exported = client.post("/api/export", json={"name": "api-result.json"})
    assert exported.status_code == 200
    assert exported.json()["export"]["digest"] == incremental["result_digest"]
