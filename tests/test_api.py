from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from rope_diag.api import create_app
from rope_diag.demo_data import generate_demo_assets
from rope_diag.session import DiagnosticSession


def _client(tmp_path: Path, assets: dict) -> TestClient:
    workspace = tmp_path / "api_ws"
    DiagnosticSession.create(
        workspace, assets["manifest"], assets["config"], reset=True
    )
    return TestClient(create_app(workspace))


def test_http_health_status_query_and_export(tmp_path: Path, assets: dict) -> None:
    client = _client(tmp_path, assets)
    health = client.get("/api/health")
    assert health.status_code == 200
    assert health.json()["data"]["session_loaded"] is True
    assert b"RoPE Diagnostics" in client.get("/").content

    run = client.post("/api/run", json={})
    assert run.status_code == 200
    assert run.json()["data"]["status"] == "completed"

    query = client.post(
        "/api/query",
        json={"shard_id": "short", "query_type": "angles", "options": {"position": 2}},
    )
    assert query.status_code == 200
    payload = query.json()["data"]
    assert payload["payload"]["rows"][0]["position"] == 2
    assert payload["cache_components"]["angle_table_fingerprint"]

    export = client.post("/api/export", json={})
    assert export.status_code == 200
    download_url = export.json()["data"]["download_url"]
    download = client.get(download_url)
    assert download.status_code == 200
    assert download.content[:2] == b"PK"


def test_http_latency_and_invalid_config_are_structured(tmp_path: Path, assets: dict) -> None:
    client = _client(tmp_path, assets)
    client.post("/api/run", json={})
    latency = client.post(
        "/api/query",
        json={
            "shard_id": "short",
            "query_type": "score_summary",
            "latency_budget_ms": 0,
            "bypass_cache": True,
        },
    )
    assert latency.status_code == 408
    error = latency.json()["error"]
    assert error["code"] == "E_LATENCY_BUDGET"
    assert error["details"]["query_id"].startswith("qry-")
    assert "elapsed_ms" in error["details"]

    invalid = client.post(
        "/api/config/validate",
        json={"base_frequency": 10000, "scaling_factor": 0.0},
    )
    assert invalid.status_code == 400
    assert invalid.json()["error"]["code"] == "E_INVALID_CONFIG"


def test_http_incremental_config_and_stop_resume(tmp_path: Path, assets: dict) -> None:
    client = _client(tmp_path, assets)
    client.post("/api/run", json={})
    changed = client.post(
        "/api/config",
        json={"patch": {"rotation_block_size": 4}, "run": True, "verify": True},
    )
    assert changed.status_code == 200
    data = changed.json()["data"]
    assert data["recompute_report"]["earliest_affected_stage"] == "block_rotation"
    assert data["consistency"]["all_byte_identical"] is True

    stop_workspace = tmp_path / "api_stop_ws"
    DiagnosticSession.create(
        stop_workspace, assets["manifest"], assets["config"], reset=True
    )
    stop_client = TestClient(create_app(stop_workspace))
    stopped_run = stop_client.post(
        "/api/run", json={"stop_after_stage": "frequency_generation"}
    )
    assert stopped_run.status_code == 200
    assert stopped_run.json()["data"]["status"] == "stopped"
    stop = stop_client.post("/api/stop", json={"reason": "test"})
    assert stop.status_code == 200
    resume = stop_client.post("/api/resume")
    assert resume.status_code == 200
    assert resume.json()["data"]["status"] == "completed"
