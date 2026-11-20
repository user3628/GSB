from __future__ import annotations

from pathlib import Path

from xrel_workbench.server import create_app
from xrel_workbench.workspace import initialize_workspace


def test_http_status_reports_and_edit(workspace) -> None:
    client = create_app(workspace.root).test_client()
    health = client.get("/api/health")
    assert health.status_code == 200
    assert health.get_json()["ok"] is True
    status = client.get("/api/status")
    assert status.status_code == 200
    assert status.get_json()["data"]["run"]["status"] == "complete"
    grades = client.get("/api/grades")
    assert grades.status_code == 200
    assert grades.get_json()["count"] == 24
    errors = client.get("/api/errors")
    assert errors.status_code == 200
    assert errors.get_json()["data"]["pair_count"] == 24
    export = client.get("/api/export/scores")
    assert export.status_code == 200
    assert export.data.count(b"\n") == 24

    edited = client.post(
        "/api/edit/bridge",
        json={"entry_id": "br-003", "weight": 0.71, "recalculate": True, "verify": True},
    )
    assert edited.status_code == 200
    payload = edited.get_json()["data"]
    assert payload["build"]["verification"]["match"] is True
    assert payload["build"]["invalidation_plan"]["summary"][0]["earliest_affected_stage"] == "stage2"

    stop = client.post("/api/stop", json={"reason": "test"})
    assert stop.status_code == 200
    resume = client.post("/api/resume", json={"verify": True})
    assert resume.status_code == 200
    assert resume.get_json()["data"]["verification"]["match"] is True


def test_http_numeric_error_contains_pair_term_stage(
    tmp_path: Path,
    generated_data: Path,
) -> None:
    workspace = initialize_workspace(generated_data, tmp_path / "numeric-http", force=True)
    client = create_app(workspace.root).test_client()
    # First build to establish upstream checkpoints, then inject a negative bridge weight.
    first = client.post("/api/run", json={"full": True, "verify": False})
    assert first.status_code == 200
    response = client.post(
        "/api/edit/bridge",
        json={"entry_id": "br-003", "weight": -1.0, "recalculate": True, "verify": False},
    )
    assert response.status_code == 422
    error = response.get_json()["error"]
    assert error["code"] == "E_NUMERIC"
    assert error["details"]["stage"] == "stage3_log_domain_score"
    assert error["details"]["pair_id"]
    assert error["details"]["term"]
