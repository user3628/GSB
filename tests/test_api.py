from __future__ import annotations

import copy
import json
from pathlib import Path

from slowquery.api import create_app
from slowquery.config import WorkbenchConfig


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _fixture(name: str):
    return json.loads((DATA_DIR / name).read_text())


def test_http_api_end_to_end_and_error_contract(config: WorkbenchConfig) -> None:
    app = create_app(config)
    client = app.test_client()

    response = client.post("/api/samples", json=_fixture("samples.json"))
    assert response.status_code == 201
    assert response.get_json()["inserted"] == 7

    response = client.post("/api/snapshots", json=_fixture("snapshot_v1.json"))
    assert response.status_code == 201
    assert response.get_json()["active_version"] == 1

    response = client.post(
        "/api/sessions", json={"session_id": "http-1", "sample_id": "sq-missing-index"}
    )
    assert response.status_code == 201
    assert response.get_json()["session"]["status"] == "CREATED"

    response = client.post("/api/sessions/http-1/diagnose", json={})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["session"]["status"] == "SUGGESTED"
    assert payload["session"]["suggestions"][0]["snapshot_version"] == 1

    response = client.get("/api/sessions/http-1/suggestions")
    assert response.status_code == 200
    suggestion = response.get_json()["suggestions"][0]
    assert suggestion["rule_id"] == "R_ADD_INDEX_ORDERS_CUSTOMER_ID_STATUS"

    response = client.post("/api/sessions/http-1/verify", json={})
    assert response.status_code == 200
    verification = response.get_json()["verification"]
    assert verification["outcome"] == "VERIFIED"
    assert verification["original_result_digest"] == verification["rewritten_result_digest"]
    assert all(verification["detail"]["resources"].values())

    response = client.post("/api/sessions/http-1/diagnose", json={})
    assert response.status_code == 200
    assert response.get_json()["reused"] is True


def test_http_error_codes_are_json_and_state_safe(config: WorkbenchConfig) -> None:
    app = create_app(config)
    client = app.test_client()
    client.post("/api/samples", json=_fixture("samples.json"))
    client.post("/api/snapshots", json=_fixture("snapshot_v1.json"))

    bad_sample = {
        "sample_id": "http-bad",
        "sql": "SELECT 1",
        "duration_ms": -1,
        "scanned_rows": 1,
        "lock_wait_ms": 0,
        "occurred_at": "2026-01-01T00:00:00Z",
    }
    response = client.post("/api/samples", json=[bad_sample])
    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "E_SAMPLE_INVALID"
    assert app.extensions["slowquery"].repo.get_sample("http-bad") is None

    invalid_snapshot = copy.deepcopy(_fixture("snapshot_v2.json"))
    invalid_snapshot["summary"]["index_count"] = 999
    response = client.post("/api/snapshots", json=invalid_snapshot)
    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "E_SNAPSHOT_INVALID"

    response = client.get("/api/sessions/missing")
    assert response.status_code == 404
    assert response.get_json()["error"]["code"] == "E_SESSION_NOT_FOUND"

    client.post("/api/sessions", json={"session_id": "early", "sample_id": "sq-missing-index"})
    response = client.post("/api/sessions/early/verify", json={})
    assert response.status_code == 409
    assert response.get_json()["error"]["code"] == "E_INVALID_TRANSITION"
    assert app.extensions["slowquery"].get_session("early")["status"] == "CREATED"

    content_type = client.post("/api/samples", data="not-json")
    assert content_type.status_code == 400
    assert content_type.get_json()["error"]["code"] == "E_REQUEST_INVALID"


def test_http_snapshot_retirement_and_verify_mismatch(config: WorkbenchConfig) -> None:
    app = create_app(config)
    client = app.test_client()
    client.post("/api/samples", json=_fixture("samples.json"))
    client.post("/api/snapshots", json=_fixture("snapshot_v1.json"))
    client.post("/api/sessions", json={"session_id": "old", "sample_id": "sq-many-rows"})
    client.post("/api/snapshots", json=_fixture("snapshot_v2.json"))

    response = client.post("/api/sessions/old/diagnose", json={"snapshot_version": 1})
    assert response.status_code == 409
    assert response.get_json()["error"]["code"] == "E_SNAPSHOT_RETIRED"
    assert app.extensions["slowquery"].get_session("old")["status"] == "CREATED"

    client.post("/api/sessions", json={"session_id": "mismatch", "sample_id": "sq-full-scan"})
    response = client.post("/api/sessions/mismatch/diagnose", json={})
    assert response.status_code == 200
    response = client.post("/api/sessions/mismatch/verify", json={})
    assert response.status_code == 422
    error = response.get_json()["error"]
    assert error["code"] == "E_VERIFY_MISMATCH"
    assert error["details"]["original_row_count"] != error["details"]["rewritten_row_count"]
    session = client.get("/api/sessions/mismatch").get_json()
    assert session["status"] == "FAILED"
    assert session["verifications"][0]["outcome"] == "FAILED"


def test_http_recovery_report_is_available(config: WorkbenchConfig) -> None:
    app = create_app(config)
    client = app.test_client()
    response = client.get("/api/recovery/report")
    assert response.status_code == 200
    report = response.get_json()
    assert report["payload"]["facts_only"] is True
    assert "facts" in report["payload"]
    assert "guarantees" in report["payload"]
