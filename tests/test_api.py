from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import read_json


def test_http_flow_and_queries(tmp_path) -> None:
    app = create_app(tmp_path / "api.db")
    with TestClient(app) as client:
        layout = read_json("sample_layout.json")
        trace = read_json("sample_trace.json")
        assert client.post("/api/v1/layouts", json=layout).status_code == 201
        assert client.get(f"/api/v1/layouts/{layout['layout_version']}").status_code == 200
        assert client.get("/api/v1/layouts/current").status_code == 200
        submitted = client.post("/api/v1/traces", json=trace)
        assert submitted.status_code == 201, submitted.text
        result = submitted.json()
        round_id = result["round_id"]

        decision_response = client.get(f"/api/v1/rounds/{round_id}/decisions")
        assert decision_response.status_code == 200
        assert len(decision_response.json()["decisions"]) == 9
        budget_response = client.get(f"/api/v1/rounds/{round_id}/budget")
        assert budget_response.status_code == 200
        assert budget_response.json()["committed_budget_summary"]["consumed_bytes"] == 2048
        signature = client.get(f"/api/v1/rounds/{round_id}/replay-signature")
        assert signature.status_code == 200
        assert signature.json()["decision_digest"] == result["decision_digest"]
        recovery = client.get("/api/v1/recovery")
        assert recovery.status_code == 200
        assert recovery.json()["clean_shutdown_report"] is False
        fixtures = client.get("/api/v1/demo/fixtures")
        assert fixtures.status_code == 200
        assert "main" in fixtures.json()["scenarios"]

        with ThreadPoolExecutor(max_workers=6) as executor:
            statuses = list(executor.map(lambda _: client.get(f"/api/v1/rounds/{round_id}").status_code, range(12)))
        assert statuses == [200] * 12


def test_http_errors_are_unified_business_json(tmp_path) -> None:
    app = create_app(tmp_path / "errors.db")
    with TestClient(app) as client:
        bad_layout = read_json("sample_layout.json")
        bad_layout["objects"][0]["k"] = 0
        response = client.post("/api/v1/layouts", json=bad_layout)
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "E_LAYOUT_INVALID"

        client.post("/api/v1/layouts", json=read_json("sample_layout.json"))
        bad_trace = read_json("sample_trace.json")
        bad_trace["requests"][1]["arrival_ms"] = 1
        response = client.post("/api/v1/traces", json=bad_trace)
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "E_TRACE_INVALID"

        trace = read_json("sample_trace.json")
        assert client.post("/api/v1/traces", json=trace).status_code == 201
        changed = read_json("sample_trace.json")
        changed["requests"][0]["priority"] = 0
        response = client.post("/api/v1/traces", json=changed)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "E_CONFLICTING_REQUEST"

        response = client.get("/api/v1/rounds/does-not-exist")
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "E_ROUND_NOT_FOUND"

        response = client.get("/api/v1/not-a-route")
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "E_ROUTE_NOT_FOUND"

        response = client.post("/api/v1/traces", json=[])
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_REQUEST_INVALID"


def test_round_mismatch_via_http(tmp_path) -> None:
    app = create_app(tmp_path / "mismatch.db")
    with TestClient(app) as client:
        client.post("/api/v1/layouts", json=read_json("sample_layout.json"))
        result = client.post("/api/v1/traces", json=read_json("mismatch_trace.json"))
        assert result.status_code == 201
        decision = result.json()["decisions"][0]
        assert decision["error"]["code"] == "E_RECONSTRUCT_MISMATCH"
