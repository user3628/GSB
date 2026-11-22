from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from mv_workbench.app import create_app

from .conftest import SAMPLES


def test_api_end_to_end_and_business_error_envelopes(tmp_path: Path) -> None:
    app = create_app(tmp_path / "api.db", samples_dir=SAMPLES)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["ok"] is True
        assert "增量物化视图" in client.get("/").text

        diagnostics = client.get("/api/diagnostics").json()["data"]
        assert diagnostics["graph"]["graph_version"] == 1
        assert len(diagnostics["graph"]["layers"]) == 2

        refresh = client.post(
            "/api/refreshes", json={"request_id": "api-build", "mode": "all_stale"}
        )
        assert refresh.status_code == 200
        assert refresh.json()["data"]["summary"]["fresh"] == 7
        assert client.get("/api/views/v_region_revenue").json()["data"]["state"] == "FRESH"

        malformed = client.post("/api/base-tables/sales/batches", content=b"{not-json")
        assert malformed.status_code == 409
        assert malformed.json()["error"]["code"] == "E_STALE_BASE"

        batches = json.loads((SAMPLES / "batches.json").read_text(encoding="utf-8"))["batches"]
        batch = client.post("/api/base-tables/sales/batches", json=batches[0])
        assert batch.status_code == 200
        stale = client.post("/api/base-tables/sales/batches", json=batches[1])
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "E_STALE_BASE"
        assert stale.json()["error"]["context"]["current_version"] == 2

        cycle = {
            "views": [
                {"name": "a", "source": {"kind": "view", "name": "b"}, "operators": []},
                {"name": "b", "source": {"kind": "view", "name": "a"}, "operators": []},
            ]
        }
        cycled = client.put("/api/view-definitions", json=cycle)
        assert cycled.status_code == 422
        assert cycled.json()["error"]["code"] == "E_DEPENDENCY_CYCLE"

        unknown = {
            "views": [
                {
                    "name": "u",
                    "source": {"kind": "table", "name": "not_declared"},
                    "operators": [],
                }
            ]
        }
        unknown_response = client.put("/api/view-definitions", json=unknown)
        assert unknown_response.status_code == 422
        assert unknown_response.json()["error"]["code"] == "E_REFERENCE_UNKNOWN"

        transition = client.post(
            "/api/admin/state-transition",
            json={"view": "v_region_revenue", "state": "PARTIAL", "reason": "bad"},
        )
        assert transition.status_code == 409
        assert transition.json()["error"]["code"] == "E_INVALID_TRANSITION"

        recovery = client.get("/api/recovery")
        assert recovery.status_code == 200
        assert recovery.json()["data"]["latest"]["status"] == "CLEAN"

        missing = client.get("/api/views/not-a-view")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_NOT_FOUND"
