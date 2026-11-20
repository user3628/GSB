from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from join_planner.api import create_runtime_app
from join_planner.config import PlannerConfig


ROOT = Path(__file__).resolve().parent.parent
QUERY_CATALOG = ROOT / "data" / "queries" / "queries.json"


def _client(snapshot_dir: Path, catalog_path: Path, runtime: Path) -> TestClient:
    app = create_runtime_app(
        snapshot_dir=snapshot_dir,
        query_catalog_path=catalog_path,
        runtime_dir=runtime,
        initial_snapshot_filename="snapshot_v1.json",
        config=PlannerConfig(fast_candidate_budget=2),
    )
    return TestClient(app)


def test_http_plans_modes_idempotency_and_cached_read(snapshot_dir: Path, tmp_path: Path) -> None:
    with _client(snapshot_dir, QUERY_CATALOG, tmp_path / "runtime") as client:
        exact_response = client.post(
            "/v1/plans",
            json={"query_id": "q_product_priority", "snapshot_version": 1, "mode": "exact"},
        )
        assert exact_response.status_code == 200
        assert exact_response.headers["x-plan-result"] == "built"
        exact = exact_response.json()
        assert exact["snapshot"]["version"] == 1
        assert exact["search"]["candidates_evaluated"] == 8

        repeated = client.post(
            "/v1/plans",
            json={"query_id": "q_product_priority", "snapshot_version": 1, "mode": "exact"},
        )
        assert repeated.status_code == 200
        assert repeated.headers["x-plan-result"] == "reused"
        assert repeated.json() == exact

        fast_response = client.post(
            "/v1/plans",
            json={"query_id": "q_product_priority", "snapshot_version": 1, "mode": "fast"},
        )
        assert fast_response.status_code == 200
        fast = fast_response.json()
        assert fast["mode"] == "fast"
        assert fast["search"]["candidates_evaluated"] == 2
        assert fast["search"]["pruned_branches"] == 6
        assert fast["plan_digest"] != exact["plan_digest"]

        cached = client.get(
            "/v1/plans/q_product_priority",
            params={"mode": "fast", "snapshot_version": 1},
        )
        assert cached.status_code == 200
        assert cached.json() == fast


def test_http_snapshot_switch_and_retired_version(snapshot_dir: Path, tmp_path: Path) -> None:
    with _client(snapshot_dir, QUERY_CATALOG, tmp_path / "runtime") as client:
        loaded = client.post("/v1/snapshots/load", json={"filename": "snapshot_v2.json"})
        assert loaded.status_code == 200
        assert loaded.json()["snapshot"]["version"] == 2
        retired = client.post(
            "/v1/plans",
            json={"query_id": "q_customer_revenue", "snapshot_version": 1, "mode": "exact"},
        )
        assert retired.status_code == 409
        assert retired.json()["code"] == "E_VERSION_RETIRED"
        current = client.post(
            "/v1/plans",
            json={"query_id": "q_customer_revenue", "snapshot_version": 2, "mode": "exact"},
        )
        assert current.status_code == 200
        assert current.json()["snapshot"]["version"] == 2


def test_http_business_error_codes(snapshot_dir: Path, tmp_path: Path, extended_catalog) -> None:
    with _client(snapshot_dir, extended_catalog.path, tmp_path / "runtime") as client:
        invalid_query = client.post(
            "/v1/plans",
            json={"query_id": "missing_query", "snapshot_version": 1, "mode": "exact"},
        )
        assert invalid_query.status_code == 400
        assert invalid_query.json()["code"] == "E_INVALID_QUERY"

        malformed = client.post(
            "/v1/plans",
            json={"query_id": "q_customer_revenue", "snapshot_version": 0, "mode": "wrong"},
        )
        assert malformed.status_code == 400
        assert malformed.json()["code"] == "E_INVALID_QUERY"

        too_large = client.post(
            "/v1/plans",
            json={"query_id": "q_too_large", "snapshot_version": 1, "mode": "exact"},
        )
        assert too_large.status_code == 422
        assert too_large.json()["code"] == "E_SEARCH_SPACE_TOO_LARGE"
        assert too_large.json()["details"]["max_relations"] == 8

        payload = json.loads((snapshot_dir / "snapshot_v2.json").read_text(encoding="utf-8"))
        payload["relations"]["orders"]["rows"] += 10
        (snapshot_dir / "tampered.json").write_text(json.dumps(payload), encoding="utf-8")
        invalid_snapshot = client.post(
            "/v1/snapshots/load",
            json={"filename": "tampered.json"},
        )
        assert invalid_snapshot.status_code == 409
        assert invalid_snapshot.json()["code"] == "E_SNAPSHOT_INVALID"
        health = client.get("/health").json()
        assert health["snapshot_version"] == 1

        unknown_plan = client.get("/v1/plans/never-built")
        assert unknown_plan.status_code == 404
        assert unknown_plan.json()["code"] == "E_PLAN_NOT_FOUND"

