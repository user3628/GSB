from __future__ import annotations

import copy
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from capacity_ledger.api import create_app

from conftest import load_json, sign


def make_client(tmp_path: Path) -> tuple[TestClient, Path]:
    db_path = tmp_path / "api.sqlite3"
    app = create_app(db_path, tmp_path / "intents", recover_on_startup=False)
    return TestClient(app), db_path


def test_api_success_and_unified_errors(sample_root: Path, tmp_path: Path) -> None:
    client, db_path = make_client(tmp_path)
    with client:
        response = client.post(
            "/v1/snapshots", json=load_json(sample_root / "snapshot_v1.json")
        )
        assert response.status_code == 200
        assert response.json()["version"] == 1

        invalid = load_json(sample_root / "snapshot_v1.json")
        invalid["services"]["auth-service"]["replicas"] = 99
        response = client.post("/v1/snapshots", json=invalid)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_SNAPSHOT_INVALID"

        response = client.post(
            "/v1/increments",
            json=load_json(sample_root / "increments" / "01_auth_price_increment.json"),
        )
        assert response.status_code == 200

        stale = load_json(sample_root / "increments" / "01_auth_price_increment.json")
        stale["commit_id"] = "stale-api"
        stale["services"]["auth-service"]["price_core_cents_per_core_hour"] = 21
        stale = sign(stale)
        response = client.post("/v1/increments", json=stale)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "E_STALE_BASE"

        response = client.post(
            "/v1/increments",
            json=load_json(sample_root / "increments" / "02_catalog_cores_increment.json"),
        )
        assert response.status_code == 200
        response = client.post(
            "/v1/increments",
            json=load_json(sample_root / "increments" / "03_payments_budget_violation.json"),
        )
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "E_BUDGET_EXCEEDED"

        committed = load_json(sample_root / "increments" / "01_auth_price_increment.json")
        committed["commit_id"] = "conflict-api"
        committed = sign(committed)
        response = client.post("/v1/increments", json=committed)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "E_STALE_BASE"

        response = client.get("/does-not-exist")
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "E_ROUTE_NOT_FOUND"

    with sqlite3.connect(db_path) as connection:
        metrics_json = connection.execute(
            "SELECT metrics_json FROM ledger_versions WHERE version = 1"
        ).fetchone()[0]
        tampered = metrics_json.replace(
            '"total_cost_cents_per_hour":1866', '"total_cost_cents_per_hour":1'
        )
        connection.execute(
            "UPDATE ledger_versions SET metrics_json = ? WHERE version = 1", (tampered,)
        )
        connection.commit()
    app = create_app(db_path, tmp_path / "intents", recover_on_startup=False)
    with TestClient(app) as client:
        response = client.get("/v1/admin/verify")
        assert response.status_code == 500
        assert response.json()["error"]["code"] == "E_LEDGER_CORRUPT"


def test_api_conflicting_commit_id(sample_root: Path, tmp_path: Path) -> None:
    client, _ = make_client(tmp_path)
    with client:
        client.post("/v1/snapshots", json=load_json(sample_root / "snapshot_v1.json"))
        increment = load_json(sample_root / "increments" / "01_auth_price_increment.json")
        assert client.post("/v1/increments", json=increment).status_code == 200
        conflicting = copy.deepcopy(increment)
        conflicting["services"]["auth-service"]["price_core_cents_per_core_hour"] = 21
        conflicting = sign(conflicting)
        response = client.post("/v1/increments", json=conflicting)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "E_CONFLICTING_COMMIT"

