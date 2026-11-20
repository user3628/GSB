from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from logindex.api import create_app
from logindex.store import reset_database


@pytest.fixture()
def client(tmp_path):
    db = tmp_path / "api.db"
    reset_database(db)
    app = create_app(db)
    with TestClient(app) as test_client:
        yield test_client


def _sample(name):
    return (Path(__file__).resolve().parents[1] / "samples" / name).read_text(
        encoding="utf-8"
    ).splitlines()


def test_health_and_submit_flow(client):
    assert client.get("/health").json()["current_version"] is None
    resp = client.post("/batches", json={
        "batch_id": "B1", "lines": _sample("batch1_full.txt")
    })
    assert resp.status_code == 200
    assert resp.json()["version"] == 1
    assert resp.json()["path_mode"] == "APPEND_ONLY"


def test_error_response_uses_business_code(client):
    resp = client.post("/batches", json={"batch_id": "BAD",
                                         "lines": _sample("batch_invalid.txt")})
    assert resp.status_code == 400
    body = resp.json()["error"]
    assert body["code"] == "E_LINE_INVALID"

    client.post("/batches", json={"batch_id": "B1",
                                  "lines": _sample("batch1_full.txt")})
    stale = client.post("/batches", json={
        "batch_id": "BX", "base_version": 0,
        "lines": ["1700000099999|svc|INFO|x"],
    })
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STALE_BASE"


def test_query_index_state_and_recovery_endpoints(client):
    client.post("/batches", json={"batch_id": "B1",
                                  "lines": _sample("batch1_full.txt")})
    client.post("/batches", json={"batch_id": "B2", "base_version": 1,
                                  "lines": _sample("batch2_append.txt")})

    query = client.get("/records/query", params={"service": "auth-svc",
                                                 "level": "INFO"})
    assert query.json()["hit_count"] == 3

    state = client.get("/index-state").json()
    assert state["version"] == 2 and state["materialized"] is True
    historical = client.get("/index-state", params={"version": 1})
    assert historical.json()["materialized"] is False

    report = client.get("/recovery-report").json()
    assert report["current_version"] == 2
    assert "startup_report" in report


def test_retimestamp_and_invalidate_endpoints(client):
    client.post("/batches", json={"batch_id": "B1",
                                  "lines": _sample("batch1_full.txt")})
    rt = client.post("/retimestamp", json={
        "batch_id": "B1", "line_no": 3, "new_timestamp_ms": 1700000000250
    })
    assert rt.json()["rebuilt_partitions"] == ["auth-svc"]

    inv = client.post("/batches/invalidate", json={"batch_id": "B1"})
    assert inv.json()["state"] == "INVALIDATED"
    bad = client.post("/batches/invalidate", json={"batch_id": "B1"})
    assert bad.json()["error"]["code"] == "E_INVALID_TRANSITION"


def test_arm_crash_endpoint_exists(client):
    resp = client.post("/test/arm-crash")
    assert resp.json()["armed"] is True
