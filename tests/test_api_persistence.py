from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from app.models import BatchSpecInput, spec_to_jsonable
from app.storage import Storage, canonical_json
from app.service import utc_now
from tests.conftest import load_raw


def _client(tmp_path: Path) -> TestClient:
    return TestClient(create_app(db_path=tmp_path / "scheduler.db", samples_dir=Path("samples")))


def test_http_end_to_end_and_unified_errors(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["mode"] == "offline"

        created = client.post("/api/batches", json={"batchId": "api-original", "sample": "original"})
        assert created.status_code == 201
        payload = created.json()
        assert payload["batchId"] == "api-original"
        assert payload["status"] == "completed"
        assert payload["result"]["summary"]["placed"] == 4

        fetched = client.get("/api/batches/api-original")
        assert fetched.status_code == 200
        assert fetched.json()["result"]["resourceConsistency"]["allNodesConsistent"] is True

        node = client.get("/api/batches/api-original/nodes/node-a")
        assert node.status_code == 200
        node_payload = node.json()
        assert node_payload["node"]["name"] == "node-a"
        assert node_payload["podFilterTraces"]
        assert node_payload["placements"]

        duplicate = client.post("/api/batches", json={"batchId": "api-original", "sample": "original"})
        assert duplicate.status_code == 409
        assert duplicate.json()["error"]["code"] == "E_BATCH_EXISTS"

        missing = client.get("/api/batches/no-such-batch")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_BATCH_NOT_FOUND"
        assert set(missing.json()["error"]) == {"code", "message", "details"}

        invalid = client.post("/api/batches", json={"batchId": "invalid", "sample": "missing"})
        assert invalid.status_code == 404
        assert invalid.json()["error"]["code"] == "E_SAMPLE_NOT_FOUND"

        index = client.get("/")
        assert index.status_code == 200
        assert "调度与亲和性判定工作台" in index.text
        assert client.get("/static/app.js").status_code == 200
        assert client.get("/api/openapi.json").status_code == 200
        assert client.get("/docs").status_code == 404


def test_invalid_quantity_does_not_write_any_batch_state(tmp_path: Path) -> None:
    raw = load_raw("original")
    raw["nodes"][0]["allocatable"]["cpu"] = "2Xi"
    with _client(tmp_path) as client:
        response = client.post("/api/batches", json={"batchId": "bad-quantity", "spec": raw})
        assert response.status_code == 422
        body = response.json()
        assert body["error"]["code"] == "E_INVALID_QUANTITY"
        assert body["error"]["details"]["path"] == "nodes[0].allocatable.cpu"

        assert client.get("/api/batches/bad-quantity").status_code == 404
        service = client.app.state.service
        assert service.storage.get_batch("bad-quantity") is None
        assert service.storage.list_incomplete() == []


def test_startup_recovery_recomputes_intent_from_persisted_spec(tmp_path: Path) -> None:
    db_path = tmp_path / "recovery.db"
    storage = Storage(db_path)
    parsed = BatchSpecInput.model_validate(load_raw("original"))
    persisted_spec = canonical_json(spec_to_jsonable(parsed))
    storage.begin_intent(
        batch_id="forced-termination",
        spec_name="original",
        spec_json=persisted_spec,
        timestamp=utc_now(),
    )
    assert storage.get_batch("forced-termination")["status"] == "intent"

    app = create_app(db_path=db_path, samples_dir=Path("samples"))
    with TestClient(app) as client:
        recovered = client.get("/api/batches/forced-termination")
        assert recovered.status_code == 200
        payload = recovered.json()
        assert payload["status"] == "completed"
        assert payload["recovered"] is True
        assert payload["result"]["summary"]["placed"] == 4

        report = client.get("/api/recovery")
        assert report.status_code == 200
        report_payload = report.json()
        assert report_payload["scope"] == "persisted-facts-only"
        actions = [event["action"] for event in report_payload["events"]]
        assert "recovery_started" in actions
        assert "recomputed_from_persisted_spec" in actions
        event = next(item for item in report_payload["events"] if item["batchId"] == "forced-termination")
        assert event["details"]["specSha256"]
        assert report_payload["currentIncomplete"] == []
        assert any("does not claim" in limitation for limitation in report_payload["limitations"])


def test_inline_spec_api_and_node_errors(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        raw = load_raw("preemption")
        response = client.post("/api/batches", json={"batchId": "inline-preemption", "spec": raw})
        assert response.status_code == 201
        payload = response.json()
        assert payload["result"]["preemption"]["enabled"] is True
        assert payload["result"]["summary"]["preempted"] == 1

        missing_node = client.get("/api/batches/inline-preemption/nodes/nope")
        assert missing_node.status_code == 404
        assert missing_node.json()["error"]["code"] == "E_NODE_NOT_FOUND"

        malformed = client.post("/api/batches", json={"batchId": "malformed", "spec": {"nodes": "bad"}})
        assert malformed.status_code == 422
        assert malformed.json()["error"]["code"] == "E_INVALID_SPEC"
