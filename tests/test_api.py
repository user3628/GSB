from __future__ import annotations

import copy
import json


def test_api_end_to_end(client) -> None:
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["scenario_loaded"] is True

    blocked = client.post(
        "/api/v1/evictions/check",
        json={"pod": "ledger-0", "request_id": "api-single-blocked"},
    )
    assert blocked.status_code == 409
    assert blocked.json()["error"]["code"] == "E_PDB_BLOCKED"
    assert blocked.json()["error"]["details"]["decision"]["blocking_reasons"][0]["gap"] == 1

    no_node = client.post(
        "/api/v1/evictions/check",
        json={"pod": "web-b-offsite", "request_id": "api-no-node"},
    )
    assert no_node.status_code == 409
    assert no_node.json()["error"]["code"] == "E_NO_FEASIBLE_NODE"

    drain = client.post(
        "/api/v1/drains",
        json={"node": "worker-a", "request_id": "api-drain"},
    )
    assert drain.status_code == 200, drain.text
    body = drain.json()
    assert body["release_summary"]["actual_allowed"] == 2
    assert body["release_summary"]["naive_allowed"] == 1

    detail = client.get("/api/v1/drains/api-drain")
    assert detail.status_code == 200
    assert detail.json()["drain_id"] == body["drain_id"]
    assert len(detail.json()["steps"]) == 3

    replay = client.post(
        "/api/v1/drains",
        json={"node": "worker-a", "request_id": "api-drain"},
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True

    missing = client.get("/api/v1/drains/missing-drain")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "E_DRAIN_NOT_FOUND"

    bad_body = client.post("/api/v1/evictions/check", json={})
    assert bad_body.status_code == 400
    assert bad_body.json()["error"]["code"] == "E_SPEC_INVALID"

    bad_sample = client.post(
        "/api/v1/samples/load", json={"sample_path": "does/not/exist.json"}
    )
    assert bad_sample.status_code == 400
    assert bad_sample.json()["error"]["code"] == "E_SPEC_INVALID"


def test_api_selector_empty(client, sample_data, tmp_path) -> None:
    data = copy.deepcopy(sample_data)
    data["pdbs"][0]["selector"] = {"not-present": "true"}
    invalid_path = tmp_path / "selector-empty.json"
    invalid_path.write_text(json.dumps(data), encoding="utf-8")
    response = client.post(
        "/api/v1/samples/load", json={"sample_path": str(invalid_path)}
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "E_SELECTOR_EMPTY"

