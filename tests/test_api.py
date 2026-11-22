from __future__ import annotations

from hotroute import samples
from hotroute.api import create_app
from hotroute.engine import Engine


def test_http_api_snapshot_increment_and_uniform_errors(db_path):
    engine = Engine(db_path)
    app = create_app(engine)
    app.config["TESTING"] = True
    client = app.test_client()

    response = client.post("/api/snapshots", json=samples.initial_snapshot(snapshot_seq=1))
    assert response.status_code == 201
    body = response.get_json()
    assert body["ok"] is True
    state_version = body["data"]["state_version"]

    response = client.post(
        "/api/increments",
        json=samples.quant_increment(state_version, snapshot_seq=2),
    )
    assert response.status_code == 201
    manifest = response.get_json()["data"]["migrations"][0]
    assert manifest["diff"]["mask"] == 4

    response = client.get("/api/migrations")
    assert response.status_code == 200
    assert len(response.get_json()["data"]) == 2

    response = client.post(
        "/api/grays/decide",
        json={"request_id": "api-ratio", "model_id": samples.MODEL_ID, "ratio": -0.1},
    )
    assert response.status_code == 400
    error = response.get_json()
    assert error["ok"] is False
    assert error["code"] == "E_GRAY_RATIO_INVALID"
    assert error["context"]["legal_interval"] == [0, 1]
    assert "current_ratio" in error["context"]

    bad_graph = samples.v1_graph()
    bad_graph["nodes"][1]["inputs"][0]["shape"] = [2, 99]
    bad_snapshot = samples.initial_snapshot(snapshot_seq=3)
    bad_snapshot["models"][0]["graph"] = bad_graph
    response = client.post("/api/snapshots", json=bad_snapshot)
    assert response.status_code == 422
    error = response.get_json()
    assert error["code"] == "E_GRAPH_INVALID"
    assert error["context"]["node_id"] == "norm"

    response = client.get("/does-not-exist")
    assert response.status_code == 404
    assert response.get_json()["ok"] is False
    assert response.get_json()["code"] == "E_BAD_REQUEST"


def test_http_inference_returns_tensor_summary(db_path):
    engine = Engine(db_path)
    app = create_app(engine)
    app.config["TESTING"] = True
    client = app.test_client()
    client.post("/api/snapshots", json=samples.initial_snapshot(snapshot_seq=1))
    response = client.post(
        "/api/infer",
        json={"request_id": "api-infer", "model_id": samples.MODEL_ID, "seed": 12},
    )
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert data["bound_version"] == "v1"
    assert data["result"]["outputs"]["act"]["shape"] == [2, 6]
    assert data["result"]["outputs"]["act"]["dtype"] == "float32"
    assert len(data["result"]["outputs"]["act"]["content_sha256"]) == 64

