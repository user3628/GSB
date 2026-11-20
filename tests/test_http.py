from __future__ import annotations

import shutil
import time

import pytest

from gsb.app import create_app


@pytest.fixture
def client(project_root, fast_config):
    state_dir = project_root / "state" / "http"
    if state_dir.exists():
        shutil.rmtree(state_dir)
    app = create_app(project_root, state_dir=state_dir, data_dir=project_root / "data")
    app.testing = True
    return app.test_client()


def test_http_full_workflow(client, fast_config):
    assert client.get("/").status_code == 200
    assert b"GSB" in client.get("/").data
    assert client.get("/api/health").get_json()["ok"] is True

    imported = client.post("/api/import", json={"dataset": "small"})
    assert imported.status_code == 200
    assert imported.get_json()["dataset"]["rows"] == 800

    configured = client.put("/api/config", json={"config": fast_config})
    assert configured.status_code == 200
    assert configured.get_json()["earliest_invalid_stage"] == "binning"

    trained = client.post("/api/train", json={"until_stage": "diagnostics", "wait": True})
    assert trained.status_code == 200
    body = trained.get_json()
    assert body["status"]["overall"] == "complete"
    assert body["run_digest"]

    status = client.get("/api/status").get_json()
    assert status["overall"] == "complete"
    assert all(status["stages"][stage]["status"] == "valid" for stage in status["stages"])

    reliability = client.get("/api/reliability")
    assert reliability.status_code == 200
    diagnostics = reliability.get_json()["diagnostics"]
    assert diagnostics["metrics"]["log_loss"] > 0
    assert diagnostics["numeric_check"]["status"] == "passed"

    comparison = client.post("/api/compare-batches", json={"batch_sizes": [7, 64, 800]})
    assert comparison.status_code == 200
    assert comparison.get_json()["all_equal"] is True

    probabilities = client.get("/api/export/probabilities")
    try:
        assert probabilities.status_code == 200
        assert probabilities.mimetype == "text/csv"
        assert b"probability_q" in probabilities.data
    finally:
        probabilities.close()
    report = client.get("/api/export/report")
    assert report.status_code == 200
    assert report.get_json()["report"]["diagnostics"]["metrics"]["brier"] > 0


def test_http_error_codes(client, fast_config):
    client.post("/api/import", json={"dataset": "small"})
    response = client.put("/api/config", json={"config": {"bins": 0}})
    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "E_INVALID_PARAM"
    client.put("/api/config", json={"config": fast_config})
    client.post("/api/train", json={"wait": True})
    response = client.post("/api/compare-batches", json={"batch_sizes": [0]})
    assert response.status_code == 409
    assert response.get_json()["error"]["code"] == "E_BATCH_STATE"


def test_http_stop_then_resume(client, fast_config):
    client.post("/api/import", json={"dataset": "small"})
    client.put("/api/config", json={"config": fast_config})
    stopped = client.post("/api/stop", json={})
    assert stopped.status_code == 200
    status = client.get("/api/status").get_json()
    assert status["stop_requested"] is True
    resumed = client.post("/api/resume", json={"wait": True})
    assert resumed.status_code == 200
    assert resumed.get_json()["status"]["overall"] == "complete"


def test_http_background_training_polling(client, fast_config):
    client.post("/api/import", json={"dataset": "small"})
    client.put("/api/config", json={"config": fast_config})
    started = client.post("/api/train", json={"until_stage": "diagnostics", "wait": False})
    assert started.status_code == 202
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        status = client.get("/api/status").get_json()
        if not status["service"]["training"]:
            break
        time.sleep(0.02)
    else:
        pytest.fail("background training did not finish")
    status = client.get("/api/status").get_json()
    assert status["overall"] == "complete"
    assert status["service"]["last_background_error"] is None
