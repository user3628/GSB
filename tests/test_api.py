import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app


def _client(tmp_path: Path):
    app = create_app(db_path=tmp_path / "api.db", allow_crash=False)
    return TestClient(app)


def test_api_health_samples_and_normal_run(tmp_path):
    with _client(tmp_path) as client:
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["data"]["status"] == "ok"

        samples = client.get("/api/samples").json()["data"]["samples"]
        assert {item["name"] for item in samples} == {
            "normal",
            "fragment",
            "precision-drift",
            "dimensions-invalid",
        }

        response = client.post("/api/demo/run/normal")
        assert response.status_code == 201
        payload = response.json()
        assert payload["ok"] is True
        run = payload["data"]["run"]
        assert run["state"] == "COMMITTED"
        assert len(run["quantiles"]) == 3
        assert run["quantiles"][0]["label"] == "P50"
        assert run["quantiles"][2]["sample_count"] == len(run["batches"])
        assert isinstance(run["batches"][0]["split_reasons"], list)
        assert isinstance(run["batches"][0]["bitmap"], dict)

        batch_uid = run["batches"][0]["batch_uid"]
        metrics = client.get(f"/api/batches/{batch_uid}/metrics")
        assert metrics.status_code == 200
        assert metrics.json()["data"]["rounding"]["output_unit"] == "0.01ms"
        errors = client.get(f"/api/batches/{batch_uid}/errors")
        assert errors.status_code == 200
        assert len(errors.json()["data"]["error_chain"]["chain"]) == 4
        validation = client.get(f"/api/batches/{batch_uid}/validation")
        assert validation.json()["data"]["conclusion"] == "PASS"

        repeated = client.post("/api/demo/run/normal").json()["data"]["run"]
        assert repeated["deterministic_digest"] == run["deterministic_digest"]


def test_api_fragment_returns_bitmap_change_and_mixed_rejection(tmp_path):
    with _client(tmp_path) as client:
        response = client.post("/api/demo/run/fragment")
        assert response.status_code == 201
        run = response.json()["data"]["run"]
        events = run["memory_plan"]["fragmentation_events"]
        assert events
        assert events[0]["before_bitmap"]["text"] != events[0]["after_bitmap"]["text"]
        assert events[0]["mixed_rejection"]["code"] == "MIXED_BATCH_REJECTED_LATENCY_CAP"


def test_api_precision_drift_and_error_envelope(tmp_path):
    with _client(tmp_path) as client:
        response = client.post("/api/demo/run/precision-drift")
        assert response.status_code == 409
        error = response.json()["error"]
        assert error["code"] == "E_PRECISION_DRIFT"
        assert error["details"]["persisted"] is True
        run = client.get(f"/api/runs/{error['details']['run_id']}").json()["data"]
        assert run["state"] == "VALIDATION_FAILED"
        failed_fields = client.get(
            f"/api/batches/{run['batches'][0]['batch_uid']}/validation"
        ).json()["data"]
        assert failed_fields["conclusion"] == "FAIL"


def test_api_spec_invalid_trace_invalid_and_not_found(tmp_path):
    with _client(tmp_path) as client:
        invalid = client.get("/api/samples/dimensions-invalid").json()["data"]
        response = client.post("/api/traces", json=invalid["payload"])
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_SPEC_INVALID"
        assert response.json()["error"]["details"]["written"] is False

        normal = client.get("/api/samples/normal").json()["data"]["payload"]
        normal["trace"]["requests"][0]["arrival_ms"] = 99
        response = client.post("/api/traces", json=normal)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_TRACE_INVALID"

        missing = client.get("/api/batches/not-a-batch/metrics")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_BATCH_NOT_FOUND"


def test_api_recovery_report_and_intent_log(tmp_path):
    with _client(tmp_path) as client:
        report = client.get("/api/recovery/report")
        assert report.status_code == 200
        body = report.json()["data"]
        assert body["pending_intents_found"] == 0
        assert body["scope"].startswith("SQLite committed rows only")
        assert body["limitations"]

        intents = client.get("/api/recovery/intents").json()["data"]["intents"]
        assert intents
        assert intents[-1]["kind"] == "RECOVERY_SCAN"
        assert all(item["status"] == "COMMITTED" for item in intents)


def test_non_monotonic_trace_writes_no_batches(tmp_path):
    database = tmp_path / "api.db"
    with _client(tmp_path) as client:
        payload = client.get("/api/samples/normal").json()["data"]["payload"]
        payload["trace"]["requests"][0]["arrival_ms"] = 99
        response = client.post("/api/traces", json=payload)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_TRACE_INVALID"
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM traces").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0
