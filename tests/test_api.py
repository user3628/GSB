from __future__ import annotations

import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient

from qworkbench.api import Settings, create_app


def _client(tmp_path: Path, config_path: Path, data_dir: Path) -> TestClient:
    app = create_app(
        Settings(
            config_path=config_path,
            data_dir=data_dir,
            annotation_file=None,
            workdir=tmp_path / "api-work",
            default_data_dir=data_dir,
        )
    )
    return TestClient(app)


def test_http_workbench_end_to_end(tmp_path, config, dataset, config_path) -> None:
    client = _client(tmp_path, config_path, Path("data/demo").resolve())
    health = client.get("/health")
    assert health.status_code == 200
    missing = client.get("/api/v1/diagnostics/numeric")
    assert missing.status_code == 409
    assert missing.json()["code"] == "E_INVALID_STAGE"

    run = client.post("/api/v1/run", json={"wait": True})
    assert run.status_code == 200, run.text
    result = run.json()["job"]["result"]
    assert result["status"] == "COMPLETE"
    status = client.get("/api/v1/status").json()
    assert status["state"] == "COMPLETE"
    fingerprints = client.get("/api/v1/fingerprints").json()
    assert fingerprints["stage_fingerprints"]["mix"]["valid"] is True
    numeric = client.get("/api/v1/diagnostics/numeric").json()["diagnostics"]
    assert numeric["corpus_aggregation"]["combined_deviation_bound"] > 0
    allocation = client.get("/api/v1/allocation").json()["allocation"]
    assert allocation["summary"]["actual_sampled"] == 30
    consistency = client.get("/api/v1/consistency").json()["consistency"]
    assert "legacy-import" in consistency["summary"]["review_sources"]
    manifest = client.get("/api/v1/manifest").json()["manifest"]
    exported = client.get("/api/v1/export")
    assert exported.status_code == 200
    assert hashlib.sha256(exported.content).hexdigest() == manifest["mixture_sha256"]


def test_http_import_replaces_dataset_and_invalidates_stage_one(tmp_path, config_path, dataset) -> None:
    client = _client(tmp_path, config_path, Path("data/demo").resolve())
    before = client.post("/api/v1/run", json={"wait": True})
    assert before.json()["job"]["result"]["status"] == "COMPLETE"
    imported_samples = dataset.samples[:20]
    imported_annotations = {
        sample["id"]: dataset.annotations[sample["id"]]
        for sample in imported_samples
        if sample["id"] in dataset.annotations
    }
    imported = client.post(
        "/api/v1/import",
        json={"samples": imported_samples, "annotations": imported_annotations},
    )
    assert imported.status_code == 200, imported.text
    status = client.get("/api/v1/status").json()
    assert status["dataset"]["samples"] == 20
    run = client.post("/api/v1/run", json={"wait": True})
    assert run.status_code == 200
    after = client.get("/api/v1/status").json()
    assert after["last_invalidation"]["earliest_stage"] == "normalize"
    assert after["state"] == "COMPLETE"


def test_http_stop_and_resume(tmp_path, config_path) -> None:
    client = _client(tmp_path, config_path, Path("data/demo").resolve())
    stopped = client.post("/api/v1/control/stop", json={"reason": "test-stop"})
    assert stopped.status_code == 200
    run = client.post("/api/v1/run", json={"wait": True}).json()["job"]["result"]
    assert run["status"] == "STOPPED"
    status = client.get("/api/v1/status").json()
    assert status["stop_requested"] is True
    resumed = client.post("/api/v1/control/resume", json={"wait": True})
    assert resumed.status_code == 200
    resumed_status = client.get("/api/v1/status").json()
    assert resumed_status["state"] == "COMPLETE"
    assert resumed_status["stop_requested"] is False


def test_http_start_stage_injection_returns_invalid_stage(tmp_path, config_path) -> None:
    client = _client(tmp_path, config_path, Path("data/demo").resolve())
    response = client.post("/api/v1/run", json={"wait": True, "start_stage": "score"})
    assert response.status_code == 409
    assert response.json()["code"] == "E_INVALID_STAGE"
