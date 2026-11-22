from __future__ import annotations

import base64
import io
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from PIL import Image

from mmcoop.api import create_app

from .conftest import mutate_config


def _client(config_path: Path, tmp_path: Path) -> TestClient:
    app = create_app(config_path, tmp_path / "api-data", tmp_path / "api-workspace")
    return TestClient(app)


def test_http_demo_build_edit_status_and_results(tmp_path: Path, config_path: Path):
    client = _client(config_path, tmp_path)
    reset = client.post("/api/demo/reset", json={"seed": 20261123, "fresh_workspace": True})
    assert reset.status_code == 200
    build = client.post("/api/build", json={})
    assert build.status_code == 200
    assert build.json()["data"]["status"] == "completed"

    status = client.get("/api/status").json()["data"]
    assert status["status"] == "ready"
    assert status["checkpoint"]["latest_sequence"] == 4
    version = client.get("/api/version").json()["data"]
    assert version["version_fingerprint"] == build.json()["data"]["version_fingerprint"]
    results = client.get("/api/results").json()["data"]
    assert results["current"] is True
    assert results["result"]["summary"]["verdict_counts"] == {"conflict": 1, "consistent": 2, "weak": 2}

    edited = client.patch("/api/data/texts/t03", json={"text": "changed http text"})
    assert edited.status_code == 200
    stale = client.get("/api/status").json()["data"]
    assert stale["status"] == "stale"
    assert stale["earliest_affected_stage"] == "text_encoding"
    assert stale["invalidations"][0]["reason_codes"] == ["text_content_changed"]
    stale_results = client.get("/api/results").json()["data"]
    assert stale_results["current"] is False
    rebuilt = client.post("/api/build", json={"through_stage": "aggregation"}).json()["data"]
    assert rebuilt["status"] == "completed"


@pytest.mark.parametrize(
    ("kind", "sample_id", "code", "stage"),
    [
        ("projection_dim", "i01", "E_PROJECTION_INVALID", "projection_alignment"),
        ("invalid_operator", "i01", "E_INVALID_OPERATOR", "fusion_scoring"),
        ("text_too_long", "t01", "E_TEXT_TOO_LONG", "text_encoding"),
        ("similarity_nan", "i01", "E_NUMERIC", "projection_alignment"),
        ("missing_ref", "i01", "E_MISSING_REF", "projection_alignment"),
    ],
)
def test_http_injection_errors_do_not_advance_checkpoints(
    tmp_path: Path,
    config_path: Path,
    kind: str,
    sample_id: str,
    code: str,
    stage: str,
):
    client = _client(config_path, tmp_path)
    client.post("/api/demo/reset", json={"fresh_workspace": True})
    client.post("/api/build", json={})
    before = client.get("/api/checkpoints").json()["data"]["scan"]["latest_sequence"]
    response = client.post("/api/inject", json={"kind": kind, "sample_id": sample_id})
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == code
    assert error["stage"] == stage
    assert error["sample_id"] == sample_id
    after = client.get("/api/checkpoints").json()["data"]["scan"]["latest_sequence"]
    assert after == before


def test_http_import_with_png_payload(tmp_path: Path, config_path: Path):
    client = _client(config_path, tmp_path)
    buffer = io.BytesIO()
    Image.new("RGB", (2, 2), (10, 20, 30)).save(buffer, format="PNG")
    payload = {
        "texts": [{"text_id": "only", "text": "中文 only sample", "lang": "zh-en", "source_channel": "import"}],
        "images": [
            {
                "image_id": "img",
                "png": "png/imported.png",
                "png_base64": base64.b64encode(buffer.getvalue()).decode("ascii"),
                "feature": [1.0] + [0.0] * 63,
                "text_id": "only",
                "source_operator": "vision",
            }
        ],
    }
    imported = client.post("/api/data/import", json=payload)
    assert imported.status_code == 200, imported.text
    assert imported.json()["data"]["text_count"] == 1
    built = client.post("/api/build", json={})
    assert built.status_code == 200
    assert built.json()["data"]["status"] == "completed"
    results = client.get("/api/results").json()["data"]["result"]
    assert results["summary"]["text_count"] == 1


def test_http_background_stop_and_resume(tmp_path: Path, config_path: Path):
    mutate_config(config_path, lambda raw: raw["runtime"].__setitem__("unit_delay_ms", 40))
    client = _client(config_path, tmp_path)
    client.post("/api/demo/reset", json={"fresh_workspace": True})
    started = client.post("/api/build", json={"background": True})
    assert started.status_code == 200
    assert started.json()["data"]["status"] == "accepted"
    time.sleep(0.08)
    stop = client.post("/api/stop")
    assert stop.status_code == 200
    deadline = time.monotonic() + 8
    background = None
    while time.monotonic() < deadline:
        background = client.get("/api/build/status").json()["data"]
        if background["status"] != "running":
            break
        time.sleep(0.03)
    assert background is not None
    assert background["status"] == "stopped", background
    before_resume = client.get("/api/checkpoints").json()["data"]["scan"]["latest_sequence"]
    assert before_resume == 0
    resumed = client.post("/api/resume", json={})
    assert resumed.status_code == 200
    assert resumed.json()["data"]["status"] == "completed"
    assert client.get("/api/status").json()["data"]["status"] == "ready"
