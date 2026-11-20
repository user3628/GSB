from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from gradsync.api import create_app
from gradsync.samples import build_sample_manifest, manifest_to_rounds


def test_api_complete_round_and_error_body(tmp_path: Path) -> None:
    app = create_app(tmp_path / "api.db")
    sample = manifest_to_rounds(build_sample_manifest())["demo-complete"]
    with TestClient(app) as client:
        created = client.post("/rounds", json=sample.create_request())
        assert created.status_code == 201, created.text
        for item in sample.all_submissions():
            response = client.post(
                f"/rounds/{sample.round_id}/submissions",
                json={
                    "node_id": item["node_id"],
                    "shard_id": item["shard_id"],
                    "sample_count": item["sample_count"],
                    "gradient": item["gradient"],
                },
            )
            assert response.status_code == 200, response.text
        finalized = client.post(f"/rounds/{sample.round_id}/finalize", json={})
        assert finalized.status_code == 200, finalized.text
        body = finalized.json()
        assert body["code"] == "OK"
        assert body["digest_sha256"] == sample.expected_digest()

        missing = client.get("/rounds/unknown")
        assert missing.status_code == 404
        assert missing.json() == {
            "ok": False,
            "code": "E_ROUND_NOT_FOUND",
            "message": "轮次不存在",
            "round_id": "unknown",
            "details": {},
        }


def test_api_conflict_returns_unified_error(tmp_path: Path) -> None:
    app = create_app(tmp_path / "api.db")
    sample = manifest_to_rounds(build_sample_manifest())["demo-complete"]
    item = sample.submissions_for("worker-a")[0]
    payload = {
        "node_id": item["node_id"],
        "shard_id": item["shard_id"],
        "sample_count": item["sample_count"],
        "gradient": item["gradient"],
    }
    with TestClient(app) as client:
        client.post("/rounds", json=sample.create_request())
        assert client.post(f"/rounds/{sample.round_id}/submissions", json=payload).status_code == 200
        altered = dict(payload)
        altered["gradient"] = list(payload["gradient"])
        altered["gradient"][0] += 1.0
        response = client.post(f"/rounds/{sample.round_id}/submissions", json=altered)
        assert response.status_code == 409
        body = response.json()
        assert body["ok"] is False
        assert body["code"] == "E_CONFLICTING_REPORT"
        assert set(body) == {"ok", "code", "message", "round_id", "details"}


def test_api_validation_error_is_unified(tmp_path: Path) -> None:
    app = create_app(tmp_path / "api.db")
    with TestClient(app) as client:
        response = client.post("/rounds", json={"round_id": "bad"})
        assert response.status_code == 422
        assert response.json()["code"] == "E_VALIDATION_ERROR"
        assert response.json()["ok"] is False

