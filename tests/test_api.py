from __future__ import annotations

import copy
import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.api import create_app
from app.snapshot import attach_checksum, pretty_json


def test_http_api_end_to_end(tmp_path: Path, baseline_payload: dict, adjusted_payload: dict, reseal) -> None:
    database = tmp_path / "api.sqlite3"
    app = create_app(database)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200

        submitted = client.post(
            "/api/v1/snapshots",
            params={"base_version": 0},
            content=pretty_json(baseline_payload),
            headers={"content-type": "application/json"},
        )
        assert submitted.status_code == 201, submitted.text
        body = submitted.json()
        assert body["snapshot_version"] == 1
        assert body["current_version"] == 1
        assert body["weighted_consistency"] == "1.000000"

        metrics = client.get("/api/v1/snapshots/1/metrics")
        assert metrics.status_code == 200
        assert metrics.json()["components"]["api"]["quota_share"]["value"] == "0.400000"

        consistency = client.get("/api/v1/snapshots/1/consistency")
        assert consistency.status_code == 200
        assert consistency.json()["conclusion"]["status"] == "CONSISTENT"

        errors = client.get("/api/v1/snapshots/1/errors")
        assert errors.status_code == 200
        assert errors.json()["rounding"]["operation_count"] > 0

        recovery = client.get("/api/v1/recovery/report")
        assert recovery.status_code == 200
        assert recovery.json()["facts_only"] is True

        second = client.post(
            "/api/v1/snapshots?base_version=1",
            content=pretty_json(adjusted_payload),
            headers={"content-type": "application/json"},
        )
        assert second.status_code == 201, second.text
        assert second.json()["current_version"] == 2

        stale = client.post(
            "/api/v1/snapshots?base_version=0",
            content=pretty_json(adjusted_payload),
            headers={"content-type": "application/json"},
        )
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "E_STALE_BASE"

        conflicting = copy.deepcopy(baseline_payload)
        conflicting["components"]["api"]["quota"] = 401
        conflicting = reseal(conflicting)
        duplicate = client.post(
            "/api/v1/snapshots",
            content=pretty_json(conflicting),
            headers={"content-type": "application/json"},
        )
        assert duplicate.status_code == 409
        assert duplicate.json()["error"]["code"] == "E_DUPLICATE_VERSION"

        tampered_text = pretty_json(baseline_payload).replace('"quota": 400', '"quota": 401', 1)
        invalid = client.post(
            "/api/v1/snapshots",
            content=tampered_text,
            headers={"content-type": "application/json"},
        )
        assert invalid.status_code == 422
        assert invalid.json()["error"]["code"] == "E_SNAPSHOT_INVALID"

        scan = client.post("/api/v1/recovery/scan")
        assert scan.status_code == 200
        assert scan.json()["facts_only"] is True
        assert client.get("/api/v1/current").json()["current_version"] == 2


def test_http_wrapper_body_and_reused_response(tmp_path: Path, baseline_payload: dict) -> None:
    app = create_app(tmp_path / "wrapper.sqlite3")
    wrapper = {"base_version": 0, "snapshot": baseline_payload}
    with TestClient(app) as client:
        first = client.post("/api/v1/snapshots", content=pretty_json(wrapper), headers={"content-type": "application/json"})
        assert first.status_code == 201
        replay_wrapper = {"base_version": 1, "snapshot": baseline_payload}
        replay = client.post(
            "/api/v1/snapshots", content=pretty_json(replay_wrapper), headers={"content-type": "application/json"}
        )
        assert replay.status_code == 200
        assert replay.json()["reused"] is True

        missing = client.get("/api/v1/snapshots/99/metrics")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_VERSION_NOT_FOUND"
