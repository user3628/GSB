from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path

from fastapi.testclient import TestClient

from upload_index.api import create_app
from upload_index.service import UploadIndexService

from conftest import part


def test_http_error_contract_and_object_view(tmp_path: Path):
    app = create_app(db_path=str(tmp_path / "api.db"), enable_crash_drill=False)
    with TestClient(app) as client:
        session = client.post(
            "/sessions", json={"object_key": "sync/demo/object.bin", "session_id": "api-session"}
        ).json()["data"]
        invalid = client.post(
            "/sessions/api-session/parts",
            json={"seq": 0, "size": 1, "sha256": "0" * 64, "arrival_ms": 1},
        )
        assert invalid.status_code == 400
        assert invalid.json()["ok"] is False
        assert invalid.json()["error"]["code"] == "E_PART_INVALID"

        committed = client.post(
            "/sessions/api-session/parts", json=part(1, "api-content", size=11)
        ).json()["data"]
        view = client.get(
            f"/sessions/api-session/view?version={committed['current_version']}"
        ).json()["data"]
        assert view["index"]["contiguous_ranges"] == [[1, 1]]
        merged = client.post(
            "/sessions/api-session/merge",
            json={"generation": 1, "total_size": 11},
        ).json()["data"]
        assert merged["part_count"] == 1
        object_view = client.get(
            "/objects/view", params={"object_key": session["object_key"], "version": 3}
        ).json()["data"]
        assert object_view["object"]["session_id"] == "api-session"

        future = client.get("/sessions/api-session/view?version=9999")
        assert future.status_code == 404
        assert future.json()["error"]["code"] == "E_VERSION_NOT_FOUND"
        unknown = client.get("/does-not-exist")
        assert unknown.status_code == 404
        assert unknown.json()["error"]["code"] == "E_ROUTE_NOT_FOUND"


def test_recovery_scans_pending_intent_and_staging_segment(tmp_path: Path):
    db_path = tmp_path / "recover.db"
    service = UploadIndexService(db_path)
    service.initialize()
    session = service.create_session({"object_key": "objects/recover.bin", "session_id": "recover-1"})
    result = service.submit_batch(
        "recover-1", {"batch_id": "committed", "parts": [part(1, "alpha", size=10)]}
    )
    last_version = result["current_version"]
    service.shutdown()
    service.close()

    conn = sqlite3.connect(db_path)
    now = int(time.time() * 1000)
    intent_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO recovery_intents(intent_id, kind, status, session_id, object_key, details_json, "
        "created_ms, updated_ms, result_json) VALUES (?, 'batch', 'pending', ?, NULL, ?, ?, ?, NULL)",
        (
            intent_id,
            "recover-1",
            json.dumps({"batch_id": "interrupted", "part_count": 2}),
            now,
            now,
        ),
    )
    event = {
        "version": last_version + 1,
        "operation": "part_committed",
        "session_id": "recover-1",
        "object_key": "objects/recover.bin",
        "created_ms": now,
        "payload": {"part": part(2, "beta", size=20), "batch_id": "interrupted"},
    }
    conn.execute(
        "INSERT INTO index_segments(session_id, start_version, end_version, events_json, status, "
        "merge_intent_id, created_ms) VALUES ('recover-1', ?, ?, ?, 'staging', ?, ?)",
        (last_version + 1, last_version + 1, json.dumps([event]), intent_id, now),
    )
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'clean_shutdown'")
    conn.commit()
    conn.close()

    recovered = UploadIndexService(db_path)
    recovered.initialize()
    try:
        report = recovered.get_recovery_report()
        assert report["forced_termination_detected"] is True
        assert report["pending_intents_found"] == 1
        assert report["recovered_intents"][0]["decision"] == "aborted_rolled_back"
        assert report["staging_segments_discarded"] == 1
        assert recovered.current_version == last_version
        view = recovered.get_session_view("recover-1")
        assert [item["seq"] for item in view["parts"]] == [1]
        assert recovered.get_session_segments("recover-1")["segment_count"] == 2
    finally:
        recovered.shutdown()
        recovered.close()

