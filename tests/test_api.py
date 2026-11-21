from __future__ import annotations

from fastapi.testclient import TestClient

from chunk_engine.api import create_app
from chunk_engine.errors import (
    E_BATCH_NOT_FOUND,
    E_BLOCK_CORRUPT,
    E_KEY_SET_TOO_LARGE,
    E_PAGE_CONSUMED,
    E_SNAPSHOT_INVALID,
)


def test_http_business_errors_pages_and_compare(engine_factory, sample_suite: dict) -> None:
    engine = engine_factory((1,))
    app = create_app(engine)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["snapshot_loaded"] is True

        submitted = client.post(
            "/batches",
            json={
                "keys": [1, 89, 97, 113, 700],
                "columns": ["id", "score", "region", "label"],
                "page_size": 2,
                "consumer_id": "api-submit",
            },
        )
        assert submitted.status_code == 200
        payload = submitted.json()
        handle = payload["result_handle"]
        assert payload["batch"]["total_rows"] == 5
        assert [row["key"] for row in payload["page"]["rows"]] == [1, 89]

        page_one = client.get(f"/results/{handle}/pages/1", params={"consumer_id": "reader-1"})
        assert page_one.status_code == 200
        consumed = client.get(f"/results/{handle}/pages/1", params={"consumer_id": "reader-2"})
        assert consumed.status_code == 409
        assert consumed.json()["error"]["code"] == E_PAGE_CONSUMED

        missing = client.get("/batches/rh_missing")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == E_BATCH_NOT_FOUND

        too_many = client.post(
            "/batches",
            json={"keys": list(range(1, 1002)), "columns": ["id"], "page_size": 2},
        )
        assert too_many.status_code == 413
        assert too_many.json()["error"]["code"] == E_KEY_SET_TOO_LARGE

        invalid = client.post("/snapshots/load", json={"path": "/definitely/not/a/snapshot"})
        assert invalid.status_code == 400
        assert invalid.json()["error"]["code"] == E_SNAPSHOT_INVALID
        assert client.get("/snapshots/active").json()["snapshot"]["version"] == 1

        comparison = client.post(
            "/compare",
            json={
                "keys": [1, 89, 97, 113, 700],
                "columns": ["id", "score", "region", "label"],
                "page_size": 2,
            },
        )
        assert comparison.status_code == 200
        assert comparison.json()["bitwise_equal"] is True

        loaded = client.post("/snapshots/load", json={"path": sample_suite["v3_corrupt"]["root"]})
        assert loaded.status_code == 200
        corrupt_key = sample_suite["queries"]["corrupt_key"]
        corrupt = client.post(
            "/batches",
            json={"keys": [corrupt_key], "columns": ["id", "label"], "page_size": 2},
        )
        assert corrupt.status_code == 400
        error = corrupt.json()["error"]
        assert error["code"] == E_BLOCK_CORRUPT
        assert error["context"]["block_no"] == 1


def test_debug_kill_hook_is_disabled_by_default(engine_factory) -> None:
    engine = engine_factory((1,))
    app = create_app(engine, allow_debug_kill=False)
    with TestClient(app) as client:
        response = client.post(
            "/batches",
            json={
                "keys": [1],
                "columns": ["id"],
                "page_size": 1,
                "debug_kill_after_committed_blocks": 1,
            },
        )
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "E_INVALID_REQUEST"
