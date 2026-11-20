from __future__ import annotations

from fastapi.testclient import TestClient

from inverted_index.api import create_app
from inverted_index.storage import IndexStore


def test_app_constructs_store_from_configuration(tmp_path, index_fields) -> None:
    app = create_app(db_path=tmp_path / "configured.db", index_fields=index_fields)
    with TestClient(app) as client:
        assert client.get("/health").json()["current_version"] == 0
        fields = client.get("/v1/index/fields").json()["index_fields"]
        assert fields == sorted(index_fields)


def test_http_api_end_to_end(tmp_path, index_fields, sample_batches) -> None:
    store = IndexStore(tmp_path / "index.db", index_fields)
    app = create_app(store=store)
    initial, incremental = sample_batches
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["current_version"] == 0
        fields = client.get("/v1/index/fields")
        assert fields.status_code == 200
        assert fields.json()["index_fields"] == sorted(index_fields)

        response = client.post("/v1/batches", json=initial)
        assert response.status_code == 201
        first = response.json()["version"]
        response = client.post("/v1/search", json={
            "version": first,
            "operator": "AND",
            "clauses": [{"kind": "term", "field": "title", "term": "quantum"}],
        })
        assert response.status_code == 200
        assert response.json()["total"] == 0

        response = client.post("/v1/batches", json=incremental)
        assert response.status_code == 201
        second = response.json()["version"]
        old = client.post("/v1/search", json={
            "version": first,
            "clauses": [{"kind": "term", "field": "title", "term": "quantum"}],
        }).json()
        new = client.post("/v1/search", json={
            "version": second,
            "clauses": [{"kind": "term", "field": "title", "term": "quantum"}],
        }).json()
        assert old["total"] == 0
        assert {item["_id"] for item in new["items"]} == {"doc-002", "doc-013"}

        postings = client.get(
            "/v1/postings", params={"path": "tags", "term": "MIGRATED", "version": second}
        )
        assert postings.status_code == 200
        assert postings.json()["term"] == "migrated"
        assert postings.json()["entries"][0]["doc_id"] == "doc-007"

        segments = client.get("/v1/index/segments").json()["segments"]
        active = [segment for segment in segments if segment["status"] == "active"]
        assert len(active) == 2
        merge = client.post(
            "/v1/index/merge",
            json={"source_segment_ids": [segment["segment_id"] for segment in active]},
        )
        assert merge.status_code == 200
        assert client.post("/v1/search", json={
            "version": first,
            "clauses": [{"kind": "term", "field": "title", "term": "quantum"}],
        }).json()["total"] == 0

        report = client.get("/v1/recovery/report")
        assert report.status_code == 200
        assert "pending_operations_found" in report.json()

        bad_path = client.post("/v1/search", json={
            "clauses": [{"kind": "term", "field": "bad[path]", "term": "x"}],
        })
        assert bad_path.status_code == 400
        assert bad_path.json()["error"]["code"] == "E_BAD_PATH"

        unknown_path = client.post("/v1/search", json={
            "clauses": [{"kind": "term", "field": "not.indexed", "term": "x"}],
        })
        assert unknown_path.status_code == 400
        assert unknown_path.json()["error"]["code"] == "E_UNKNOWN_PATH"

        missing = client.get("/v1/not-a-route")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_NOT_FOUND"
