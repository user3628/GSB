from __future__ import annotations

import json
from pathlib import Path

from kvstore.api import create_app
from kvstore.store import KVStore


def _client(tmp_path: Path):
    store = KVStore(tmp_path / "data")
    app = create_app(store)
    app.config.update(TESTING=True)
    return app.test_client(), store


def test_http_write_read_delete_and_error_shape(tmp_path: Path) -> None:
    client, _ = _client(tmp_path)
    write = client.put("/v1/kv/hello", json={"value": {"x": 1}, "version": 1})
    assert write.status_code == 201
    assert write.get_json()["ok"] is True
    read = client.get("/v1/kv/hello?read_timeout_ms=200")
    assert read.status_code == 200
    assert read.get_json()["value"] == {"x": 1}
    missing = client.get("/v1/kv/missing")
    assert missing.status_code == 404
    assert missing.get_json()["error"]["code"] == "E_KEY_NOT_FOUND"
    deleted = client.delete("/v1/kv/hello")
    assert deleted.get_json()["deleted"] is True


def test_http_exports_and_imports(tmp_path: Path) -> None:
    client, store = _client(tmp_path)
    binary = Path("samples/binary_sample.kvsb").read_bytes()
    imported = client.post(
        "/v1/import/binary",
        data=binary,
        headers={"Content-Type": "application/octet-stream", "X-Batch-Id": "http-binary-1"},
    )
    assert imported.status_code == 200
    assert imported.get_json()["summary"]["committed_records"] == 4

    exported = client.get("/v1/export/binary")
    assert exported.status_code == 200
    assert exported.data.startswith(b"KVSB")
    assert exported.headers["X-Record-Count"] == "4"
    jsonl = client.get("/v1/export/jsonl")
    assert jsonl.status_code == 200
    assert len([line for line in jsonl.data.splitlines() if line]) == 4

    wrong = client.post("/v1/import/binary", data=b'{"key":"x"}\n')
    assert wrong.status_code == 400
    assert wrong.get_json()["error"]["code"] == "E_FORMAT_INVALID"

    negotiated = client.post(
        "/v1/import/negotiate",
        data=Path("samples/jsonl_sample.jsonl").read_bytes(),
        headers={"Content-Type": "application/x-ndjson"},
    )
    assert negotiated.status_code == 200
    assert negotiated.get_json()["dry_run"] is True
    assert negotiated.get_json()["negotiation"]["record_count"] == 7
    assert store.index_summary()["live_keys"] == 4


def test_http_snapshot_expire_recovery_and_unknown_field(tmp_path: Path) -> None:
    client, store = _client(tmp_path)
    client.put("/v1/kv/a", json={"value": 1, "version": 1})
    snapshot = client.post("/v1/snapshot/switch", json={})
    assert snapshot.status_code == 200
    assert snapshot.get_json()["snapshot_seq"] == 1
    index = client.get("/v1/index").get_json()
    assert index["live_keys"] == 1
    recovery = client.get("/v1/recovery/report")
    assert recovery.status_code == 200
    assert recovery.get_json()["report"]["source"] == "persistent_intent_and_segment_header"
    expire = client.post("/v1/expire", json={"limit": 10})
    assert expire.get_json()["expired_count"] == 0
    bad = client.put("/v1/kv/b", json={"value": 1, "unexpected": True})
    assert bad.status_code == 400
    assert bad.get_json()["error"]["code"] == "E_INVALID_ARGUMENT"
    malformed = client.post(
        "/v1/snapshot/switch",
        data=b"{not-json}",
        headers={"Content-Type": "application/json"},
    )
    assert malformed.status_code == 400
    assert malformed.get_json()["ok"] is False


def test_http_batch_conflict_is_unified_error(tmp_path: Path) -> None:
    client, _ = _client(tmp_path)
    payload = b'{"key":"batch","value":1,"version":1,"written_at_us":4100000000000000}\n'
    headers = {"Content-Type": "application/x-ndjson", "X-Batch-Id": "same-http-batch"}
    first = client.post("/v1/import/jsonl", data=payload, headers=headers)
    assert first.status_code == 200
    second = client.post("/v1/import/jsonl", data=payload, headers=headers)
    assert second.status_code == 409
    body = second.get_json()
    assert body["error"]["code"] == "E_CONCURRENT_WRITE"


def test_http_method_error_uses_unified_json(tmp_path: Path) -> None:
    client, _ = _client(tmp_path)
    response = client.post("/health")
    assert response.status_code == 405
    assert response.get_json()["error"]["code"] == "E_METHOD_NOT_ALLOWED"
