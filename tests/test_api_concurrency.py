from __future__ import annotations

import concurrent.futures
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.engine import Engine
from app.main import create_app
from app.models import WindowRecord
from app.schemas import BatchRequest


def batch_payload(batch_id: str = "api-1", measure=5, kind="integer", scale=0):
    measure_type = {"kind": kind}
    if kind == "decimal":
        measure_type["scale"] = scale
    return {
        "batch_id": batch_id,
        "window_spec": {
            "kind": "tumbling",
            "width_ms": 1000,
            "boundary": "left_closed_right_open",
        },
        "measure_type": measure_type,
        "events": [
            {
                "event_id": f"{batch_id}-e1",
                "timestamp_ms": 100,
                "group_key": "g",
                "measure": measure,
                "labels": {"via": "http"},
            }
        ],
        "timestamp_order": "sort",
        "late_policy": "queue",
    }


def test_http_errors_are_uniform_and_include_context(tmp_path: Path) -> None:
    app = create_app(f"sqlite:///{tmp_path / 'api.db'}")
    with TestClient(app) as client:
        index = client.get("/")
        assert index.status_code == 200
        assert "SQLite 流式窗口聚合工作台" in index.text

        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["sqlite_wal"] is True

        first = client.post("/api/batches", json=batch_payload("api-1"))
        assert first.status_code == 200, first.text
        version = first.json()["state_version"]

        mismatch_payload = batch_payload("api-mismatch")
        mismatch_payload["measure_type"] = {"kind": "integer"}
        mismatch_payload["events"][0]["measure"] = "1.25"
        mismatch = client.post("/api/batches", json=mismatch_payload)
        assert mismatch.status_code == 422
        body = mismatch.json()
        assert body["code"] == "E_MEASURE_TYPE_MISMATCH"
        assert body["context"]["batch_id"] == "api-mismatch"
        assert client.get("/api/status").json()["state_version"] == version

        conflict_payload = batch_payload("api-1", measure=99)
        conflict = client.post("/api/batches", json=conflict_payload)
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "E_CONFLICTING_BATCH"
        assert conflict.json()["context"]["batch_id"] == "api-1"

        deduplicated = client.post("/api/batches", json=batch_payload("api-1"))
        assert deduplicated.status_code == 200
        assert deduplicated.json()["deduplicated"] is True
        assert client.get("/api/status").json()["state_version"] == version

        windows = client.get("/api/windows", params={"include_type_history": "true"}).json()
        assert windows["window_count"] == 1
        assert windows["windows"][0]["sum"] == 5

        snapshot = client.post("/api/snapshots", json={}).json()
        snapshot_body = client.get(f"/api/snapshots/{snapshot['version']}").json()
        assert snapshot_body["checksum"] == snapshot["checksum"]
        bad = dict(snapshot_body)
        bad["checksum"] = "sha256:broken"
        invalid = client.post("/api/snapshots/load", json={"snapshot": bad})
        assert invalid.status_code == 422
        assert invalid.json()["code"] == "E_SNAPSHOT_INVALID"

        validation = client.post("/api/batches", json={"batch_id": "broken"})
        assert validation.status_code == 422
        assert validation.json()["code"] == "E_INVALID_REQUEST"
        assert "errors" in validation.json()["context"]

        samples = client.get("/api/samples").json()["samples"]
        assert any(item["name"] == "events_integer.json" for item in samples)
        sample = client.get("/api/samples/events_integer.json").json()
        assert sample["measure_type"] == {"kind": "integer"}


def test_same_batch_concurrent_submissions_apply_once(tmp_path: Path) -> None:
    engine = Engine(f"sqlite:///{tmp_path / 'concurrent.db'}")
    try:
        request = BatchRequest.model_validate(batch_payload("parallel-batch"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda _: engine.ingest_batch(request), range(8)))
        assert sum(1 for item in responses if item["deduplicated"] is False) == 1
        assert sum(1 for item in responses if item["deduplicated"] is True) == 7
        assert engine.status()["state_version"] == 1
        assert engine.query_windows()["windows"][0]["count"] == 1
    finally:
        engine.close()


def test_read_transaction_keeps_one_committed_snapshot(tmp_path: Path) -> None:
    engine = Engine(f"sqlite:///{tmp_path / 'snapshot-read.db'}")
    try:
        first = BatchRequest.model_validate(batch_payload("read-1"))
        second = BatchRequest.model_validate(
            batch_payload("read-2", measure="2.50", kind="decimal", scale=2)
        )
        engine.ingest_batch(first)

        with engine.db.read_session() as session:
            version_before = engine._state_version(session)
            windows_before = [
                (row.window_id, row.aggregate_json, row.version)
                for row in session.scalars(select(WindowRecord).order_by(WindowRecord.window_id))
            ]
            assert version_before == 1

            # A write from another thread commits while this read transaction
            # remains pinned to the first WAL snapshot.
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(engine.ingest_batch, second)
                assert future.result()["state_version"] == 2

            version_after = engine._state_version(session)
            windows_after = [
                (row.window_id, row.aggregate_json, row.version)
                for row in session.scalars(select(WindowRecord).order_by(WindowRecord.window_id))
            ]
        assert version_after == version_before
        assert windows_after == windows_before

        fresh = engine.query_windows()
        assert fresh["state_version"] == 2
        assert fresh["windows"][0]["sum"] == "7.50"
    finally:
        engine.close()
