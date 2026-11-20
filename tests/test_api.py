from __future__ import annotations

import hashlib


def digest(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


def test_api_full_flow_and_typed_errors(client):
    health = client.get("/health")
    assert health.status_code == 200

    bad_block = client.post(
        "/api/volumes/vol0/blocks/write",
        json={"block_no": 99, "content": "x", "sha256": digest("x"), "request_id": "a1"},
    )
    assert bad_block.status_code == 404
    assert bad_block.json()["code"] == "E_BLOCK_NOT_FOUND"

    negative_block = client.post(
        "/api/volumes/vol0/blocks/write",
        json={"block_no": -1, "content": "x", "sha256": digest("x"), "request_id": "a1-negative"},
    )
    assert negative_block.status_code == 404
    assert negative_block.json()["code"] == "E_BLOCK_NOT_FOUND"

    bad_digest = client.post(
        "/api/volumes/vol0/blocks/write",
        json={"block_no": 0, "content": "x", "sha256": "0" * 64, "request_id": "a2"},
    )
    assert bad_digest.status_code == 409
    assert bad_digest.json()["code"] == "E_BLOCK_CORRUPT"

    written = client.post(
        "/api/volumes/vol0/blocks/write",
        json={"block_no": 0, "content": "v2", "sha256": digest("v2"), "request_id": "a3"},
    )
    assert written.status_code == 200
    assert written.json()["version"] == 2

    full = client.post(
        "/api/volumes/vol0/snapshots/full",
        json={"snapshot_id": "s0", "request_id": "a4"},
    )
    assert full.status_code == 200
    assert full.json()["snapshot"]["ref_count"] == 4

    written2 = client.post(
        "/api/volumes/vol0/blocks/write",
        json={"block_no": 1, "content": "v2b", "sha256": digest("v2b"), "request_id": "a5"},
    )
    assert written2.json()["cow_performed"] is True

    incremental = client.post(
        "/api/volumes/vol0/snapshots/incremental",
        json={"snapshot_id": "s1", "parent_id": "s0", "request_id": "a6"},
    )
    assert incremental.status_code == 200
    assert incremental.json()["snapshot"]["ref_count"] == 1

    diff = client.get("/api/volumes/vol0/snapshots/s0/diff")
    assert diff.status_code == 200
    assert diff.json()["write_back_blocks"] == 1

    chain = client.get("/api/volumes/vol0/chain")
    assert chain.json()["chain_depth"] == 2
    assert chain.json()["reference_block_count"] == 5

    blocked_delete = client.post(
        "/api/volumes/vol0/snapshots/s0/delete", json={"request_id": "a7"}
    )
    assert blocked_delete.status_code == 409
    assert blocked_delete.json()["code"] == "E_SNAPSHOT_REFERENCED"

    rollback = client.post(
        "/api/volumes/vol0/snapshots/s0/rollback", json={"request_id": "a8"}
    )
    assert rollback.status_code == 200
    assert rollback.json()["write_back_blocks"] == 1

    report = client.get("/api/volumes/vol0/recovery-report")
    assert report.status_code == 200
    assert report.json()["status"] == "CONSISTENT"
    assert report.json()["durable_only"] is True


def test_api_validation_and_conflicting_request_are_typed(client):
    invalid = client.post("/api/volumes/vol0/blocks/write", json={"block_no": "x"})
    assert invalid.status_code == 422
    assert invalid.json()["code"] == "E_INVALID_REQUEST"

    body = {"block_no": 0, "content": "first", "sha256": digest("first"), "request_id": "same"}
    assert client.post("/api/volumes/vol0/blocks/write", json=body).status_code == 200
    body["content"] = "second"
    body["sha256"] = digest("second")
    conflict = client.post("/api/volumes/vol0/blocks/write", json=body)
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "E_CONFLICTING_REQUEST"

    missing_route = client.get("/api/does-not-exist")
    assert missing_route.status_code == 404
    assert missing_route.json()["code"] == "E_ROUTE_NOT_FOUND"


def test_api_merge_and_delete(client):
    client.post(
        "/api/volumes/vol0/snapshots/full",
        json={"snapshot_id": "root", "request_id": "c1"},
    )
    client.post(
        "/api/volumes/vol0/blocks/write",
        json={"block_no": 0, "content": "child", "sha256": digest("child"), "request_id": "w1"},
    )
    client.post(
        "/api/volumes/vol0/snapshots/incremental",
        json={"snapshot_id": "child", "parent_id": "root", "request_id": "c2"},
    )
    merged = client.post(
        "/api/volumes/vol0/snapshots/child/merge",
        json={"target_snapshot_id": "root", "request_id": "m1"},
    )
    assert merged.status_code == 200
    assert merged.json()["source_state_after"] == "MERGED"
    assert merged.json()["chain_depth_before"] == 2
    assert merged.json()["chain_depth_after"] == 1

    deleted = client.post(
        "/api/volumes/vol0/snapshots/root/delete", json={"request_id": "d1"}
    )
    assert deleted.status_code == 200
    assert deleted.json()["state"] == "DELETED"

    replay = client.post(
        "/api/volumes/vol0/snapshots/root/delete", json={"request_id": "d1"}
    )
    assert replay.status_code == 200
    assert replay.json() == deleted.json()

