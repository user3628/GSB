from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from transcript_workbench.http_api import create_app
from transcript_workbench.workspace import Workspace


def _client(root) -> TestClient:
    return TestClient(create_app(root))


def test_http_import_versions_exports_and_errors(demo_data, tmp_path):
    root = tmp_path / "http-workspace"
    client = _client(root)
    assert client.get("/api/health").status_code == 200
    imported = client.post(
        "/api/workspaces/import",
        json={"manifest_path": str(demo_data["manifest_path"])},
    )
    assert imported.status_code == 200
    assert imported.json()["version"] == 1
    versions = client.get("/api/versions").json()
    assert versions["latest_version"] == 1

    state = Workspace(root).load_state()
    word = min(
        (word for word in state.words if word.chunk_id == "chunk-01"),
        key=lambda item: (item.start_ms, item.entry_id),
    )
    edited = client.post(
        "/api/edits",
        json={
            "base_version": 1,
            "operations": [
                {"op": "update_word", "entry_id": word.entry_id, "text": word.text + "H"}
            ],
        },
    )
    assert edited.status_code == 200
    assert edited.json()["version"] == 2
    assert client.get("/api/versions/1/diff/2").json()["counts"]["modified"] == 1
    for fmt in ("json", "srt", "vtt"):
        exported = client.post(
            "/api/exports", json={"version": 2, "format": fmt, "chunk_id": "chunk-01"}
        )
        assert exported.status_code == 200
        assert exported.json()["word_count"] > 0

    roundtrip = client.post("/api/roundtrip", json={"version": 2})
    assert roundtrip.status_code == 200
    assert roundtrip.json()["all_exact"] is True

    stale = client.post(
        "/api/edits",
        json={
            "base_version": 1,
            "operations": [
                {"op": "update_word", "entry_id": word.entry_id, "text": "stale"}
            ],
        },
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STALE_VERSION"

    legacy = demo_data["legacy_path"].read_text(encoding="utf-8")
    migrated = client.post(
        "/api/imports",
        json={
            "base_version": 2,
            "format": "json",
            "content": legacy,
            "chunk_id": "chunk-01",
        },
    )
    assert migrated.status_code == 200
    assert migrated.json()["version"] == 3
    assert migrated.json()["kind"] == "migration"

    invalid = client.post(
        "/api/imports",
        json={
            "base_version": 3,
            "format": "json",
            "content": demo_data["invalid_path"].read_text(encoding="utf-8"),
            "chunk_id": "chunk-01",
        },
    )
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "E_TIMELINE_ERROR"
    assert invalid.json()["error"]["details"]["chunk_id"] == "chunk-01"

    stopped = client.post("/api/stop")
    assert stopped.status_code == 200
    rejected = client.post("/api/edits", json={"base_version": 3, "operations": []})
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "E_STOPPING"
    recovered = client.post("/api/recover")
    assert recovered.status_code == 200
    assert recovered.json()["version"] == 3


def test_http_merge_and_fork_are_available(boot_workspace):
    client = _client(boot_workspace.root)
    state = boot_workspace.load_state()
    word = min(
        (item for item in state.words if item.chunk_id == "chunk-02"),
        key=lambda item: (item.start_ms, item.entry_id),
    )
    left = client.post(
        "/api/edits",
        json={
            "base_version": 1,
            "operations": [
                {"op": "update_word", "entry_id": word.entry_id, "text": word.text + "L"}
            ],
        },
    )
    assert left.status_code == 200
    right = client.post(
        "/api/forks",
        json={
            "base_version": 2,
            "parent_version": 1,
            "operations": [
                {"op": "update_word", "entry_id": word.entry_id, "text": word.text + "R"}
            ],
        },
    )
    assert right.status_code == 200
    merged = client.post(
        "/api/merge",
        json={
            "base_version": 3,
            "left_version": 2,
            "right_version": 3,
            "strategy": "mark-conflict",
        },
    )
    assert merged.status_code == 200
    assert merged.json()["merge"]["conflict_count"] == 1
    assert merged.json()["merge"]["conflicts"][0]["left"]["text"].endswith("L")
    assert merged.json()["merge"]["conflicts"][0]["right"]["text"].endswith("R")


def test_http_validation_errors_are_business_shaped(boot_workspace):
    client = _client(boot_workspace.root)
    response = client.post(
        "/api/edits", json={"base_version": "not-an-int", "operations": []}
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "E_INVALID_REQUEST"
