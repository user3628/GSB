"""Shared fixtures: isolated DB per test, TestClient, sample loader."""
from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture()
def settings(tmp_path):
    db_path = tmp_path / "test_store.db"
    os.environ["SHARD_STORE_DB"] = str(db_path)
    return Settings(db_path=db_path, samples_dir=ROOT / "samples")


@pytest.fixture()
def client(settings):
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def manifest():
    return json.loads((ROOT / "samples" / "manifest.json").read_text())


def sample_part(manifest: dict, object_index: int, part_number: int) -> bytes:
    entry = manifest["objects"][object_index]["parts"][part_number - 1]
    return (ROOT / entry["file"]).read_bytes()


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def create_session(client, **overrides):
    payload = {
        "object_key": "test/obj",
        "min_part_size": 1,
        "max_part_size": 10 * 1024 * 1024,
        "merge_strategy": "fast",
        "conflict_policy": "reject",
    }
    payload.update(overrides)
    r = client.post("/api/v1/sessions", json=payload)
    assert r.status_code == 200, r.text
    return r.json()["data"]


def upload_part(client, session_id: str, part_number: int, content: bytes,
                sha256: str | None = None):
    payload = {"content_base64": b64(content)}
    if sha256:
        payload["sha256"] = sha256
    return client.post(
        f"/api/v1/sessions/{session_id}/parts/{part_number}", json=payload
    )
