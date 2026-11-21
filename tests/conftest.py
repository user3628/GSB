from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any, Callable

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.main import create_app
from app.models import FullSnapshotInput, IncrementalSnapshotInput
from app.store import Store


def load_sample(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "samples" / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "workbench.db"


@pytest.fixture
def store(db_path: Path) -> Store:
    value = Store(db_path)
    value.initialize()
    return value


@pytest.fixture
def client(db_path: Path) -> TestClient:
    app = create_app(db_path)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def sample() -> Callable[[str], dict[str, Any]]:
    return load_sample


def submit_v1(store: Store) -> dict[str, Any]:
    payload = load_sample("snapshot_v1")
    return store.submit_full(FullSnapshotInput.model_validate(payload))


def submit_v2(store: Store) -> dict[str, Any]:
    payload = load_sample("snapshot_v2")
    return store.submit_full(FullSnapshotInput.model_validate(payload))


def increment_payload(commit_id: str = "test-increment", *, digest_char: str = "a") -> dict[str, Any]:
    payload = copy.deepcopy(load_sample("increment_image"))
    payload["commit_id"] = commit_id
    payload["units"][0]["image_digest"] = "sha256:" + digest_char * 64
    return payload


def submit_increment(store: Store, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    model = IncrementalSnapshotInput.model_validate(payload or load_sample("increment_image"))
    return store.submit_incremental(model)
