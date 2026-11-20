from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.db import Database
from app.service import CowService


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "cow.sqlite3",
        volume_id="vol0",
        block_count=4,
        fault_injection=False,
    )


@pytest.fixture
def crash_settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "cow-crash.sqlite3",
        volume_id="vol0",
        block_count=4,
        fault_injection=True,
    )


def make_service(settings: Settings) -> CowService:
    service = CowService(Database(settings.db_path), settings)
    service.initialize()
    return service


@pytest.fixture
def service(settings: Settings) -> CowService:
    return make_service(settings)


@pytest.fixture
def client(settings: Settings):
    application = create_app(settings)
    with TestClient(application) as test_client:
        yield test_client


def write(service: CowService, block_no: int, content: bytes, request_id: str):
    return service.write_block("vol0", block_no, content, digest(content), request_id)


def full_snapshot(service: CowService, snapshot_id: str, request_id: str):
    return service.create_snapshot("vol0", "full", None, snapshot_id, request_id)


def incremental_snapshot(
    service: CowService,
    snapshot_id: str,
    parent_id: str,
    request_id: str,
):
    return service.create_snapshot(
        "vol0", "incremental", parent_id, snapshot_id, request_id
    )

