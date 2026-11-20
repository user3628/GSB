from __future__ import annotations

import copy
import json
import threading
from pathlib import Path
from typing import Any

import pytest

from shardroute.datastore import SQLiteDataStore
from shardroute.engine import ShardRouteService
from shardroute.server import create_http_server
from shardroute.snapshot import finalize_snapshot_document
from shardroute.util import read_json


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


@pytest.fixture
def snapshot_document() -> dict[str, Any]:
    return read_json(SAMPLES / "snapshot_v1.json")


@pytest.fixture
def patch_document() -> dict[str, Any]:
    return read_json(SAMPLES / "patch_v2_orders_hash.json")


@pytest.fixture
def seed_document() -> dict[str, Any]:
    return read_json(SAMPLES / "seed_data.json")


@pytest.fixture
def service(tmp_path: Path, seed_document: dict[str, Any]) -> ShardRouteService:
    data_dir = tmp_path / "data"
    SQLiteDataStore(data_dir).initialize(seed_document)
    instance = ShardRouteService(
        metadata_db=tmp_path / "metadata.sqlite3",
        data_dir=data_dir,
        snapshot_file=SAMPLES / "snapshot_v1.json",
    )
    try:
        yield instance
    finally:
        instance.close(graceful=True)


@pytest.fixture
def http_service(service: ShardRouteService):
    server = create_http_server(service, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield service, server, base_url
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def finalized(document: dict[str, Any]) -> dict[str, Any]:
    return finalize_snapshot_document(copy.deepcopy(document))


def snapshot_doc_without_checksum(path: Path) -> dict[str, Any]:
    document = read_json(path)
    document.pop("checksum", None)
    return document

