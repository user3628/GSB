import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config_dispatch.coordinator import Coordinator
from config_dispatch.endpoint_server import EndpointCluster
from config_dispatch.models import compute_checksum
from config_dispatch.push_client import PushClient
from config_dispatch.repository import Repository

SAMPLES = ROOT / "samples"


@pytest.fixture()
def repo(tmp_path):
    repository = Repository(str(tmp_path / "test.db"))
    yield repository
    repository.close()


@pytest.fixture()
def coordinator(repo):
    return Coordinator(repo, PushClient(timeout_seconds=1.0))


@pytest.fixture()
def declarations_raw():
    return json.loads((SAMPLES / "components.json").read_text(encoding="utf-8"))


@pytest.fixture()
def snapshot_payload():
    def _make(version: int, items=None, checksum=None, generated_at=None):
        items = items if items is not None else {"queue.name": f"q-{version}", "log.level": "INFO"}
        return {
            "version": version,
            "generated_at": generated_at or f"2026-11-20T0{version}:00:00+08:00",
            "config_items": items,
            "checksum": checksum or compute_checksum(items),
        }

    return _make


@pytest.fixture()
def success_script(declarations_raw):
    return {
        item["component_id"]: {
            "/apply": {"behavior": "success"},
            "/rollback": {"behavior": "success"},
        }
        for item in declarations_raw
    }


@pytest.fixture()
def cluster_factory():
    clusters = []

    def _start(declarations_raw, script):
        declarations = {item["component_id"]: item for item in declarations_raw}
        cluster = EndpointCluster(declarations, script)
        cluster.start()
        clusters.append(cluster)
        return cluster

    yield _start
    for cluster in clusters:
        cluster.stop()
