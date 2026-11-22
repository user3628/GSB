from __future__ import annotations

import json
from pathlib import Path

import pytest

from rca_pipeline.config import get_settings
from rca_pipeline.service import Pipeline

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "test_pipeline.db"


@pytest.fixture()
def pipeline(db_path):
    return Pipeline(get_settings(db_path))


@pytest.fixture()
def dataset():
    payload = json.loads((DATA_DIR / "sli_samples.json").read_text())
    return payload["batches"]


@pytest.fixture()
def topo_v1():
    return json.loads((DATA_DIR / "topology_snapshot.json").read_text())


@pytest.fixture()
def topo_v2():
    return json.loads((DATA_DIR / "topology_increment.json").read_text())


@pytest.fixture()
def topo_v3():
    return json.loads((DATA_DIR / "topology_snapshot_v3.json").read_text())


def submit_topology_v1(pipeline, topo_v1):
    return pipeline.submit_topology(topo_v1)
