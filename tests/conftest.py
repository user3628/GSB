import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from graphstore.engine import GraphEngine  # noqa: E402

SAMPLES = ROOT / "samples"


@pytest.fixture
def engine(tmp_path):
    eng = GraphEngine(str(tmp_path / "graph.db"))
    report = eng.start()
    assert report["last_complete_version"] is None
    eng.load_snapshot(str(SAMPLES / "snapshot_v1.json"))
    yield eng
    eng.close()


@pytest.fixture
def sample_path():
    return SAMPLES
