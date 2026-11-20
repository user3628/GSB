import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logindex.store import reset_database  # noqa: E402


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "test_logindex.db"
    reset_database(path)
    return path


@pytest.fixture()
def engine(db_path):
    from logindex.engine import LogIndexEngine

    return LogIndexEngine(db_path)


@pytest.fixture()
def sample_lines():
    base = Path(__file__).resolve().parents[1] / "samples"

    def _read(name):
        return (base / name).read_text(encoding="utf-8").splitlines()

    return _read
