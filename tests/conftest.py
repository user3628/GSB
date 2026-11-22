from __future__ import annotations

import pytest

from hotroute.engine import Engine


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "hotroute.db")


@pytest.fixture
def engine(db_path):
    return Engine(db_path)

