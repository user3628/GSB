from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from app.engine import Engine


@pytest.fixture()
def db_path() -> Path:
    path = Path(tempfile.mkstemp(prefix="streamagg-test-", suffix=".db")[1])
    yield path
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(path) + suffix).unlink()
        except FileNotFoundError:
            pass


@pytest.fixture()
def engine(db_path: Path):
    instance = Engine(f"sqlite:///{db_path}")
    try:
        yield instance
    finally:
        instance.close()
