from __future__ import annotations

from pathlib import Path

import pytest

from kvstore.store import KVStore


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


@pytest.fixture
def store(tmp_path: Path) -> KVStore:
    return KVStore(tmp_path / "data")


@pytest.fixture
def sample_binary() -> bytes:
    return (SAMPLES / "binary_sample.kvsb").read_bytes()


@pytest.fixture
def sample_jsonl() -> bytes:
    return (SAMPLES / "jsonl_sample.jsonl").read_bytes()
