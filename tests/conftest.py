from __future__ import annotations

import json
from pathlib import Path

import pytest

from inverted_index.storage import IndexStore


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def index_fields() -> tuple[str, ...]:
    return tuple(json.loads((ROOT / "samples/index_config.json").read_text(encoding="utf-8"))["index_fields"])


@pytest.fixture
def sample_batches() -> tuple[dict, dict]:
    initial = json.loads((ROOT / "samples/batches/001_initial.json").read_text(encoding="utf-8"))
    incremental = json.loads((ROOT / "samples/batches/002_incremental.json").read_text(encoding="utf-8"))
    return initial, incremental


@pytest.fixture
def store(tmp_path: Path, index_fields: tuple[str, ...]) -> IndexStore:
    return IndexStore(tmp_path / "index.db", index_fields)


@pytest.fixture
def loaded_store(store: IndexStore, sample_batches: tuple[dict, dict]) -> tuple[IndexStore, int, int]:
    initial, incremental = sample_batches
    first = store.write_batch(
        inserts=initial["inserts"], batch_id=initial["batch_id"]
    )
    second = store.write_batch(
        inserts=incremental["inserts"],
        updates=incremental["updates"],
        deletes=incremental["deletes"],
        batch_id=incremental["batch_id"],
    )
    return store, first.version, second.version
