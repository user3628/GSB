from __future__ import annotations

from pathlib import Path

import pytest

from qiu.engine import QueryEngine

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def data_paths() -> dict[str, Path]:
    return {
        "config": ROOT / "data" / "config.json",
        "dictionary": ROOT / "data" / "dictionary.json",
        "eval": ROOT / "data" / "eval.jsonl",
        "training": ROOT / "data" / "training.jsonl",
    }


@pytest.fixture()
def engine(data_paths: dict[str, Path]) -> QueryEngine:
    return QueryEngine(
        config_path=data_paths["config"],
        training_path=data_paths["training"],
        dictionary_path=data_paths["dictionary"],
    )


@pytest.fixture()
def sample() -> dict[str, object]:
    return {
        "channel": "map",
        "history_categories": [],
        "locale": "zh-CN",
        "query_id": "test-query-1",
        "raw_query": "北京咖啡店",
    }
