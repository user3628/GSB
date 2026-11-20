from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from qiu.engine import QueryEngine
from qiu.errors import QueryError
from qiu.io import canonical_json, pretty_json


def test_error_codes_and_stages_are_consistent(engine: QueryEngine, sample: dict[str, object]) -> None:
    cases = [
        ("E_INVALID_LOCALE", "validation", {**sample, "locale": "xx-XX"}, {}),
        ("E_EMPTY_QUERY", "normalization", {**sample, "raw_query": "？！。"}, {}),
        ("E_QUERY_TOO_LONG", "normalization", sample, {"force_query_length": 999}),
        ("E_NUMERIC", "intent", sample, {"nan_confidence": True}),
        ("E_DICT_VERSION", "slots", sample, {"dict_version": "wrong"}),
        ("E_DICT_MISSING", "slots", sample, {"missing_dictionary": True}),
    ]
    for code, stage, bad_sample, faults in cases:
        with pytest.raises(QueryError) as caught:
            engine.parse(bad_sample, faults)
        assert caught.value.code == code
        assert caught.value.stage == stage
        assert caught.value.query_id == sample["query_id"]
        payload = canonical_json(caught.value.to_dict())
        assert '"ok":false' in payload
        assert '"code":"' + code + '"' in payload


def _copy_assets(data_paths: dict[str, Path], tmp_path: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for key in ("config", "training", "dictionary"):
        target = tmp_path / data_paths[key].name
        shutil.copyfile(data_paths[key], target)
        result[key] = target
    return result


def _new_engine(paths: dict[str, Path]) -> QueryEngine:
    return QueryEngine(
        config_path=paths["config"],
        training_path=paths["training"],
        dictionary_path=paths["dictionary"],
    )


def test_policy_change_invalidates_only_decision_and_never_returns_stale_result(data_paths: dict[str, Path], tmp_path: Path) -> None:
    paths = _copy_assets(data_paths, tmp_path)
    engine = _new_engine(paths)
    sample = {
        "channel": "shop",
        "history_categories": [],
        "locale": "zh-CN",
        "query_id": "cache-policy",
        "raw_query": "淘宝下单",
    }
    first = engine.parse(sample)
    second = engine.parse(sample)
    assert first["cache"]["status"] == "miss"
    assert second["cache"]["status"] == "hit"
    assert first["intent"]["source"] == "primary_classifier"

    config = json.loads(paths["config"].read_text(encoding="utf-8"))
    config["policy"]["confidence_threshold"] = 0.95
    paths["config"].write_text(pretty_json(config), encoding="utf-8")
    changed = engine.parse(sample)
    assert changed["cache"]["reused_stages"] == ["normalization", "intent", "slots"]
    assert changed["cache"]["invalidated_stages"] == ["decision"]
    assert changed["intent"]["source"] == "channel_default"
    assert changed["intent"]["label"] == "transactional"
    assert changed["version_fingerprints"]["decision"] != first["version_fingerprints"]["decision"]


def test_dictionary_and_training_changes_reuse_unaffected_stages(data_paths: dict[str, Path], tmp_path: Path) -> None:
    paths = _copy_assets(data_paths, tmp_path)
    engine = _new_engine(paths)
    sample = {
        "channel": "map",
        "history_categories": [],
        "locale": "zh-CN",
        "query_id": "cache-assets",
        "raw_query": "北京咖啡店",
    }
    engine.parse(sample)

    dictionary = json.loads(paths["dictionary"].read_text(encoding="utf-8"))
    dictionary["entries"].append({"canonical": "新词", "term": "新词", "type": "brand"})
    paths["dictionary"].write_text(pretty_json(dictionary), encoding="utf-8")
    dictionary_changed = engine.parse(sample)
    assert dictionary_changed["cache"]["reused_stages"] == ["normalization", "intent"]
    assert dictionary_changed["cache"]["invalidated_stages"] == ["slots", "decision"]

    with paths["training"].open("a", encoding="utf-8") as handle:
        handle.write(
            canonical_json(
                {
                    "channel": "app",
                    "history_categories": [],
                    "intent": "navigation",
                    "locale": "zh-CN",
                    "query_id": "train-extra",
                    "raw_query": "华为官网",
                }
            )
            + "\n"
        )
    training_changed = engine.parse(sample)
    assert training_changed["cache"]["reused_stages"] == ["normalization", "slots"]
    assert training_changed["cache"]["invalidated_stages"] == ["intent", "decision"]


def test_missing_dictionary_file_raises_dict_missing(data_paths: dict[str, Path], tmp_path: Path) -> None:
    paths = _copy_assets(data_paths, tmp_path)
    paths["dictionary"].unlink()
    engine = _new_engine(paths)
    with pytest.raises(QueryError) as caught:
        engine.parse(
            {
                "channel": "map",
                "history_categories": [],
                "locale": "zh-CN",
                "query_id": "missing-file",
                "raw_query": "北京咖啡店",
            }
        )
    assert caught.value.code == "E_DICT_MISSING"
