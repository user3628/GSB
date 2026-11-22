from __future__ import annotations

import copy
import json
import math
from pathlib import Path

import pytest

from dep_engine import AnalysisConfig, parse_sentence
from dep_engine.canonical import canonical_bytes
from dep_engine.generator import DEMO_CONFIG
from dep_engine.validation import validate_sentence


def config(**updates):
    raw = copy.deepcopy(DEMO_CONFIG)
    raw.update(updates)
    return AnalysisConfig.from_mapping(raw)


def test_parser_is_byte_deterministic_and_well_formed():
    cfg = config()
    kwargs = dict(
        text="我们 在 本地 运行 依存 分析 引擎 。",
        sentence_id="shard-0000:sentence-0001",
        shard_index=0,
        sentence_index=1,
        config=cfg,
    )
    first = parse_sentence(**kwargs)
    second = parse_sentence(**kwargs)
    assert canonical_bytes(first) == canonical_bytes(second)
    assert first["tokens"] == ["我们", "在", "本地", "运行", "依存", "分析", "引擎"]
    assert len(first["heads"]) == len(first["labels"]) == len(first["distances"]) == 7
    assert first["heads"].count(-1) == 1
    assert first["labels"][first["heads"].index(-1)] == "ROOT"
    for index, head in enumerate(first["heads"]):
        expected_distance = abs(index - head) if head >= 0 else 0
        assert first["distances"][index] == expected_distance


def test_empty_and_punctuation_only_sentence_is_diagnosed():
    result = parse_sentence(
        text="。！？ ...",
        sentence_id="shard-0002:sentence-0007",
        shard_index=2,
        sentence_index=7,
        config=config(),
    )
    assert result["status"] == "error"
    assert result["error"]["code"] == "E_EMPTY_SENTENCE"
    assert result["sentence_id"] == "shard-0002:sentence-0007"
    assert result["error"]["details"]["token_index"] == 0


@pytest.mark.parametrize(
    ("mutation", "source"),
    [
        (lambda raw: raw["score_weights"].__setitem__("shift", float("nan")), "score_weights.shift"),
        (lambda raw: raw.__setitem__("smoothing_strength", float("inf")), "smoothing_strength"),
        (lambda raw: raw.__setitem__("confidence_threshold", float("nan")), "confidence_threshold"),
    ],
)
def test_numeric_failure_reports_source_and_position(mutation, source):
    raw = copy.deepcopy(DEMO_CONFIG)
    mutation(raw)
    result = parse_sentence(
        text="数值 诊断 不能 静默 替换 。",
        sentence_id="shard-0000:sentence-0007",
        shard_index=0,
        sentence_index=7,
        config=AnalysisConfig.from_mapping(raw),
    )
    assert result["status"] == "error"
    assert result["error"]["code"] == "E_NUMERIC"
    details = result["error"]["details"]
    assert details["score_source"] == source
    assert details["token_index"] == 0
    assert "action_step" in details
    assert "buffer_position" in details
    assert "stack_size" in details


def test_validation_detects_cycle_multiple_roots_and_distance():
    cfg = config(max_dependency_distance=1)
    base = {
        "shard_index": 0,
        "sentence_index": 1,
        "sentence_id": "s1",
        "tokens": ["a", "b", "c"],
        "labels": ["LEFT_DEP", "LEFT_DEP", "LEFT_DEP"],
        "distances": [1, 1, 1],
        "status": "ok",
        "error": None,
    }
    cycle = validate_sentence({**base, "heads": [1, 2, 0]}, cfg)
    types = {violation["type"] for violation in cycle["violations"]}
    assert cycle["code"] == "E_TREE_INVALID"
    assert "CYCLE" in types
    assert "NO_ROOT" in types

    roots = validate_sentence({**base, "heads": [-1, -1, 2]}, cfg)
    assert roots["code"] == "E_TREE_INVALID"
    assert "MULTIPLE_ROOTS" in {v["type"] for v in roots["violations"]}

    distance = validate_sentence(
        {**base, "heads": [2, 0, -1], "distances": [2, 1, 0]}, cfg
    )
    assert distance["code"] == "E_TREE_INVALID"
    max_distance = [
        v for v in distance["violations"] if v["type"] == "MAX_DEPENDENCY_DISTANCE"
    ]
    assert max_distance
    assert max_distance[0]["token_index"] == 0
    assert max_distance[0]["limit"] == 1
