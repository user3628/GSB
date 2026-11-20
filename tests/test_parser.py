"""Requirement 1 & 7: deterministic parser, tree validity, numeric diagnostics."""
from __future__ import annotations

import math

import pytest

from depworkbench.errors import E_NUMERIC, E_TREE_INVALID, WorkbenchError
from depworkbench.parser import parse_sentence
from depworkbench.tokenizer import tokenize


def test_tokenizer_handles_mixed_text():
    assert tokenize("hello 世界 world 。") == ["hello", "世", "界", "world"]
    assert tokenize("run123 猫") == ["run123", "猫"]


def test_parse_is_byte_deterministic(config):
    text = "parser 调度 checkpoint 资源 。"
    first = parse_sentence(text, config.model, "s1").to_dict()
    second = parse_sentence(text, config.model, "s1").to_dict()
    assert first == second
    assert first["sentence_id"] == "s1"


def test_every_non_root_token_has_one_head_and_single_root(config, ensure_samples):
    from depworkbench.shards import load_shards
    shards = load_shards(ensure_samples, ensure_samples / "manifest.json")
    for shard in shards:
        for index, sentence in enumerate(shard.sentences):
            parse = parse_sentence(sentence, config.model,
                                   f"{shard.index}-{index}")
            assert len(parse.heads) == len(parse.tokens)
            roots = [i for i, head in enumerate(parse.heads, start=1)
                     if head == 0]
            assert len(roots) == 1
            assert all(head >= 0 for head in parse.heads)
            assert len(parse.arcs) == len(parse.tokens)


def test_distance_limit_violation_raises_tree_invalid(config):
    from dataclasses import replace
    tight = replace(config.model, max_dependency_distance=1)
    with pytest.raises(WorkbenchError) as exc:
        parse_sentence("parser 调度 checkpoint 资源 模型 数据 。",
                       tight, "tight-1")
    assert exc.value.code == E_TREE_INVALID
    assert exc.value.details["sentence_id"] == "tight-1"
    assert exc.value.details["token_index"] >= 1


def test_numeric_injection_raises_numeric_error(config):
    with pytest.raises(WorkbenchError) as exc:
        parse_sentence("parser 调度 checkpoint 资源 。", config.model,
                       "num-1", force_numeric_at=2)
    assert exc.value.code == E_NUMERIC
    assert exc.value.details["token_index"] == 2
    assert exc.value.details["action"] in {"shift", "left", "right"}


def test_overflow_weight_produces_numeric_error(config):
    from dataclasses import replace
    weights = {k: tuple(1.0e309 if i == 0 else w for i, w in enumerate(v))
               for k, v in config.model.weights.items()}
    huge = replace(config.model, weights=weights)
    with pytest.raises(WorkbenchError) as exc:
        parse_sentence("parser 调度 checkpoint 资源 。", huge, "big-1")
    assert exc.value.code == E_NUMERIC


def test_empty_sentence_is_tree_invalid(config):
    with pytest.raises(WorkbenchError) as exc:
        parse_sentence(" ，。 ", config.model, "empty-1")
    assert exc.value.code == E_TREE_INVALID


def test_output_contains_required_fields(config):
    parse = parse_sentence("the cat sits 猫 mat", config.model, "fields-1")
    payload = parse.to_dict()
    for key in ("heads", "relations", "distances", "total_score",
                "tokens", "tags", "arcs", "actions"):
        assert key in payload
    assert isinstance(payload["total_score"], float)
    assert math.isfinite(payload["total_score"])
