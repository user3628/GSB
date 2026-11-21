from __future__ import annotations

import copy

from rag_orchestrator.engine import Orchestrator
from rag_orchestrator.errors import E_QUERY_INVALID
from rag_orchestrator.normalize import normalize_text, run_normalization, tokenize


def test_nfkc_whitespace_and_tokenization(sample_env) -> None:
    assert normalize_text("  ＲＡＧ　　并行\t检索  ") == "RAG 并行 检索"
    assert tokenize("RAG 并行 retrieval-augmented") == ["rag", "并", "行", "retrieval-augmented"]
    product = run_normalization(
        {"query_id": "fx", "query": "  ＲＡＧ　　并行检索  ", "locale": "zh-CN", "channel": "api"},
        sample_env.config(),
    )
    assert product["normalized_query"] == "RAG 并行检索"
    assert product["subqueries"][0]["source"] == "base"
    assert [item["index"] for item in product["subqueries"]] == list(range(len(product["subqueries"])))
    assert all(item["search_terms"] for item in product["subqueries"])


def test_empty_query_is_invalid_and_leaves_no_checkpoint(sample_env, tmp_path) -> None:
    cfg, docs, _ = sample_env.load()
    workspace = tmp_path / "empty-ws"
    result = Orchestrator(cfg, docs, str(workspace)).run(sample_env.request("q-006"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_QUERY_INVALID
    assert result["error"]["query_id"] == "q-006"
    assert result["error"]["subquery_index"] == 0
    assert Orchestrator(cfg, docs, str(workspace)).store.records("q-006") == []


def test_too_many_subqueries_reports_max_index(sample_env, tmp_path) -> None:
    cfg, docs, _ = sample_env.load()
    result = Orchestrator(cfg, docs, str(tmp_path / "long-ws")).run(sample_env.request("q-007"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_QUERY_INVALID
    assert result["error"]["query_id"] == "q-007"
    assert result["error"]["subquery_index"] == cfg["normalizer"]["max_subqueries"]
    assert result["error"]["details"]["reason"] == "too_many_subqueries"


def test_subquery_without_overlap_is_invalid(sample_env, tmp_path) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["normalizer"]["decomposition_rules"].append(
        {
            "id": "bad-output",
            "triggers": ["RAG"],
            "priority": 25,
            "outputs": ["zzzz unrelated"],
        }
    )
    docs = sample_env.documents()
    result = Orchestrator(cfg, docs, str(tmp_path / "overlap-ws")).run(sample_env.request("q-001"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_QUERY_INVALID
    assert result["error"]["details"]["reason"] == "no_term_overlap"
    assert "subquery_index" in result["error"]
    assert Orchestrator(cfg, docs, str(tmp_path / "overlap-ws")).store.records("q-001") == []
