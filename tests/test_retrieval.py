from __future__ import annotations

import copy
import math

from rag_orchestrator.engine import Orchestrator
from rag_orchestrator.errors import E_NUMERIC
from rag_orchestrator.normalize import run_normalization
from rag_orchestrator.retrieval import prepare_documents, retrieve_subquery


def test_duplicate_doc_id_is_deduplicated_with_all_source_shards(sample_env) -> None:
    cfg, docs, _ = sample_env.load()
    normalized = run_normalization(sample_env.request("q-002"), cfg)
    prepared = prepare_documents(docs)
    for subquery in normalized["subqueries"]:
        result = retrieve_subquery(subquery, prepared, cfg["retrieval"], "q-002")
        matches = [item for item in result["candidates"] if item["doc_id"] == "doc-003"]
        if not matches:
            continue
        assert len(matches) == 1
        assert matches[0]["source_shards"] == ["shard-02", "shard-03"]
        assert matches[0]["duplicate_record_count"] == 2
        return
    raise AssertionError("expected doc-003 to match q-002")


def test_score_components_are_finite_and_explicit(sample_env) -> None:
    cfg, docs, _ = sample_env.load()
    normalized = run_normalization(sample_env.request("q-001"), cfg)
    prepared = prepare_documents(docs)
    result = retrieve_subquery(normalized["subqueries"][0], prepared, cfg["retrieval"], "q-001")
    assert result["candidates"]
    for candidate in result["candidates"]:
        assert set(candidate["score"]) == {"lexical_log", "title_log", "source_log", "length_log", "total_log"}
        assert all(math.isfinite(value) for value in candidate["score"].values())
        assert candidate["retrieval_score"] == candidate["score"]["total_log"]


def test_non_finite_retrieval_score_returns_e_numeric_without_dispatch_checkpoint(sample_env, tmp_path) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["retrieval"]["lexical_weight"] = 1.7e308
    docs = sample_env.documents()
    workspace = tmp_path / "numeric-ws"
    result = Orchestrator(cfg, docs, str(workspace)).run(sample_env.request("q-001"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_NUMERIC
    assert result["error"]["query_id"] == "q-001"
    assert result["error"].get("doc_id")
    records = Orchestrator(cfg, docs, str(workspace)).store.records("q-001")
    assert [record["stage"] for record in records] == ["normalize"]
