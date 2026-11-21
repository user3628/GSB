from __future__ import annotations

import copy

from rag_orchestrator.budget import plan_budget
from rag_orchestrator.config import validate_config
from rag_orchestrator.engine import Orchestrator
from rag_orchestrator.errors import E_BUDGET_EXHAUSTED, E_CONFIG_INVALID, E_EVIDENCE_EMPTY
from rag_orchestrator.evidence import run_rerank


def _subquery(index: int, priority: int, terms: list[str]) -> dict:
    return {"index": index, "priority": priority, "text": " ".join(terms), "term_count": len(terms), "search_terms": terms}


def test_fixed_credit_planner_never_exceeds_global_or_channel_quota(sample_env) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["budget"].update(
        {
            "global_retrieval_quota": 100,
            "per_channel_quota": 35,
            "max_concurrent_channels": 2,
            "cost_per_subquery": 5,
            "cost_per_doc_shard": 1,
            "cost_per_term": 1,
        }
    )
    plan = plan_budget(
        [_subquery(0, 0, ["a", "b", "c"]), _subquery(1, 10, ["d", "e"]), _subquery(2, 20, ["f"] * 8)],
        document_shard_count=9,
        budget_config=cfg["budget"],
    )
    assert plan["global_used"] <= plan["global_quota"]
    assert all(channel["used"] <= channel["quota"] for channel in plan["channels"])
    assert plan["rejected"]
    assert plan["degraded"] is True
    assert sum(item["estimate_credits"] for item in plan["accepted"]) == plan["global_used"]


def test_out_of_range_quota_configuration_uses_stable_error(sample_env) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["budget"]["global_retrieval_quota"] = 0
    try:
        validate_config(cfg)
    except Exception as exc:  # noqa: BLE001
        assert exc.code == E_CONFIG_INVALID
    else:
        raise AssertionError("zero quota must be rejected by config validation")


def test_low_global_quota_degrades_but_completes(sample_env, tmp_path) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["budget"]["global_retrieval_quota"] = 130
    docs = sample_env.documents()
    result = Orchestrator(cfg, docs, str(tmp_path / "degraded-ws")).run(sample_env.request("q-001"))
    assert result["status"] == "degraded"
    assert result["budget"]["accepted_count"] == 1
    assert result["budget"]["rejected_count"] >= 1
    assert result["budget"]["global_used"] <= result["budget"]["global_quota"]
    assert result["evidence"]


def test_zero_admitted_subqueries_returns_budget_error(sample_env, tmp_path) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["budget"]["global_retrieval_quota"] = 1
    docs = sample_env.documents()
    workspace = tmp_path / "exhausted-ws"
    result = Orchestrator(cfg, docs, str(workspace)).run(sample_env.request("q-001"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_BUDGET_EXHAUSTED
    assert result["error"]["details"]["accepted_subquery_count"] == 0
    assert [record["stage"] for record in Orchestrator(cfg, docs, str(workspace)).store.records("q-001")] == ["normalize"]


def test_zero_evidence_limit_returns_e_evidence_empty(sample_env, tmp_path) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["rerank"]["max_evidence"] = 0
    docs = sample_env.documents()
    workspace = tmp_path / "evidence-ws"
    result = Orchestrator(cfg, docs, str(workspace)).run(sample_env.request("q-001"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_EVIDENCE_EMPTY
    assert result["error"]["query_id"] == "q-001"
    stages = [record["stage"] for record in Orchestrator(cfg, docs, str(workspace)).store.records("q-001")]
    assert stages == ["normalize", "dispatch"]


def test_char_budget_without_fitting_min_fragment_returns_e_evidence_empty(sample_env, tmp_path) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["rerank"]["max_total_chars"] = 20
    cfg["rerank"]["min_fragment_chars"] = 24
    docs = sample_env.documents()
    result = Orchestrator(cfg, docs, str(tmp_path / "char-ws")).run(sample_env.request("q-001"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_EVIDENCE_EMPTY
    assert result["error"]["details"]["reason"] == "no_selectable_evidence"


def test_evidence_and_citations_cover_each_other(sample_env, tmp_path) -> None:
    cfg, docs, _ = sample_env.load()
    result = Orchestrator(cfg, docs, str(tmp_path / "citation-ws")).run(sample_env.request("q-001"))
    assert result["status"] == "ok"
    evidence_ids = [item["doc_id"] for item in result["evidence"]]
    citation_ids = [item["doc_id"] for item in result["answer"]["citations"]]
    assert evidence_ids == citation_ids
    assert result["answer"]["citation_coverage"]["fully_covered"] is True
    assert result["answer"]["summary_sha256"]
    assert result["answer"]["expected_doc_hit"] is True


def _candidate(doc_id: str, subquery_index: int, body: str, score: float) -> dict:
    return {
        "doc_id": doc_id,
        "title": doc_id,
        "body": body,
        "lang": "en-US",
        "source": "official",
        "source_shards": [f"shard-{subquery_index + 1}"],
        "duplicate_record_count": 1,
        "matched_terms": ["alpha"],
        "title_matched_terms": ["alpha"],
        "document_token_count": len(body.split()),
        "score": {
            "lexical_log": score,
            "title_log": 0.0,
            "source_log": 0.0,
            "length_log": 0.0,
            "total_log": score,
        },
        "retrieval_score": score,
        "subquery_index": subquery_index,
        "subquery_text": "alpha",
        "subquery_priority": 0,
    }


def test_mmr_and_cross_subquery_duplicate_reasons_are_recorded(sample_env) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["rerank"].update(
        {
            "max_evidence": 5,
            "max_total_chars": 1000,
            "min_fragment_chars": 0,
            "mmr_similarity_threshold": 0.5,
        }
    )
    normalized = {
        "query_id": "synthetic",
        "expanded_query_terms": ["alpha"],
    }
    dispatch = {
        "subquery_results": [
            {
                "subquery_index": 0,
                "subquery_text": "alpha one",
                "priority": 0,
                "status": "accepted",
                "candidates": [
                    _candidate("doc-a", 0, "alpha beta gamma delta epsilon", 20.0),
                    _candidate("doc-b", 0, "alpha beta gamma delta zeta", 18.0),
                ],
            },
            {
                "subquery_index": 1,
                "subquery_text": "alpha two",
                "priority": 10,
                "status": "accepted",
                "candidates": [
                    _candidate("doc-c", 1, "alpha theta iota kappa lambda", 12.0),
                    _candidate("doc-a", 1, "alpha beta gamma delta epsilon", 11.0),
                ],
            },
        ]
    }
    product = run_rerank(normalized, dispatch, cfg)
    reasons = {item["reason"] for item in product["discarded_evidence"]}
    assert "mmr_overlap" in reasons
    assert "duplicate_doc_across_subqueries" in reasons
    assert [item["doc_id"] for item in product["evidence"]] == ["doc-a", "doc-c"]
