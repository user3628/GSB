from __future__ import annotations

import copy
from pathlib import Path

import pytest

from rag_orchestrator.engine import Orchestrator, stable_bytes
from rag_orchestrator.errors import E_INVALID_STAGE, E_LATENCY_BUDGET, E_QUERY_INVALID, E_WORKER


def test_parallel_and_serial_are_byte_identical(sample_env, tmp_path) -> None:
    cfg, docs, _ = sample_env.load()
    request = sample_env.request("q-001")
    parallel = Orchestrator(cfg, docs, str(tmp_path / "parallel-ws"), mode="parallel").run(request)
    serial = Orchestrator(cfg, docs, str(tmp_path / "serial-ws"), mode="serial").run(request)
    assert parallel["status"] == "ok"
    assert stable_bytes(parallel) == stable_bytes(serial)
    assert parallel["answer"] == serial["answer"]
    assert parallel["budget"] == serial["budget"]


def test_cache_reuse_does_not_change_stable_bytes(sample_env, tmp_path) -> None:
    cfg, docs, _ = sample_env.load()
    workspace = str(tmp_path / "cache-ws")
    request = sample_env.request("q-001")
    first = Orchestrator(cfg, docs, workspace, mode="parallel").run(request)
    second = Orchestrator(cfg, docs, workspace, mode="serial").run(request)
    assert stable_bytes(first) == stable_bytes(second)
    assert all(item["status"] == "reused" for item in second["diagnostics"]["cache_usage"].values())


def test_stop_then_resume_matches_from_scratch(sample_env, tmp_path) -> None:
    cfg, docs, _ = sample_env.load()
    request = sample_env.request("q-001")
    scratch = Orchestrator(cfg, docs, str(tmp_path / "scratch-ws"), mode="parallel").run(request)
    workspace = str(tmp_path / "resume-ws")
    stopped = Orchestrator(
        cfg,
        docs,
        workspace,
        mode="parallel",
        stop_after_stage="dispatch",
    ).run(request)
    assert stopped["status"] == "stopped"
    assert stopped["stopped_after_stage"] == "dispatch"
    assert stopped["next_stage"] == "rerank"
    resumed = Orchestrator(cfg, docs, workspace, mode="parallel").run(request)
    assert resumed["status"] == "ok"
    assert stable_bytes(scratch) == stable_bytes(resumed)
    records = Orchestrator(cfg, docs, workspace).store.records("q-001")
    assert [(item["stage"], item["seq"], item["stage_seq"]) for item in records] == [
        ("normalize", 1, 1),
        ("dispatch", 2, 1),
        ("rerank", 3, 1),
        ("assemble", 4, 1),
    ]
    assert resumed["invalidation"]["earliest_stage"] is None


@pytest.mark.parametrize(
    ("mutator", "expected_stage"),
    [
        (lambda cfg: cfg["normalizer"]["synonyms"].__setitem__("预算", ["budget", "quota"]), "normalize"),
        (lambda cfg: cfg["normalizer"].__setitem__("base_priority", 1), "normalize"),
        (lambda cfg: cfg["retrieval"].__setitem__("lexical_weight", 2.25), "dispatch"),
        (lambda cfg: cfg["budget"].__setitem__("global_retrieval_quota", 899), "dispatch"),
        (lambda cfg: cfg["rerank"].__setitem__("length_fit_weight", 0.91), "rerank"),
        (lambda cfg: cfg["rerank"].__setitem__("max_evidence", 3), "rerank"),
        (lambda cfg: cfg["assembly"].__setitem__("template_version", "grounded-local-v2"), "assemble"),
    ],
)
def test_version_fingerprint_invalidates_earliest_stage(sample_env, tmp_path, mutator, expected_stage) -> None:
    cfg, docs, _ = sample_env.load()
    workspace = str(tmp_path / f"invalidate-{expected_stage}-{abs(hash(mutator))}")
    request = sample_env.request("q-001")
    baseline = Orchestrator(cfg, docs, workspace, mode="serial").run(request)
    assert baseline["status"] == "ok"
    changed = copy.deepcopy(cfg)
    mutator(changed)
    result = Orchestrator(changed, docs, workspace, mode="serial").run(request)
    assert result["status"] == "ok"
    assert result["invalidation"]["earliest_stage"] == expected_stage
    assert result["invalidation"]["reasons"]
    order = ["normalize", "dispatch", "rerank", "assemble"]
    earliest_index = order.index(expected_stage)
    for stage in order[:earliest_index]:
        assert result["diagnostics"]["cache_usage"][stage]["status"] == "reused"
    for stage in order[earliest_index:]:
        assert result["diagnostics"]["cache_usage"][stage]["status"] == "rebuilt"


def test_document_content_change_invalidates_dispatch(sample_env, tmp_path) -> None:
    cfg, docs, _ = sample_env.load()
    workspace = str(tmp_path / "doc-invalidate-ws")
    request = sample_env.request("q-001")
    Orchestrator(cfg, docs, workspace).run(request)
    changed_docs = copy.deepcopy(docs)
    changed_docs[0]["body"] += " local mutation"
    result = Orchestrator(cfg, changed_docs, workspace).run(request)
    assert result["invalidation"]["earliest_stage"] == "dispatch"
    assert result["diagnostics"]["cache_usage"]["normalize"]["status"] == "reused"


def test_latency_budget_uses_measured_stage_time_not_thread_timeout(sample_env, tmp_path) -> None:
    cfg = copy.deepcopy(sample_env.config())
    cfg["budget"]["latency_budget_ms"] = 100.0
    docs = sample_env.documents()

    class StepClock:
        def __init__(self) -> None:
            self.value = 0.0

        def __call__(self) -> float:
            current = self.value
            self.value += 0.060
            return current

    workspace = tmp_path / "latency-ws"
    result = Orchestrator(cfg, docs, str(workspace), clock=StepClock()).run(sample_env.request("q-001"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_LATENCY_BUDGET
    assert result["error"]["details"]["stage"] == "dispatch"
    assert result["error"]["details"]["accepted_subquery_count"] > 0
    assert result["error"]["details"]["elapsed_ms"] > cfg["budget"]["latency_budget_ms"]
    stages = [record["stage"] for record in Orchestrator(cfg, docs, str(workspace)).store.records("q-001")]
    assert stages == ["normalize"]


def test_worker_exception_is_structured_and_stage_not_advanced(sample_env, tmp_path, monkeypatch) -> None:
    cfg, docs, _ = sample_env.load()
    workspace = tmp_path / "worker-ws"
    original = __import__("rag_orchestrator.engine", fromlist=["retrieve_subquery"]).retrieve_subquery

    def flaky(subquery, prepared_documents, retrieval_config, query_id):
        if subquery["index"] == 1:
            raise RuntimeError("injected worker failure")
        return original(subquery, prepared_documents, retrieval_config, query_id)

    monkeypatch.setattr("rag_orchestrator.engine.retrieve_subquery", flaky)
    result = Orchestrator(cfg, docs, str(workspace), mode="serial").run(sample_env.request("q-001"))
    assert result["status"] == "error"
    assert result["error"]["code"] == E_WORKER
    assert result["error"]["details"]["worker_errors"]
    assert result["error"]["details"]["completed_subqueries"]
    assert result["partial"]["subquery_results"]
    stages = [record["stage"] for record in Orchestrator(cfg, docs, str(workspace)).store.records("q-001")]
    assert stages == ["normalize"]


def test_run_all_parallel_and_serial_match_and_show_stable_errors(sample_env, tmp_path) -> None:
    cfg, docs, requests = sample_env.load()
    parallel = Orchestrator(cfg, docs, str(tmp_path / "all-p"), mode="parallel").run_all(requests)
    serial = Orchestrator(cfg, docs, str(tmp_path / "all-s"), mode="serial").run_all(requests)
    assert parallel["summary_sha256"] == serial["summary_sha256"]
    by_id = {item["query_id"]: item for item in parallel["results"]}
    assert by_id["q-006"]["error"]["code"] == E_QUERY_INVALID
    assert by_id["q-007"]["error"]["code"] == E_QUERY_INVALID
    assert by_id["q-001"]["status"] == "ok"
    assert by_id["q-001"]["answer"]["citation_coverage"]["fully_covered"] is True
