from __future__ import annotations

import hashlib
from pathlib import Path

from rag_orchestrator.config import load_config
from rag_orchestrator.samplegen import generate_samples
from rag_orchestrator.util import read_jsonl


def _digest_tree(path: Path) -> dict[str, str]:
    return {
        str(file.relative_to(path)): hashlib.sha256(file.read_bytes()).hexdigest()
        for file in sorted(path.rglob("*"))
        if file.is_file()
    }


def test_generator_is_byte_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    manifest_a = generate_samples(first, seed=20261122)
    manifest_b = generate_samples(second, seed=20261122)
    assert manifest_a == manifest_b
    assert _digest_tree(first) == _digest_tree(second)


def test_generated_corpus_has_required_coverage(sample_env) -> None:
    docs = sample_env.documents()
    requests = sample_env.requests()
    shards = {doc["shard"] for doc in docs}
    assert len(shards) >= 3
    assert len(requests) >= 5
    doc_003 = [doc for doc in docs if doc["doc_id"] == "doc-003"]
    assert len(doc_003) == 2
    assert len({doc["shard"] for doc in doc_003}) == 2
    assert any(any("\u4e00" <= char <= "\u9fff" for char in request["query"]) and any(char.isascii() and char.isalpha() for char in request["query"]) for request in requests)
    assert any(not request["query"].strip() for request in requests)


def test_config_generated_by_sample_writer_is_valid(sample_env) -> None:
    cfg = load_config(sample_env.config_path)
    assert cfg["normalizer"]["max_subqueries"] >= 1
    assert cfg["budget"]["global_retrieval_quota"] > 0
    assert cfg["rerank"]["max_evidence"] >= 1
