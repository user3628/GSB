"""Version fingerprints for each orchestration stage."""

from __future__ import annotations

from typing import Any

from .config import config_section_hashes
from .util import sha256_json


ENGINE_VERSION = "rag-orchestrator-stage-v1"


def normalize_query_input_fingerprint(request: dict[str, Any]) -> str:
    return sha256_json(
        {
            "engine": ENGINE_VERSION,
            "query": request.get("query"),
            "locale": request.get("locale"),
            "channel": request.get("channel"),
            "expected_doc_id": request.get("expected_doc_id"),
        }
    )


def documents_fingerprint(documents: list[dict[str, Any]]) -> str:
    canonical_docs = sorted(
        (
            {
                "doc_id": doc.get("doc_id"),
                "title": doc.get("title"),
                "body": doc.get("body"),
                "lang": doc.get("lang"),
                "source": doc.get("source"),
                "shard": doc.get("shard", doc.get("source")),
            }
            for doc in documents
        ),
        key=lambda item: (
            str(item["doc_id"]),
            str(item["shard"]),
            str(item["title"]),
            str(item["body"]),
        ),
    )
    return sha256_json(canonical_docs)


def stage_components(
    stage: str,
    *,
    config_hashes: dict[str, str],
    input_fingerprint: str,
    documents_hash: str | None = None,
) -> dict[str, str]:
    if stage == "normalize":
        return {
            "engine": sha256_json(ENGINE_VERSION),
            "input": input_fingerprint,
            "synonyms": config_hashes["synonyms"],
            "decomposition": config_hashes["decomposition"],
        }
    if stage == "dispatch":
        return {
            "engine": sha256_json(ENGINE_VERSION),
            "input": input_fingerprint,
            "documents": documents_hash or "",
            "retrieval": config_hashes["retrieval"],
            "budget": config_hashes["budget"],
        }
    if stage == "rerank":
        return {
            "engine": sha256_json(ENGINE_VERSION),
            "input": input_fingerprint,
            "rerank": config_hashes["rerank"],
            "evidence": config_hashes["evidence"],
        }
    if stage == "assemble":
        return {
            "engine": sha256_json(ENGINE_VERSION),
            "input": input_fingerprint,
            "assembly": config_hashes["assembly"],
        }
    raise ValueError(f"unknown stage: {stage}")


def stage_fingerprint(components: dict[str, str]) -> str:
    return sha256_json(components)


def version_descriptor(
    config: dict[str, Any],
    stage_fingerprints: dict[str, str] | None = None,
) -> dict[str, Any]:
    hashes = config_section_hashes(config)
    return {
        "engine_version": ENGINE_VERSION,
        "config_fingerprint": hashes["config"],
        "budget_config_version": hashes["budget"][:16],
        "section_fingerprints": hashes,
        "stage_fingerprints": stage_fingerprints or {},
    }
