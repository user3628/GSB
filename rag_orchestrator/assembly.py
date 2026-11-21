"""Deterministic local answer templates and citation attribution."""

from __future__ import annotations

import hashlib
from typing import Any

from .errors import E_EVIDENCE_EMPTY, EngineError
from .util import canonical_json_bytes, sha256_json


def _template_language(normalized: dict[str, Any]) -> str:
    locale = str(normalized.get("locale", "")).lower()
    if locale.startswith("en"):
        return "en"
    return "zh"


def run_assembly(
    normalized: dict[str, Any],
    rerank_product: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    query_id = normalized.get("query_id")
    evidence = rerank_product["evidence"]
    if not evidence:
        raise EngineError(
            E_EVIDENCE_EMPTY,
            "cannot assemble an answer without evidence",
            query_id=query_id,
            details={"reason": "empty_evidence"},
        )

    citations: list[dict[str, Any]] = []
    cited_doc_ids: set[str] = set()
    for item in evidence:
        doc_id = item.get("doc_id")
        if not isinstance(doc_id, str) or not doc_id:
            raise EngineError(
                E_EVIDENCE_EMPTY,
                "evidence citation attribution is incomplete",
                query_id=query_id,
                details={"reason": "citation_without_doc_id", "evidence_index": item.get("evidence_index")},
            )
        citations.append(
            {
                "citation_id": item["citation_id"],
                "doc_id": doc_id,
                "title": item["title"],
                "source": item["source"],
                "source_shards": item["source_shards"],
                "evidence_index": item["evidence_index"],
            }
        )
        cited_doc_ids.add(doc_id)

    evidence_doc_ids = {item["doc_id"] for item in evidence}
    if evidence_doc_ids - cited_doc_ids:
        raise EngineError(
            E_EVIDENCE_EMPTY,
            "citations do not cover all evidence fragments",
            query_id=query_id,
            details={"uncovered_doc_ids": sorted(evidence_doc_ids - cited_doc_ids)},
        )

    language = _template_language(normalized)
    lines: list[str] = []
    if language == "en":
        lines.append("[Local grounded answer]")
        lines.append(f"Query: {normalized['normalized_query']}")
        lines.append("Evidence:")
        for item in evidence:
            lines.append(
                f"{item['citation_id']} 《{item['title']}》 ({item['doc_id']}, {item['source']}): {item['text']}"
            )
        lines.append(
            f"Summary: {len(evidence)} deterministic evidence fragments were selected; citations cover every fragment."
        )
        lines.append("Citations: " + "; ".join(f"{c['citation_id']} {c['doc_id']}" for c in citations))
    else:
        lines.append("【本地检索增强答案】")
        lines.append(f"问题：{normalized['normalized_query']}")
        lines.append("证据：")
        for item in evidence:
            lines.append(
                f"{item['citation_id']} 《{item['title']}》（{item['doc_id']}，{item['source']}）：{item['text']}"
            )
        lines.append(
            f"摘要：已确定性选取 {len(evidence)} 条证据片段，引用归属覆盖全部片段。"
        )
        lines.append("引用：" + "；".join(f"{c['citation_id']} {c['doc_id']}" for c in citations))

    answer_text = "\n".join(lines)
    max_chars = int(config["assembly"]["answer_max_chars"])
    truncated = len(answer_text) > max_chars
    if truncated:
        answer_text = answer_text[:max_chars]
    expected_doc_id = normalized.get("expected_doc_id")
    expected_doc_hit = bool(expected_doc_id and expected_doc_id in cited_doc_ids)
    answer = {
        "text": answer_text,
        "template_version": config["assembly"]["template_version"],
        "language": language,
        "evidence_count": len(evidence),
        "citations": citations,
        "cited_doc_ids": sorted(cited_doc_ids),
        "expected_doc_id": expected_doc_id,
        "expected_doc_hit": expected_doc_hit,
        "truncated": truncated,
        "summary_sha256": hashlib.sha256(answer_text.encode("utf-8")).hexdigest(),
        "citation_coverage": {
            "evidence_doc_ids": sorted(evidence_doc_ids),
            "cited_doc_ids": sorted(cited_doc_ids),
            "fully_covered": evidence_doc_ids == cited_doc_ids,
        },
    }
    answer["answer_sha256"] = sha256_json(answer)
    return {
        "query_id": query_id,
        "answer": answer,
        "assembly": {
            "deterministic": True,
            "llm_used": False,
            "citation_count": len(citations),
            "answer_bytes": len(canonical_json_bytes(answer)),
        },
    }
