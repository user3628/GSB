"""Multi-shard lexical retrieval with log-domain score components."""

from __future__ import annotations

import math
from typing import Any

from .errors import E_DOCUMENT_INVALID, E_NUMERIC, EngineError
from .normalize import normalize_text, tokenize
from .util import require_finite, stable_round


def prepare_documents(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for position, document in enumerate(documents):
        if not isinstance(document, dict):
            raise EngineError(E_DOCUMENT_INVALID, "document record must be a mapping", details={"position": position})
        missing = [
            key
            for key in ("doc_id", "title", "body", "lang", "source")
            if not isinstance(document.get(key), str) or not document.get(key)
        ]
        if missing:
            raise EngineError(
                E_DOCUMENT_INVALID,
                "document record has missing or invalid fields",
                doc_id=str(document.get("doc_id")) if document.get("doc_id") is not None else None,
                details={"position": position, "missing": missing},
            )
        title = normalize_text(document["title"])
        body = normalize_text(document["body"])
        shard = document.get("shard", document["source"])
        if not isinstance(shard, str) or not shard:
            shard = document["source"]
        prepared.append(
            {
                "doc_id": document["doc_id"],
                "title": title,
                "body": body,
                "lang": document["lang"],
                "source": document["source"],
                "shard": shard,
                "title_tokens": tokenize(title),
                "body_tokens": tokenize(body),
            }
        )
    prepared.sort(
        key=lambda item: (
            item["doc_id"],
            item["shard"],
            item["title"],
            item["body"],
            item["source"],
        )
    )
    return prepared


def _score_record(
    subquery: dict[str, Any],
    record: dict[str, Any],
    retrieval: dict[str, Any],
    query_id: str | None,
) -> dict[str, Any] | None:
    search_terms = set(subquery["search_terms"])
    title_tokens = set(record["title_tokens"])
    body_tokens = set(record["body_tokens"])
    document_tokens = title_tokens | body_tokens
    overlap = sorted(search_terms & document_tokens)
    if not overlap:
        return None
    title_overlap = sorted(search_terms & title_tokens)
    source_prior = float(retrieval["source_prior"].get(record["source"], 0.0))
    try:
        lexical = retrieval["lexical_weight"] * math.log1p(len(overlap))
        title = retrieval["title_weight"] * math.log1p(len(title_overlap))
        source = retrieval["source_weight"] * math.log1p(source_prior)
        length_delta = abs(math.log1p(len(document_tokens)) - math.log1p(retrieval["ideal_tokens"]))
        length = -retrieval["length_weight"] * length_delta
        total = lexical + title + source + length
        for name, value in (
            ("lexical", lexical),
            ("title", title),
            ("source", source),
            ("length", length),
            ("total", total),
        ):
            require_finite(value, name)
    except (ValueError, OverflowError) as exc:
        raise EngineError(
            E_NUMERIC,
            "retrieval score is NaN or Inf",
            query_id=query_id,
            doc_id=record["doc_id"],
            details={"subquery_index": subquery["index"], "numeric_error": str(exc)},
        ) from exc

    components = {
        "lexical_log": stable_round(lexical),
        "title_log": stable_round(title),
        "source_log": stable_round(source),
        "length_log": stable_round(length),
        "total_log": stable_round(total),
    }
    return {
        "subquery_index": int(subquery["index"]),
        "subquery_text": subquery["text"],
        "subquery_priority": int(subquery["priority"]),
        "doc_id": record["doc_id"],
        "title": record["title"],
        "body": record["body"],
        "lang": record["lang"],
        "source": record["source"],
        "source_shards": [record["shard"]],
        "duplicate_record_count": 1,
        "matched_terms": overlap,
        "title_matched_terms": title_overlap,
        "document_token_count": len(document_tokens),
        "score": components,
        "retrieval_score": components["total_log"],
    }


def retrieve_subquery(
    subquery: dict[str, Any],
    prepared_documents: list[dict[str, Any]],
    retrieval_config: dict[str, Any],
    query_id: str | None,
) -> dict[str, Any]:
    """Return one deduplicated candidate list for one subquery."""

    best_by_doc: dict[str, dict[str, Any]] = {}
    sources_by_doc: dict[str, set[str]] = {}
    duplicate_counts: dict[str, int] = {}
    for record in prepared_documents:
        candidate = _score_record(subquery, record, retrieval_config, query_id)
        if candidate is None:
            continue
        doc_id = candidate["doc_id"]
        sources_by_doc.setdefault(doc_id, set()).add(record["shard"])
        duplicate_counts[doc_id] = duplicate_counts.get(doc_id, 0) + 1
        previous = best_by_doc.get(doc_id)
        if previous is None:
            best_by_doc[doc_id] = candidate
            continue
        current_key = (
            candidate["retrieval_score"],
            candidate["title"],
            candidate["body"],
            candidate["source"],
        )
        previous_key = (
            previous["retrieval_score"],
            previous["title"],
            previous["body"],
            previous["source"],
        )
        if current_key > previous_key:
            best_by_doc[doc_id] = candidate

    candidates: list[dict[str, Any]] = []
    for doc_id, candidate in best_by_doc.items():
        candidate["source_shards"] = sorted(sources_by_doc[doc_id])
        candidate["duplicate_record_count"] = duplicate_counts[doc_id]
        candidates.append(candidate)
    candidates.sort(key=lambda item: (-item["retrieval_score"], item["doc_id"], item["source"]))
    return {
        "subquery_index": int(subquery["index"]),
        "subquery_text": subquery["text"],
        "priority": int(subquery["priority"]),
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
