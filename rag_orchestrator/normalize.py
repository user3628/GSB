"""Query normalization, tokenization, and deterministic decomposition."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable

from .errors import E_QUERY_INVALID, EngineError


_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)
_CLAUSE_SPLIT_RE = re.compile(r"[，,。.;；!?！？、]+", re.UNICODE)


def _is_cjk(character: str) -> bool:
    code = ord(character)
    return (
        0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xF900 <= code <= 0xFAFF
        or 0x20000 <= code <= 0x2FA1F
    )


def _is_latin_word_character(character: str) -> bool:
    if character.isdigit():
        return True
    if "a" <= character <= "z" or "A" <= character <= "Z":
        return True
    try:
        return unicodedata.name(character).startswith("LATIN")
    except ValueError:
        return False


def normalize_text(value: str) -> str:
    """NFKC, full/half-width unify, then collapse all whitespace."""

    normalized = unicodedata.normalize("NFKC", value)
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def tokenize(value: str) -> list[str]:
    """Chinese characters are single terms; Latin/digit runs are one term."""

    normalized = normalize_text(value).casefold()
    tokens: list[str] = []
    latin_buffer: list[str] = []

    def flush_latin() -> None:
        if latin_buffer:
            tokens.append("".join(latin_buffer))
            latin_buffer.clear()

    for character in normalized:
        if _is_cjk(character):
            flush_latin()
            tokens.append(character)
        elif _is_latin_word_character(character):
            latin_buffer.append(character)
        elif character in {"_", "-", "."} and latin_buffer:
            latin_buffer.append(character)
        else:
            flush_latin()
    flush_latin()
    return tokens


def _ordered_unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _query_id(request: dict[str, Any]) -> str | None:
    value = request.get("query_id")
    return value if isinstance(value, str) and value else None


def _invalid(
    request: dict[str, Any],
    message: str,
    *,
    subquery_index: int,
    details: dict[str, Any] | None = None,
) -> EngineError:
    return EngineError(
        E_QUERY_INVALID,
        message,
        query_id=_query_id(request),
        subquery_index=subquery_index,
        details=details,
    )


def _make_subquery(
    text: str,
    *,
    priority: int,
    source: str,
    matched_query_terms: Iterable[str] = (),
) -> dict[str, Any]:
    normalized = normalize_text(text)
    tokens = tokenize(normalized)
    return {
        "text": normalized,
        "priority": int(priority),
        "source": source,
        "tokens": tokens,
        "matched_query_terms": sorted(set(matched_query_terms)),
    }


def run_normalization(request: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    query_id = _query_id(request)
    raw_query = request.get("query")
    if not isinstance(raw_query, str):
        raise _invalid(request, "query must be a string", subquery_index=0, details={"reason": "missing_query"})

    normalized_query = normalize_text(raw_query)
    if not normalized_query:
        raise _invalid(request, "query is empty after normalization", subquery_index=0, details={"reason": "empty_query"})

    normalizer = config["normalizer"]
    query_tokens = tokenize(normalized_query)
    if not query_tokens:
        raise _invalid(request, "query has no indexable terms", subquery_index=0, details={"reason": "no_terms"})

    query_cf = normalized_query.casefold()
    allowed_terms: set[str] = set(query_tokens)
    synonym_expansions: set[str] = set(query_tokens)
    synonym_matches: list[dict[str, Any]] = []
    for key, values in normalizer["synonyms"].items():
        key_normalized = normalize_text(key)
        value_norms = [normalize_text(item) for item in values]
        candidates = [key_normalized, *value_norms]
        if any(candidate and candidate.casefold() in query_cf for candidate in candidates):
            group_terms = set(tokenize(key_normalized))
            for value in value_norms:
                group_terms.update(tokenize(value))
            allowed_terms.update(group_terms)
            synonym_expansions.update(group_terms)
            synonym_matches.append(
                {
                    "key": key,
                    "values": list(values),
                    "terms": sorted(group_terms),
                }
            )

    preliminary: list[dict[str, Any]] = []
    base = _make_subquery(
        normalized_query,
        priority=normalizer["base_priority"],
        source="base",
        matched_query_terms=query_tokens,
    )
    preliminary.append(base)

    clauses = [normalize_text(part) for part in _CLAUSE_SPLIT_RE.split(normalized_query)]
    clauses = [part for part in clauses if part and tokenize(part)]
    if len(clauses) > 1:
        for clause in clauses:
            preliminary.append(
                _make_subquery(
                    clause,
                    priority=normalizer["clause_priority"],
                    source="clause",
                    matched_query_terms=query_tokens,
                )
            )

    for rule in normalizer["decomposition_rules"]:
        matched = any(
            normalize_text(trigger).casefold() in query_cf
            for trigger in rule["triggers"]
            if normalize_text(trigger)
        )
        if not matched:
            continue
        for output in rule["outputs"]:
            preliminary.append(
                _make_subquery(
                    output,
                    priority=rule["priority"],
                    source=f"rule:{rule['id']}",
                    matched_query_terms=query_tokens,
                )
            )

    for match in synonym_matches:
        existing_texts = {item["text"].casefold() for item in preliminary}
        replacement = None
        for value in match["values"]:
            normalized_value = normalize_text(value)
            if normalized_value and normalized_value.casefold() not in existing_texts:
                replacement = normalized_value
                break
        if replacement:
            preliminary.append(
                _make_subquery(
                    replacement,
                    priority=normalizer["synonym_priority"],
                    source=f"synonym:{match['key']}",
                    matched_query_terms=match["terms"],
                )
            )

    deduplicated: list[dict[str, Any]] = []
    seen_text: set[str] = set()
    for item in preliminary:
        key = item["text"].casefold()
        if key in seen_text:
            continue
        seen_text.add(key)
        if item["source"] == "base":
            search_terms = set(item["tokens"]) | synonym_expansions
        else:
            search_terms = set(item["tokens"]) | synonym_expansions
        item["search_terms"] = sorted(search_terms)
        item["term_count"] = len(item["search_terms"])
        deduplicated.append(item)

    max_subqueries = normalizer["max_subqueries"]
    if len(deduplicated) > max_subqueries:
        raise _invalid(
            request,
            f"subquery count {len(deduplicated)} exceeds max_subqueries {max_subqueries}",
            subquery_index=max_subqueries,
            details={"reason": "too_many_subqueries", "count": len(deduplicated), "max_subqueries": max_subqueries},
        )

    deduplicated.sort(key=lambda item: item["priority"])
    for index, item in enumerate(deduplicated):
        item["index"] = index
        overlap = sorted(set(item["tokens"]) & allowed_terms)
        if not overlap:
            raise _invalid(
                request,
                "subquery has no term overlap with the normalized query",
                subquery_index=index,
                details={"reason": "no_term_overlap", "subquery": item["text"]},
            )
        item["matched_query_terms"] = overlap

    ordered_subqueries = []
    for index, item in enumerate(deduplicated):
        ordered_subqueries.append(
            {
                "index": index,
                "text": item["text"],
                "priority": item["priority"],
                "source": item["source"],
                "tokens": item["tokens"],
                "search_terms": item["search_terms"],
                "matched_query_terms": item["matched_query_terms"],
                "term_count": item["term_count"],
            }
        )

    return {
        "query_id": query_id,
        "raw_query": raw_query,
        "normalized_query": normalized_query,
        "locale": str(request.get("locale", "zh-CN")),
        "channel": str(request.get("channel", "local")),
        "expected_doc_id": request.get("expected_doc_id"),
        "query_tokens": query_tokens,
        "expanded_query_terms": sorted(synonym_expansions),
        "synonym_matches": synonym_matches,
        "subqueries": ordered_subqueries,
        "normalization": {
            "unicode": "NFKC",
            "whitespace": "collapsed",
            "width": "unified",
            "tokenization": "cjk-single-char+latin-run",
        },
    }
