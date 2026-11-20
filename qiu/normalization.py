"""Unicode normalization and deterministic tokenization."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from .errors import QueryError, RequestError

_TOKEN_RE = re.compile(r"[a-z0-9]+|[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_and_tokenize(raw_query: Any, *, query_id: str | None, max_chars: int) -> dict[str, Any]:
    if not isinstance(raw_query, str):
        raise RequestError("raw_query must be a string", query_id=query_id)

    if len(raw_query) > max_chars:
        raise QueryError(
            "E_QUERY_TOO_LONG",
            "normalization",
            "raw_query exceeds the configured character limit",
            query_id=query_id,
            details={"actual_chars": len(raw_query), "max_query_chars": max_chars},
        )

    # NFKC performs compatibility folding, including full-width ASCII forms.
    normalized = unicodedata.normalize("NFKC", raw_query)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip().lower()

    tokens: list[dict[str, Any]] = []
    for match in _TOKEN_RE.finditer(normalized):
        text = match.group(0)
        kind = "latin_alnum" if text.isascii() else "chinese"
        tokens.append({"end": match.end(), "kind": kind, "start": match.start(), "text": text})

    if not tokens:
        raise QueryError(
            "E_EMPTY_QUERY",
            "normalization",
            "query is empty or contains no searchable token",
            query_id=query_id,
            details={"normalized_query": normalized},
        )

    return {
        "normalized_query": normalized,
        "offset_basis": "normalized_query",
        "tokens": tokens,
    }
