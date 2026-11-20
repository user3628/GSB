from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable

from .errors import E_EMPTY_TEXT


_CJK_CHAR_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_CJK_RUN_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]+")
_LATIN_RUN_RE = re.compile(r"[a-z0-9]+")


def normalize_text(text: str, *, collapse_whitespace: bool = True) -> str:
    normalized = unicodedata.normalize("NFKC", text or "").lower()
    if collapse_whitespace:
        normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _is_cjk(char: str) -> bool:
    return bool(_CJK_CHAR_RE.fullmatch(char))


def _latin_tokens(text: str) -> Iterable[dict[str, Any]]:
    for match in _LATIN_RUN_RE.finditer(text):
        yield {
            "term": match.group(0),
            "start": match.start(),
            "end": match.end(),
            "lang": "en",
            "script": "latin",
        }


def _cjk_tokens(text: str) -> Iterable[dict[str, Any]]:
    for run in _CJK_RUN_RE.finditer(text):
        value = run.group(0)
        base = run.start()
        for index, char in enumerate(value):
            yield {
                "term": char,
                "start": base + index,
                "end": base + index + 1,
                "lang": "zh",
                "script": "cjk",
            }
        for index in range(len(value) - 1):
            yield {
                "term": value[index : index + 2],
                "start": base + index,
                "end": base + index + 2,
                "lang": "zh",
                "script": "cjk",
            }


def extract_tokens(text: str) -> list[dict[str, Any]]:
    tokens = [*_latin_tokens(text), *_cjk_tokens(text)]
    tokens.sort(key=lambda token: (token["start"], token["end"], token["term"]))
    return tokens


def normalize_pair(
    pair: dict[str, Any],
    config: dict[str, Any],
    *,
    shard: str,
) -> dict[str, Any]:
    settings = config["normalization"]
    query = normalize_text(
        str(pair.get("query", "")),
        collapse_whitespace=bool(settings["collapse_whitespace"]),
    )
    doc = normalize_text(
        str(pair.get("doc", "")),
        collapse_whitespace=bool(settings["collapse_whitespace"]),
    )
    query_tokens = extract_tokens(query)
    doc_tokens = extract_tokens(doc)
    for index, token in enumerate(query_tokens, 1):
        token["term_id"] = f"q{index:06d}"
    for index, token in enumerate(doc_tokens, 1):
        token["term_id"] = f"d{index:06d}"

    diagnostics: list[dict[str, Any]] = []
    if not query_tokens:
        diagnostics.append(
            {
                "code": E_EMPTY_TEXT,
                "pair_id": str(pair["pair_id"]),
                "stage": "stage1_normalize_extract",
                "field": "query",
                "message": "query contains no extractable term",
            }
        )
    if not doc_tokens:
        diagnostics.append(
            {
                "code": E_EMPTY_TEXT,
                "pair_id": str(pair["pair_id"]),
                "stage": "stage1_normalize_extract",
                "field": "doc",
                "message": "doc contains no extractable term",
            }
        )

    record: dict[str, Any] = {
        "pair_id": str(pair["pair_id"]),
        "shard": shard,
        "query": query,
        "doc": doc,
        "lang_query": str(pair.get("lang_query", "")),
        "lang_doc": str(pair.get("lang_doc", "")),
        "source": str(pair.get("source", "")),
        "query_tokens": query_tokens,
        "doc_tokens": doc_tokens,
        "diagnostics": diagnostics,
    }
    if "human_relevance" in pair and pair["human_relevance"] is not None:
        record["human_relevance"] = int(pair["human_relevance"])
    return record
