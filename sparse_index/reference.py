"""Independent uncompressed sequential traversal used for verification."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .config import IndexConfig
from .errors import error
from .stages import load_stage2
from .tokenizer import normalize_token


def _components(
    term_rows: list[tuple[str, dict[str, Any]]],
    doc_weights: dict[int, int],
) -> list[dict[str, Any]]:
    rows = [
        {
            "term_id": int(term["term_id"]),
            "term": str(term["term"]),
            "quantized": int(doc_weights[int(term["term_id"])]),
            "scale": int(term["quant_scale"]),
            "weight": round(int(doc_weights[int(term["term_id"])]) / int(term["quant_scale"]), 12),
        }
        for _, term in term_rows
        if int(term["term_id"]) in doc_weights
    ]
    rows.sort(key=lambda row: row["term_id"])
    return rows


def reference_query(
    config: IndexConfig,
    artifacts_dir: str | Path,
    *,
    terms: list[str],
    mode: str = "and",
    limit: int = 100,
) -> dict[str, Any]:
    """Perform full sequential traversal without touching compressed blocks."""

    if not isinstance(terms, list) or not terms:
        raise error("E_QUERY_OUT_OF_BOUNDS", "查询词项数量必须在 1 以上", terms=terms)
    if not isinstance(terms, list) or len(terms) > 64:
        raise error("E_QUERY_OUT_OF_BOUNDS", "查询词项数量越界", term_count=len(terms) if isinstance(terms, list) else None)
    if not all(isinstance(term, str) and term.strip() for term in terms):
        raise error("E_QUERY_OUT_OF_BOUNDS", "查询词项必须是非空字符串", terms=terms)
    mode = str(mode).lower()
    if mode not in ("and", "or"):
        raise error("E_QUERY_OUT_OF_BOUNDS", "查询语义必须是 and 或 or", mode=mode)
    max_limit = int(config.value("query.max_limit", 1000))
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > max_limit:
        raise error(
            "E_QUERY_OUT_OF_BOUNDS",
            "limit 超出允许范围",
            limit=limit,
            min_limit=1,
            max_limit=max_limit,
        )
    normalized_terms = sorted({normalize_token(term) for term in terms if normalize_token(term)})
    if not normalized_terms or len(normalized_terms) != len({normalize_token(term) for term in terms}):
        raise error("E_QUERY_OUT_OF_BOUNDS", "规范化后查询词项为空", terms=terms)
    stage2 = load_stage2(Path(artifacts_dir) / "stage2")
    by_name = {str(term["term"]): term for term in stage2["terms"]}
    term_rows = [
        (term, by_name[term]) if term in by_name else (term, None) for term in normalized_terms
    ]
    public_term_rows = [
        {"term": term, "term_id": int(found["term_id"]) if found is not None else None}
        for term, found in term_rows
    ]
    known = [(term, found) for term, found in term_rows if found is not None]
    hits: list[dict[str, Any]] = []
    if mode == "and" and len(known) != len(term_rows):
        hits = []
    elif not known:
        hits = []
    else:
        lists: list[tuple[str, dict[str, Any], list[list[Any]]]] = [
            (term, found, found["postings"]) for term, found in known
        ]
        lists.sort(key=lambda item: (len(item[2]), int(item[1]["term_id"])))
        if mode == "and":
            position = 0
            while position < len(lists[0][2]):
                doc_id = str(lists[0][2][position][0])
                all_match = True
                per_list_positions: list[int] = [position]
                for _, _, postings in lists[1:]:
                    cursor = 0
                    while cursor < len(postings) and str(postings[cursor][0]) < doc_id:
                        cursor += 1
                    if cursor >= len(postings) or str(postings[cursor][0]) != doc_id:
                        all_match = False
                        if cursor < len(postings):
                            next_doc = str(postings[cursor][0])
                            while position < len(lists[0][2]) and str(lists[0][2][position][0]) < next_doc:
                                position += 1
                        else:
                            position = len(lists[0][2])
                        break
                    per_list_positions.append(cursor)
                if not all_match:
                    continue
                weights = {
                    int(lists[index][1]["term_id"]): int(lists[index][2][per_list_positions[index]][1])
                    for index in range(len(lists))
                }
                components = _components(known, weights)
                hits.append(
                    {
                        "doc_id": doc_id,
                        "score": round(math.fsum(row["weight"] for row in components), 12),
                        "components": components,
                    }
                )
                position += 1
        else:
            per_list_positions = [0] * len(lists)
            while True:
                candidates = [
                    str(lists[index][2][per_list_positions[index]][0])
                    for index in range(len(lists))
                    if per_list_positions[index] < len(lists[index][2])
                ]
                if not candidates:
                    break
                doc_id = min(candidates)
                weights: dict[int, int] = {}
                for index, (_, term, postings) in enumerate(lists):
                    cursor = per_list_positions[index]
                    while cursor < len(postings) and str(postings[cursor][0]) < doc_id:
                        cursor += 1
                    if cursor < len(postings) and str(postings[cursor][0]) == doc_id:
                        weights[int(term["term_id"])] = int(postings[cursor][1])
                        per_list_positions[index] = cursor + 1
                    else:
                        per_list_positions[index] = cursor
                components = _components(known, weights)
                hits.append(
                    {
                        "doc_id": doc_id,
                        "score": round(math.fsum(row["weight"] for row in components), 12),
                        "components": components,
                    }
                )
    hits.sort(key=lambda hit: (-float(hit["score"]), str(hit["doc_id"])))
    return {
        "mode": mode,
        "terms": public_term_rows,
        "limit": limit,
        "total_hits": len(hits),
        "hits": hits[:limit],
    }
