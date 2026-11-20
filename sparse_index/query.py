"""Lazy compressed query execution, skip-list intersection, and caching."""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .cache import QueryCache
from .codec import FILE_MAGIC, decode_block
from .config import IndexConfig
from .errors import error
from .stages import load_stage2, load_stage3
from .tokenizer import normalize_token
from .utils import canonical_json_bytes, sha256_bytes


@dataclass
class CursorValue:
    doc_id: str
    weight: int


class PostingCursor:
    """Decode one optional posting block at a time and use sparse skips."""

    def __init__(
        self,
        term: dict[str, Any],
        binary_path: Path,
        ordinal_to_doc_id: list[str],
        *,
        deadline: float | None = None,
    ):
        self.term = term
        self.term_id = int(term["term_id"])
        self.binary_path = binary_path
        self.ordinal_to_doc_id = ordinal_to_doc_id
        self.deadline = deadline
        self._handle = binary_path.open("rb")
        try:
            if self._handle.read(len(FILE_MAGIC)) != FILE_MAGIC:
                raise error("E_BLOCK_CORRUPTION", "postings.bin 文件头不匹配")
            self.blocks: list[dict[str, Any]] = term["blocks"]
            self.skips: list[dict[str, Any]] = term["skips"]
            self.block_starts: list[int] = []
            running = 0
            for block in self.blocks:
                self.block_starts.append(running)
                running += int(block["posting_count"])
            self.block_index = 0
            self.position = 0
            self.ordinal = 0
            self.block_postings: list[CursorValue] = []
            self.skip_jumps = 0
            if self.blocks:
                self._load_block(0)
        except Exception:
            self._handle.close()
            raise

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "PostingCursor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _check_deadline(self) -> None:
        if self.deadline is not None and time.perf_counter() > self.deadline:
            raise error("E_LATENCY_BUDGET", "查询超过延迟预算", elapsed_checkpoint="inside_postings")

    def _load_block(self, block_index: int) -> None:
        self._check_deadline()
        if not 0 <= block_index < len(self.blocks):
            self.block_postings = []
            return
        block = self.blocks[block_index]
        self._handle.seek(int(block["offset"]))
        block_bytes = self._handle.read(int(block["length"]))
        decoded = decode_block(
            block_bytes,
            self.ordinal_to_doc_id,
            term_id=self.term_id,
            block_index=block_index,
        )
        expected_count = int(block["posting_count"])
        if len(decoded) != expected_count:
            raise error(
                "E_BLOCK_CORRUPTION",
                "分块 posting 数量与元数据不一致",
                term_id=self.term_id,
                block_index=block_index,
                expected=expected_count,
                actual=len(decoded),
            )
        if decoded[0][0] != str(block["first_doc_id"]) or decoded[-1][0] != str(block["last_doc_id"]):
            raise error(
                "E_BLOCK_CORRUPTION",
                "分块首尾 doc_id 与元数据不一致",
                term_id=self.term_id,
                block_index=block_index,
            )
        self.block_index = block_index
        self.position = 0
        self.ordinal = self.block_starts[block_index]
        self.block_postings = [CursorValue(doc_id, weight) for doc_id, weight in decoded]

    @property
    def valid(self) -> bool:
        return bool(self.block_postings) and self.position < len(self.block_postings)

    @property
    def current(self) -> CursorValue:
        if not self.valid:
            raise error("E_QUERY_OUT_OF_BOUNDS", "游标已越界", term_id=self.term_id)
        return self.block_postings[self.position]

    def advance(self) -> None:
        if not self.valid:
            return
        self.position += 1
        self.ordinal += 1
        if self.position >= len(self.block_postings):
            next_block = self.block_index + 1
            if next_block < len(self.blocks):
                self._load_block(next_block)
            else:
                self.block_postings = []
                self.position = 0

    def seek(self, target_doc_id: str) -> None:
        """Move to the first posting whose doc id is >= target."""

        if not self.valid:
            return
        if self.current.doc_id >= target_doc_id:
            return
        # Use the sparse skip list for large forward moves.  Candidate skips
        # must point beyond the current block, so jumping never revisits data.
        candidate: dict[str, Any] | None = None
        for skip in self.skips:
            if int(skip["block_index"]) <= self.block_index:
                continue
            if str(skip["doc_id"]) > target_doc_id:
                break
            candidate = skip
        if candidate is not None:
            target_block = int(candidate["block_index"])
            if target_block != self.block_index:
                self._load_block(target_block)
                self.skip_jumps += 1
        while self.valid and self.current.doc_id < target_doc_id:
            block = self.blocks[self.block_index]
            if self.current.doc_id == str(block["last_doc_id"]):
                next_block = self.block_index + 1
                if next_block >= len(self.blocks):
                    self.block_postings = []
                    self.position = 0
                    return
                self._load_block(next_block)
            else:
                self.advance()


def _result_from_hits(
    *,
    mode: str,
    term_rows: list[dict[str, Any]],
    limit: int,
    hits: list[dict[str, Any]],
) -> dict[str, Any]:
    hits.sort(key=lambda hit: (-float(hit["score"]), str(hit["doc_id"])))
    total_hits = len(hits)
    return {
        "mode": mode,
        "terms": term_rows,
        "limit": limit,
        "total_hits": total_hits,
        "hits": hits[:limit],
    }


def _make_hit(doc_id: str, components: Iterable[tuple[int, str, int, int]]) -> dict[str, Any]:
    ordered = sorted(components, key=lambda item: item[0])
    component_rows = [
        {
            "term_id": term_id,
            "term": term,
            "quantized": quantized,
            "scale": scale,
            "weight": round(quantized / scale, 12),
        }
        for term_id, term, quantized, scale in ordered
    ]
    score = round(math.fsum(row["weight"] for row in component_rows), 12)
    return {"doc_id": doc_id, "score": score, "components": component_rows}


class QueryEngine:
    def __init__(self, config: IndexConfig, artifacts_dir: str | Path):
        self.config = config
        self.artifacts_dir = Path(artifacts_dir).resolve()
        self.stage2 = load_stage2(self.artifacts_dir / "stage2")
        self.manifest, self.binary_path = load_stage3(self.artifacts_dir / "stage3")
        self.terms_by_id = {int(term["term_id"]): term for term in self.manifest["terms"]}
        self.terms_by_name = {
            str(term["term"]): term
            for term in self.manifest["terms"]
        }
        self.ordinal_to_doc_id = list(self.manifest["document_order"])
        self.max_limit = int(config.value("query.max_limit", 1000))
        self.latency_budget_ms = float(config.value("query.latency_budget_ms", 1000.0))
        if not math.isfinite(self.latency_budget_ms) or self.latency_budget_ms < 0:
            raise error(
                "E_CONFIG",
                "查询延迟预算必须为非负有限数值",
                latency_budget_ms=self.latency_budget_ms,
            )
        capacity = int(config.value("query.cache_capacity", 32))
        self.cache = QueryCache(
            self.artifacts_dir / "cache" / "query_cache.json",
            capacity,
            config.config_fingerprint,
        )
        self._artifact_summary = self._build_artifact_summary()

    def _build_artifact_summary(self) -> dict[str, Any]:
        from .utils import sha256_file

        paths = {
            "tokenize": self.artifacts_dir / "stage1" / "terms.json",
            "statistics": self.artifacts_dir / "stage2" / "statistics.json",
            "postings": self.binary_path,
            "postings_manifest": self.artifacts_dir / "stage3" / "postings.json",
            "query_summary": self.artifacts_dir / "stage4" / "query_results.json",
        }
        digests = {
            stage: sha256_file(path)
            for stage, path in paths.items()
            if path.exists()
        }
        return {
            "stage_fingerprints": self.config.stage_fingerprints(),
            "artifact_digests": digests,
            "postings_binary_digest": self.manifest.get("binary_digest"),
        }

    def _validate(self, terms: Any, mode: Any, limit: Any) -> tuple[list[str], str, int]:
        if not isinstance(terms, list) or not terms:
            raise error("E_QUERY_OUT_OF_BOUNDS", "查询词项数量必须在 1 以上", terms=terms)
        if len(terms) > 64:
            raise error(
                "E_QUERY_OUT_OF_BOUNDS",
                "查询词项数量超过上限",
                term_count=len(terms),
                max_terms=64,
            )
        if not all(isinstance(term, str) and term.strip() for term in terms):
            raise error("E_QUERY_OUT_OF_BOUNDS", "查询词项必须是非空字符串", terms=terms)
        normalized_mode = str(mode).lower()
        if normalized_mode not in ("and", "or"):
            raise error("E_QUERY_OUT_OF_BOUNDS", "查询语义必须是 and 或 or", mode=mode)
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise error("E_QUERY_OUT_OF_BOUNDS", "limit 必须是整数", limit=limit)
        if limit < 1 or limit > self.max_limit:
            raise error(
                "E_QUERY_OUT_OF_BOUNDS",
                "limit 超出允许范围",
                limit=limit,
                min_limit=1,
                max_limit=self.max_limit,
            )
        normalized_terms = sorted({normalize_token(term) for term in terms})
        if any(not term for term in normalized_terms):
            raise error("E_QUERY_OUT_OF_BOUNDS", "规范化后查询词项为空", terms=terms)
        return normalized_terms, normalized_mode, limit

    def _cache_key(self, terms: list[str], term_ids: list[int | None], mode: str, limit: int) -> str:
        payload = {
            "terms": terms,
            "term_ids": term_ids,
            "mode": mode,
            "limit": limit,
            "config_fingerprint": self.config.config_fingerprint,
            "artifact_summary": self._artifact_summary,
        }
        return sha256_bytes(canonical_json_bytes(payload))

    def _deadline(self) -> float:
        return time.perf_counter() + self.latency_budget_ms / 1000.0

    def _check_deadline(self, query_id: str, started: float, deadline: float) -> None:
        if time.perf_counter() > deadline:
            raise error(
                "E_LATENCY_BUDGET",
                "查询超过配置的延迟预算",
                query_id=query_id,
                elapsed_ms=round((time.perf_counter() - started) * 1000.0, 6),
                latency_budget_ms=self.latency_budget_ms,
            )

    def execute(
        self,
        *,
        query_id: str,
        terms: list[str],
        mode: str = "and",
        limit: int = 100,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        deadline = started + self.latency_budget_ms / 1000.0
        normalized_terms, normalized_mode, normalized_limit = self._validate(terms, mode, limit)
        resolved = [(term, self.terms_by_name.get(term)) for term in normalized_terms]
        term_ids = [int(term["term_id"]) if term is not None else None for _, term in resolved]
        term_rows = [
            {
                "term": term,
                "term_id": int(found["term_id"]) if found is not None else None,
            }
            for term, found in resolved
        ]
        cache_key = self._cache_key(normalized_terms, term_ids, normalized_mode, normalized_limit)
        cache_hit = False
        if use_cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                cache_hit = True
                self._check_deadline(query_id, started, deadline)
                return {"result": cached, "meta": self._meta(query_id, started, cache_hit)}
        self._check_deadline(query_id, started, deadline)

        known = [found for _, found in resolved if found is not None]
        if normalized_mode == "and" and len(known) != len(resolved):
            result = _result_from_hits(
                mode=normalized_mode,
                term_rows=term_rows,
                limit=normalized_limit,
                hits=[],
            )
        elif not known:
            result = _result_from_hits(
                mode=normalized_mode,
                term_rows=term_rows,
                limit=normalized_limit,
                hits=[],
            )
        else:
            known.sort(key=lambda term: (int(term["document_frequency"]), int(term["term_id"])))
            cursors: list[PostingCursor] = []
            try:
                for term in known:
                    cursors.append(
                        PostingCursor(
                            term,
                            self.binary_path,
                            self.ordinal_to_doc_id,
                            deadline=deadline,
                        )
                    )
                if normalized_mode == "and":
                    hits = self._intersect(cursors, query_id, started, deadline)
                else:
                    hits = self._union(cursors, query_id, started, deadline)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if getattr(exc, "code", None) == "E_LATENCY_BUDGET":
                    raise error(
                        "E_LATENCY_BUDGET",
                        "查询超过配置的延迟预算",
                        query_id=query_id,
                        elapsed_ms=round((time.perf_counter() - started) * 1000.0, 6),
                        latency_budget_ms=self.latency_budget_ms,
                    )
                raise
            finally:
                for cursor in cursors:
                    cursor.close()
            result = _result_from_hits(
                mode=normalized_mode,
                term_rows=term_rows,
                limit=normalized_limit,
                hits=hits,
            )
        self._check_deadline(query_id, started, deadline)
        if use_cache:
            self.cache.put(cache_key, result)
        return {"result": result, "meta": self._meta(query_id, started, cache_hit)}

    def _meta(self, query_id: str, started: float, cache_hit: bool) -> dict[str, Any]:
        return {
            "query_id": query_id,
            "cache_hit": cache_hit,
            "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 6),
            "latency_budget_ms": self.latency_budget_ms,
            "cache": self.cache.stats(),
        }

    def _intersect(
        self,
        cursors: list[PostingCursor],
        query_id: str,
        started: float,
        deadline: float,
    ) -> list[dict[str, Any]]:
        lead = cursors[0]
        others = cursors[1:]
        hits: list[dict[str, Any]] = []
        iterations = 0
        while lead.valid:
            iterations += 1
            if iterations % 256 == 0:
                self._check_deadline(query_id, started, deadline)
            candidate = lead.current.doc_id
            matched = True
            for cursor in others:
                cursor.seek(candidate)
                if not cursor.valid:
                    matched = False
                    lead.block_postings = []
                    break
                if cursor.current.doc_id != candidate:
                    lead.seek(cursor.current.doc_id)
                    matched = False
                    break
            if not matched:
                continue
            components = [
                (cursor.term_id, str(cursor.term["term"]), cursor.current.weight, int(cursor.term["quant_scale"]))
                for cursor in cursors
            ]
            hits.append(_make_hit(candidate, components))
            lead.advance()
        return hits

    def _union(
        self,
        cursors: list[PostingCursor],
        query_id: str,
        started: float,
        deadline: float,
    ) -> list[dict[str, Any]]:
        import heapq

        heap: list[tuple[str, int]] = []
        for index, cursor in enumerate(cursors):
            if cursor.valid:
                heapq.heappush(heap, (cursor.current.doc_id, index))
        hits: list[dict[str, Any]] = []
        iterations = 0
        while heap:
            iterations += 1
            if iterations % 256 == 0:
                self._check_deadline(query_id, started, deadline)
            doc_id = heap[0][0]
            members: list[tuple[PostingCursor, int]] = []
            while heap and heap[0][0] == doc_id:
                _, index = heapq.heappop(heap)
                cursor = cursors[index]
                consumed_weight = cursor.current.weight
                members.append((cursor, consumed_weight))
                cursor.advance()
                if cursor.valid:
                    heapq.heappush(heap, (cursor.current.doc_id, index))
            components = [
                (cursor.term_id, str(cursor.term["term"]), consumed_weight, int(cursor.term["quant_scale"]))
                for cursor, consumed_weight in members
            ]
            hits.append(_make_hit(doc_id, components))
        return hits


def term_rows_for_query(engine: QueryEngine, terms: list[str]) -> list[dict[str, Any]]:
    return [
        {
            "term": normalize_token(term),
            "term_id": int(engine.terms_by_name[normalize_token(term)]["term_id"])
            if normalize_token(term) in engine.terms_by_name
            else None,
        }
        for term in sorted(set(terms), key=normalize_token)
    ]
