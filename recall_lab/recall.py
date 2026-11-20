from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from typing import Any

from .budget import RecallDecision
from .config import SOURCE_ORDER
from .data import InputBundle, Item, Query
from .errors import BusinessError, ErrorCode
from .util import ensure_finite


TOKEN_RE = re.compile(r"[A-Za-z0-9]+|[\u3400-\u9fff]")


def tokenize(text: str) -> tuple[str, ...]:
    normalized = unicodedata.normalize("NFKC", text).casefold()
    return tuple(TOKEN_RE.findall(normalized))


def _checked_score(value: float, *, query_id: str, source: str, item_id: str) -> float:
    return ensure_finite(value, field="recall_score", query_id=query_id, source=source, item_id=item_id)


def _checked_cosine(numerator: float, left_norm: float, right_norm: float) -> float:
    denominator = left_norm * right_norm
    return 0.0 if denominator == 0.0 else numerator / denominator


@dataclass(frozen=True)
class _VectorItem:
    item: Item
    norm: float
    popularity_factor: float


class RecallEngine:
    """Deterministic three-route in-memory recall implementation."""

    def __init__(self, bundle: InputBundle):
        self.bundle = bundle
        self.items = bundle.items
        self.queries = bundle.queries
        self.item_by_id = {item.item_id: item for item in self.items}
        self.feature_dim = bundle.feature_dim
        self._category_items: dict[str, list[Item]] = {}
        self._category_counts: Counter[str] = Counter()
        self._history_frequency: Counter[str] = Counter()
        for item in self.items:
            self._category_items.setdefault(item.category, []).append(item)
            self._category_counts[item.category] += 1
        for query in self.queries:
            self._history_frequency.update(query.history_categories)

        self._item_terms: dict[str, frozenset[str]] = {}
        self._postings: dict[str, list[str]] = {}
        for item in self.items:
            terms = frozenset(tokenize(item.title))
            self._item_terms[item.item_id] = terms
            for term in terms:
                self._postings.setdefault(term, []).append(item.item_id)
        for postings in self._postings.values():
            postings.sort()

        document_count = len(self.items)
        self._idf = {
            term: math.log((document_count + 1.0) / (len(postings) + 1.0)) + 1.0
            for term, postings in self._postings.items()
        }

        vector_items: list[_VectorItem] = []
        raw_norms: list[float] = []
        for item in self.items:
            self._validate_features(item.features, query_id=None, item_id=item.item_id)
            norm = math.sqrt(math.fsum(value * value for value in item.features))
            raw_norms.append(norm)
            vector_items.append(_VectorItem(item=item, norm=norm, popularity_factor=1.0))
        max_norm = max(raw_norms, default=0.0)
        if max_norm > 0:
            vector_items = [
                _VectorItem(
                    item=entry.item,
                    norm=entry.norm,
                    popularity_factor=1.0 + 0.05 * min(1.0, entry.norm / max_norm),
                )
                for entry in vector_items
            ]
        self._vector_items = tuple(vector_items)
        self._popularity_by_id = {
            entry.item.item_id: entry.popularity_factor for entry in self._vector_items
        }
        self._category_items = {
            category: sorted(items, key=lambda item: item.item_id)
            for category, items in self._category_items.items()
        }

    def _validate_features(self, features: tuple[float, ...], *, query_id: str | None, item_id: str | None) -> None:
        if len(features) != self.feature_dim:
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "feature dimension mismatch",
                {"query_id": query_id, "item_id": item_id, "expected": self.feature_dim, "actual": len(features)},
            )
        for index, value in enumerate(features):
            if not math.isfinite(float(value)):
                raise BusinessError(
                    ErrorCode.NUMERIC,
                    "non-finite feature detected",
                    {"query_id": query_id, "item_id": item_id, "feature_index": index, "value": str(value)},
                )

    def _validate_query(self, query: Query) -> None:
        if not query.text.strip():
            raise BusinessError(
                ErrorCode.EMPTY_INPUT,
                "query text is empty",
                {"query_id": query.query_id, "scale": self.bundle.entry.scale, "reason": "EMPTY_QUERY"},
            )
        self._validate_features(query.features, query_id=query.query_id, item_id=None)

    def recall_all(self, decision: RecallDecision) -> dict[str, Any]:
        if not self.items:
            query_id = self.queries[0].query_id if self.queries else None
            raise BusinessError(
                ErrorCode.EMPTY_INPUT,
                "item library is empty",
                {"query_id": query_id, "scale": self.bundle.entry.scale, "reason": "EMPTY_ITEM_LIBRARY"},
            )
        for query in self.queries:
            self._validate_query(query)

        query_results: list[dict[str, Any]] = []
        emitted_by_source = Counter()
        for query in self.queries:
            routes: dict[str, Any] = {}
            for source in SOURCE_ORDER:
                route_decision = decision.routes[source]
                if not route_decision.enabled:
                    routes[source] = {
                        "status": "disabled",
                        "enabled": False,
                        "allocated_depth": 0,
                        "emitted": 0,
                        "reason": route_decision.reason,
                        "reason_text": route_decision.reason_text,
                        "candidates": [],
                    }
                    continue
                if source == "lexical":
                    candidates = self._lexical_candidates(query, route_decision.effective_depth)
                    method = "idf_weighted_token_overlap"
                elif source == "category":
                    candidates = self._category_candidates(query, route_decision.effective_depth)
                    method = "history_category_frequency"
                else:
                    candidates = self._vector_candidates(query, route_decision.effective_depth)
                    method = "cosine_similarity"
                emitted_by_source[source] += len(candidates)
                routes[source] = {
                    "status": "ok",
                    "enabled": True,
                    "allocated_depth": route_decision.effective_depth,
                    "emitted": len(candidates),
                    "reason": route_decision.reason,
                    "reason_text": route_decision.reason_text,
                    "method": method,
                    "candidates": candidates,
                }
            query_results.append(
                {
                    "query_id": query.query_id,
                    "query_text": query.text,
                    "locale": query.locale,
                    "channel": query.channel,
                    "history_categories": list(query.history_categories),
                    "routes": routes,
                }
            )

        planned_by_source = {
            source: decision.routes[source].planned_evaluations for source in SOURCE_ORDER
        }
        for source in SOURCE_ORDER:
            if emitted_by_source[source] > planned_by_source[source]:
                raise BusinessError(
                    ErrorCode.INTERNAL,
                    "recall emitted more candidates than its allocation",
                    {
                        "source": source,
                        "emitted": emitted_by_source[source],
                        "planned": planned_by_source[source],
                    },
                )
        emitted_total = sum(emitted_by_source.values())
        if emitted_total > decision.global_budget:
            raise BusinessError(
                ErrorCode.INTERNAL,
                "recall emitted more candidates than the global budget",
                {"emitted": emitted_total, "budget": decision.global_budget},
            )

        return {
            "format": "recall-artifact-v1",
            "scale": self.bundle.entry.scale,
            "seed": self.bundle.entry.seed,
            "item_count": len(self.items),
            "query_count": len(self.queries),
            "decision": decision.as_dict(),
            "budget_audit": {
                "planned_total": decision.used_evaluations,
                "emitted_total": emitted_total,
                "global_budget": decision.global_budget,
                "within_budget": emitted_total <= decision.global_budget,
                "by_source": {
                    source: {
                        "planned": planned_by_source[source],
                        "emitted": emitted_by_source[source],
                        "quota": decision.routes[source].planned_evaluations,
                        "within_quota": emitted_by_source[source] <= planned_by_source[source],
                    }
                    for source in SOURCE_ORDER
                },
            },
            "queries": query_results,
        }

    def _lexical_candidates(self, query: Query, depth: int) -> list[dict[str, Any]]:
        query_terms = sorted(set(tokenize(query.text)))
        scores: dict[str, float] = {}
        matches: dict[str, list[dict[str, Any]]] = {}
        for term in query_terms:
            idf = self._idf.get(term)
            if idf is None:
                continue
            for item_id in self._postings.get(term, ()):
                scores[item_id] = scores.get(item_id, 0.0) + idf
                matches.setdefault(item_id, []).append({"term": term, "idf": idf, "contribution": idf})
        scored: list[tuple[float, str]] = []
        for item_id, raw_score in scores.items():
            score = _checked_score(raw_score, query_id=query.query_id, source="lexical", item_id=item_id)
            scored.append((score, item_id))
        scored.sort(key=lambda pair: (-pair[0], pair[1]))
        result: list[dict[str, Any]] = []
        for rank, (score, item_id) in enumerate(scored[:depth], start=1):
            term_details = sorted(matches[item_id], key=lambda detail: detail["term"])
            result.append(
                {
                    "item_id": item_id,
                    "rank": rank,
                    "score": score,
                    "detail": {
                        "matched_terms": term_details,
                        "matched_term_count": len(term_details),
                        "formula": "sum(idf(matched_term))",
                    },
                }
            )
        return result

    def _category_candidates(self, query: Query, depth: int) -> list[dict[str, Any]]:
        history_weights: dict[str, float] = {}
        for index, category in enumerate(query.history_categories):
            history_weights[category] = history_weights.get(category, 0.0) + 1.0 / (index + 1.0)
        matched_categories = sorted(history_weights)
        scored: list[tuple[float, str]] = []
        for category in matched_categories:
            history_weight = history_weights[category]
            category_frequency = self._history_frequency.get(category, 0)
            category_prior = math.log1p(category_frequency)
            for vector_item in self._vector_items:
                item = vector_item.item
                if item.category != category:
                    continue
                raw_score = history_weight * category_prior * vector_item.popularity_factor
                score = _checked_score(raw_score, query_id=query.query_id, source="category", item_id=item.item_id)
                scored.append((score, item.item_id))
        scored.sort(key=lambda pair: (-pair[0], pair[1]))
        result: list[dict[str, Any]] = []
        for rank, (score, item_id) in enumerate(scored[:depth], start=1):
            item = self.item_by_id[item_id]
            category = item.category
            position_weight = history_weights.get(category, 0.0)
            popularity_factor = self._popularity_by_id[item_id]
            result.append(
                {
                    "item_id": item_id,
                    "rank": rank,
                    "score": score,
                    "detail": {
                        "category": category,
                        "history_position_weight": position_weight,
                        "history_category_frequency": self._history_frequency.get(category, 0),
                        "category_prior": math.log1p(self._history_frequency.get(category, 0)),
                        "popularity_factor": popularity_factor,
                        "formula": "history_position_weight * log1p(history_category_frequency) * popularity_factor",
                    },
                }
            )
        return result

    def _vector_candidates(self, query: Query, depth: int) -> list[dict[str, Any]]:
        query_norm = math.sqrt(math.fsum(value * value for value in query.features))
        scored: list[tuple[float, str, dict[str, float]]] = []
        for vector_item in self._vector_items:
            dot = math.fsum(
                query_feature * item_feature
                for query_feature, item_feature in zip(query.features, vector_item.item.features)
            )
            cosine = _checked_cosine(dot, query_norm, vector_item.norm)
            score = _checked_score(
                cosine,
                query_id=query.query_id,
                source="vector",
                item_id=vector_item.item.item_id,
            )
            scored.append(
                (
                    score,
                    vector_item.item.item_id,
                    {"dot_product": dot, "query_norm": query_norm, "item_norm": vector_item.norm},
                )
            )
        scored.sort(key=lambda entry: (-entry[0], entry[1]))
        result: list[dict[str, Any]] = []
        for rank, (score, item_id, detail) in enumerate(scored[:depth], start=1):
            result.append(
                {
                    "item_id": item_id,
                    "rank": rank,
                    "score": score,
                    "detail": {**detail, "formula": "dot(q,i) / (||q|| * ||i||)"},
                }
            )
        return result
