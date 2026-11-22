"""Interactive query engine with latency budget and deterministic caching.

Every query runs against a deadline derived from the configured latency
budget.  When the deadline is exceeded the query aborts with a structured
E_LATENCY_BUDGET error carrying the query id and the elapsed time instead
of blocking.  Results are served through the LRU cache whose keys embed
the sample fingerprint, config version and stage fingerprint, so a cache
hit is byte-identical to a fresh computation.
"""
from __future__ import annotations

import time

import numpy as np

from .cache import QueryCache
from .errors import E_INVALID_STAGE, E_LATENCY_BUDGET, PipelineError
from .fingerprint import canonical_json_bytes

QUERY_KINDS = ("feature_summary", "explained_variance", "combinations")


class QueryEngine:
    def __init__(self, pipeline, cache: QueryCache, latency_budget_ms: float) -> None:
        self.pipeline = pipeline
        self.cache = cache
        self.latency_budget_ms = float(latency_budget_ms)
        self._counter = 0
        self.traces: list[dict] = []

    # ------------------------------------------------------------- helpers
    def _next_query_id(self) -> str:
        self._counter += 1
        return f"q-{self._counter:06d}"

    def _check_deadline(self, query_id: str, kind: str, started: float) -> None:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if elapsed_ms > self.latency_budget_ms:
            raise PipelineError(
                E_LATENCY_BUDGET,
                f"query {query_id} exceeded the latency budget",
                query_id=query_id,
                kind=kind,
                elapsed_ms=round(elapsed_ms, 3),
                budget_ms=self.latency_budget_ms,
            )

    def _stage_fingerprint(self, kind: str) -> str:
        fps = self.pipeline.stage_fingerprints()
        return fps["reduce"] if kind != "combinations" else fps["cross"]

    # --------------------------------------------------------------- query
    def query(self, kind: str, **params) -> dict:
        if kind not in QUERY_KINDS:
            raise PipelineError(
                E_INVALID_STAGE, f"unknown query kind {kind!r}", kind=kind,
                known=list(QUERY_KINDS),
            )
        query_id = self._next_query_id()
        started = time.perf_counter()
        key = self.cache.make_key(
            kind=kind,
            params=params,
            sample_fingerprint=self.pipeline.sample_fingerprint,
            config_version=self.pipeline.config_version,
            stage_fingerprint=self._stage_fingerprint(kind),
        )
        self._check_deadline(query_id, kind, started)

        def compute() -> bytes:
            self._check_deadline(query_id, kind, started)
            payload = self._compute_payload(kind, params)
            self._check_deadline(query_id, kind, started)
            return canonical_json_bytes(payload)

        raw, hit = self.cache.get_or_compute(key, compute)
        self._check_deadline(query_id, kind, started)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        trace = {
            "query_id": query_id,
            "kind": kind,
            "params": params,
            "cache_hit": hit,
            "elapsed_ms": round(elapsed_ms, 3),
            "budget_ms": self.latency_budget_ms,
            "status": "ok",
        }
        self.traces.append(trace)
        import json

        return {
            "query_id": query_id,
            "cache_hit": hit,
            "elapsed_ms": trace["elapsed_ms"],
            "budget_ms": self.latency_budget_ms,
            "result": json.loads(raw.decode("utf-8")),
        }

    # ------------------------------------------------------------- payloads
    def _compute_payload(self, kind: str, params: dict) -> dict:
        if kind == "feature_summary":
            return self._feature_summary(params)
        if kind == "explained_variance":
            return self._explained_variance(params)
        return self._combinations(params)

    def _feature_summary(self, params: dict) -> dict:
        reduce_art = self.pipeline.load_artifact("reduce")["artifact"]
        method = params.get("method")
        out = []
        for cand in reduce_art["candidates"]:
            if method and cand["method"] != method:
                continue
            projected = np.asarray(cand["projected"]["values"], dtype=np.float64)
            scales = np.asarray(cand["projected"]["scales"], dtype=np.float64)
            values = projected / scales
            out.append({
                "method": cand["method"],
                "target_dim": cand["target_dim"],
                "projected_fingerprint": cand["projected_fingerprint"],
                "column_means": [round(float(v), 10) for v in values.mean(axis=0)],
                "column_stds": [round(float(v), 10) for v in values.std(axis=0)],
                "error_bound": cand["error_bound"],
            })
        if method and not out:
            raise PipelineError(
                E_INVALID_STAGE, f"no reduction candidate for method {method!r}",
                method=method,
            )
        return {"kind": "feature_summary", "candidates": out}

    def _explained_variance(self, params: dict) -> dict:
        reduce_art = self.pipeline.load_artifact("reduce")["artifact"]
        return {
            "kind": "explained_variance",
            "candidates": [
                {
                    "method": cand["method"],
                    "explained_variance_ratio": cand["explained_variance_ratio"],
                    "components_fingerprint": cand["components_fingerprint"],
                }
                for cand in reduce_art["candidates"]
            ],
        }

    def _combinations(self, params: dict) -> dict:
        cross_art = self.pipeline.load_artifact("cross")["artifact"]
        limit = int(params.get("limit", len(cross_art["combinations"])))
        return {
            "kind": "combinations",
            "combinations": cross_art["combinations"][:limit],
            "truncated": cross_art["truncated"],
            "feature_count": len(cross_art["features"]),
        }
