from __future__ import annotations

import json
import math
import queue
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .cache import QueryCache
from .canonical import PipelineError, canonical_bytes
from .pipeline import Pipeline


class QueryService:
    def __init__(self, run_dir: Path | str) -> None:
        self.pipeline = Pipeline(run_dir)

    def _cache(self) -> QueryCache:
        config = self.pipeline.load_config()
        return QueryCache(
            self.pipeline.run_dir, int(config.values["cache_max_entries"])
        )

    def _budget(self, requested: float | None) -> float:
        if requested is None:
            config = self.pipeline.load_config()
            return float(config.values["latency_budget_ms"])
        if isinstance(requested, bool) or not isinstance(requested, (int, float)):
            raise PipelineError(
                "E_INVALID_CONFIG", "budget_ms 必须是数值。", {"budget_ms": requested}
            )
        if not math.isfinite(float(requested)) or float(requested) < 0.0:
            raise PipelineError(
                "E_INVALID_CONFIG", "budget_ms 不能为负数。", {"budget_ms": requested}
            )
        return float(requested)

    @staticmethod
    def _error_payload(exc: PipelineError) -> dict[str, Any]:
        return {"code": exc.code, "message": exc.message, "details": exc.details}

    def _latency_error(
        self, query_id: str, started: float, budget_ms: float
    ) -> dict[str, Any]:
        elapsed = max(0.0, time.perf_counter() - started)
        return {
            "code": "E_LATENCY_BUDGET",
            "message": "查询超出交互延迟预算，已返回超时错误而未继续阻塞等待。",
            "query_id": query_id,
            "budget_ms": budget_ms,
            "elapsed_ms": round(elapsed * 1000.0, 6),
            "elapsed_us": int(elapsed * 1_000_000),
        }

    def _run_with_budget(
        self,
        function: Callable[[], dict[str, Any]],
        *,
        requested_budget_ms: float | None,
        query_id: str | None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        identifier = (
            query_id
            if isinstance(query_id, str) and query_id
            else f"qry-{uuid.uuid4().hex}"
        )
        try:
            budget_ms = self._budget(requested_budget_ms)
        except PipelineError as exc:
            return self._error_payload(exc)
        if budget_ms <= 0.0:
            return self._latency_error(identifier, started, budget_ms)

        result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def worker() -> None:
            try:
                result_queue.put((True, function()))
            except PipelineError as exc:
                result_queue.put((False, exc))
            except Exception as exc:  # pragma: no cover - defensive boundary
                result_queue.put(
                    (
                        False,
                        PipelineError(
                            "E_INTERNAL",
                            "查询执行失败。",
                            {"error": f"{type(exc).__name__}: {exc}"},
                        ),
                    )
                )

        thread = threading.Thread(target=worker, name=f"query-{identifier}", daemon=True)
        thread.start()
        try:
            ok, value = result_queue.get(timeout=budget_ms / 1000.0)
        except queue.Empty:
            return self._latency_error(identifier, started, budget_ms)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if not ok:
            assert isinstance(value, PipelineError)
            return self._error_payload(value)
        if elapsed_ms > budget_ms:
            return self._latency_error(identifier, started, budget_ms)
        return value

    def _snapshot(self) -> dict[str, Any]:
        config = self.pipeline.load_config()
        state = self.pipeline.load_state()
        _, input_fingerprint = self.pipeline.load_input()
        earliest = self.pipeline.determine_invalidation(
            state, input_fingerprint, config
        )
        return {
            "config": config,
            "state": state,
            "input_fingerprint": input_fingerprint,
            "earliest": earliest,
            "invalidation_reason": self.pipeline._invalidation_reason(
                state, input_fingerprint, config, earliest
            ),
        }

    def _invalidation(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        earliest = snapshot["earliest"]
        return {
            "stale": earliest > 0,
            "earliest_affected_stage": earliest,
            "reason": snapshot["invalidation_reason"],
        }

    def _sample_payload(
        self, sample_id: str, use_cache: bool
    ) -> dict[str, Any]:
        snapshot = self._snapshot()
        state = snapshot["state"]
        metadata = state.get("stages", {}).get("2", {})
        if metadata.get("status") != "complete" or not self.pipeline._artifact_is_valid(2, state):
            raise PipelineError(
                "E_STAGE_NOT_READY", "阶段 2 产物尚未完成，无法查询判定依据。", {"stage": 2}
            )
        stage2 = self.pipeline.load_stage_payload(2)
        records = {item["sample_id"]: item for item in stage2.get("records", [])}
        record = records.get(sample_id)
        if record is None:
            raise PipelineError(
                "E_SAMPLE_NOT_FOUND", "未找到指定样本。", {"sample_id": sample_id}
            )

        cache = self._cache()
        key = cache.make_key(
            kind="sample",
            content_fingerprint=record["content_fingerprint"],
            config_version=snapshot["config"].version,
            stage_fingerprint=metadata["artifact_digest"],
            scope=sample_id,
        )
        if use_cache:
            cached = cache.get(key)
            if cached is not None:
                return json.loads(cached.decode("utf-8"))

        stage4_record: dict[str, Any] | None = None
        source_review: dict[str, Any] | None = None
        if self.pipeline._artifact_is_valid(4, state):
            stage4 = self.pipeline.load_stage_payload(4)
            source_reviews = {
                item["source"]: item for item in stage4.get("source_reviews", [])
            }
            source_review = source_reviews.get(record["source"])
            review_items = {
                item["sample_id"]: item
                for item in stage4.get("samples_requiring_review", [])
            }
            stage4_record = review_items.get(sample_id)

        reasons = set(record.get("review_reasons", []))
        if stage4_record:
            reasons.update(stage4_record.get("reasons", []))
        stage1_record: dict[str, Any] | None = None
        if self.pipeline._artifact_is_valid(1, state):
            stage1 = self.pipeline.load_stage_payload(1)
            stage1_record = next(
                (item for item in stage1.get("records", []) if item["sample_id"] == sample_id),
                None,
            )

        payload = {
            "code": "OK",
            "sample_id": sample_id,
            "content_fingerprint": record["content_fingerprint"],
            "config_version": snapshot["config"].version,
            "stage_fingerprint": metadata["artifact_digest"],
            "source": record["source"],
            "lang": record["lang"],
            "detection": {
                "status": record["status"],
                "hallucination": record["hallucination"],
                "consistency_score": record["consistency_score"],
                "unsupported_ratio": record["unsupported_ratio"],
                "thresholds": record["thresholds"],
                "sentence_support": record["sentence_support"],
                "errors": record["errors"],
            },
            "manual_annotation": record["annotation"],
            "normalization": None
            if stage1_record is None
            else {
                "normalized": stage1_record["normalized"],
                "tokens": stage1_record["tokens"],
                "sentences": stage1_record["sentences"],
            },
            "review": {
                "required": bool(reasons),
                "reasons": sorted(reasons),
                "source_review": source_review,
            },
            "invalidation": self._invalidation(snapshot),
        }
        if use_cache:
            cache.put(key, canonical_bytes(payload))
        return payload

    def _summary_payload(
        self,
        source: str | None,
        lang: str | None,
        use_cache: bool,
    ) -> dict[str, Any]:
        snapshot = self._snapshot()
        state = snapshot["state"]
        metadata = state.get("stages", {}).get("3", {})
        if metadata.get("status") != "complete" or not self.pipeline._artifact_is_valid(3, state):
            raise PipelineError(
                "E_STAGE_NOT_READY", "阶段 3 汇总产物尚未完成。", {"stage": 3}
            )
        stage3 = self.pipeline.load_stage_payload(3)
        cache = self._cache()
        scope = json.dumps(
            {"source": source, "lang": lang}, sort_keys=True, ensure_ascii=False
        )
        key = cache.make_key(
            kind="summary",
            content_fingerprint=snapshot["input_fingerprint"],
            config_version=snapshot["config"].version,
            stage_fingerprint=metadata["artifact_digest"],
            scope=scope,
        )
        if use_cache:
            cached = cache.get(key)
            if cached is not None:
                return json.loads(cached.decode("utf-8"))

        strata = stage3["strata"]
        if source is not None:
            strata = [item for item in strata if item["source"] == source]
        if lang is not None:
            strata = [item for item in strata if item["lang"] == lang]
        payload = {
            "code": "OK",
            "config_version": snapshot["config"].version,
            "stage_fingerprint": metadata["artifact_digest"],
            "input_fingerprint": snapshot["input_fingerprint"],
            "filter": {"source": source, "lang": lang},
            "quantization_scale": stage3["quantization_scale"],
            "log_accumulation": stage3["log_accumulation"],
            "strata": strata,
            "totals": stage3["totals"] if source is None and lang is None else None,
            "invalidation": self._invalidation(snapshot),
        }
        if use_cache:
            cache.put(key, canonical_bytes(payload))
        return payload

    def query_sample(
        self,
        sample_id: str,
        *,
        budget_ms: float | None = None,
        query_id: str | None = None,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        return self._run_with_budget(
            lambda: self._sample_payload(sample_id, use_cache),
            requested_budget_ms=budget_ms,
            query_id=query_id,
        )

    def query_summary(
        self,
        *,
        source: str | None = None,
        lang: str | None = None,
        budget_ms: float | None = None,
        query_id: str | None = None,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        return self._run_with_budget(
            lambda: self._summary_payload(source, lang, use_cache),
            requested_budget_ms=budget_ms,
            query_id=query_id,
        )

    def clear_cache(self) -> dict[str, Any]:
        cache = self._cache()
        cache.clear()
        return {"code": "OK", "cache": cache.stats()}

    def cache_stats(self) -> dict[str, Any]:
        return {"code": "OK", **self._cache().stats()}
