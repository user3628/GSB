"""End-to-end query understanding with phase-level versioned caching."""

from __future__ import annotations

import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any

from .assets import AssetSnapshot, AssetStore
from .classifier import NaiveBayesIntentScorer
from .config import config_slice, invalid_locale_error, load_config
from .errors import QueryError, RequestError
from .fallback import decide_intent
from .io import canonical_json, sha256_bytes, sha256_json
from .normalization import normalize_and_tokenize
from .slots import SlotExtractor

STAGES: tuple[str, ...] = ("normalization", "intent", "slots", "decision")


class QueryEngine:
    """A local engine whose cache is always gated by current stage fingerprints."""

    def __init__(
        self,
        *,
        config_path: str | Path,
        training_path: str | Path,
        dictionary_path: str | Path,
        cache_limit: int = 256,
    ) -> None:
        self.store = AssetStore(
            config_path=config_path,
            training_path=training_path,
            dictionary_path=dictionary_path,
        )
        self.cache_limit = cache_limit
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._lock = threading.RLock()
        self.snapshot: AssetSnapshot | None = None
        self.config: dict[str, Any] = {}
        self.scorer: NaiveBayesIntentScorer | None = None
        self.slot_extractor: SlotExtractor | None = None
        self.versions: dict[str, Any] = {}
        self.overall_fingerprint = ""
        self._install(self.store.read())

    def parse(self, sample: dict[str, Any], faults: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            self._ensure_fresh()
            return self._parse_locked(sample, faults or {})

    def parse_many(self, samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.parse(sample) for sample in samples]

    def refresh(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            before = self.versions
            snapshot = self.store.read()
            changed = force or self.snapshot is None or snapshot.signature != self.snapshot.signature
            if changed:
                self._install(snapshot)
            return {
                "changed": bool(changed and before != self.versions),
                "fingerprint_before": _overall_from_versions(before),
                "fingerprint_after": self.overall_fingerprint,
                "versions": self.versions,
            }

    def version_status(self) -> dict[str, Any]:
        with self._lock:
            self._ensure_fresh()
            assert self.snapshot is not None
            assert self.scorer is not None
            assert self.slot_extractor is not None
            return {
                "assets": self.snapshot.digests,
                "cache": {
                    "entry_count": len(self._cache),
                    "limit": self.cache_limit,
                },
                "config_fingerprint": self.overall_fingerprint,
                "dictionary": self.slot_extractor.summarize(),
                "model": self.scorer.model_summary(),
                "paths": {
                    "config": str(self.store.config_path),
                    "dictionary": str(self.store.dictionary_path),
                    "training": str(self.store.training_path),
                },
                "schema_version": "qiu-version-v1",
                "stage_fingerprints": {stage: self.versions[stage]["fingerprint"] for stage in STAGES},
                "versions": self.versions,
            }

    def _ensure_fresh(self) -> None:
        snapshot = self.store.read()
        if self.snapshot is None or snapshot.signature != self.snapshot.signature:
            self._install(snapshot)

    def _install(self, snapshot: AssetSnapshot) -> None:
        config = load_config(snapshot.config)
        normalization_config = config_slice(config, "normalization")
        classifier_config = config_slice(config, "classifier")
        slot_config = config_slice(config, "slots")
        policy_config = config_slice(config, "policy")

        scorer = NaiveBayesIntentScorer(snapshot.training, classifier_config, normalization_config)
        slot_extractor = SlotExtractor(snapshot.dictionary, slot_config)
        if snapshot.dictionary_error and slot_extractor.load_error is None:
            slot_extractor.load_error = snapshot.dictionary_error

        normalization_fp = sha256_json({"config": normalization_config})
        intent_fp = sha256_json(
            {
                "config": classifier_config,
                "normalization_fingerprint": normalization_fp,
                "training_data": snapshot.digests["training_file"],
            }
        )
        slot_fp = sha256_json(
            {
                "config": slot_config,
                "dictionary": snapshot.digests["dictionary_file"],
                "dictionary_version": slot_extractor.actual_version,
                "normalization_fingerprint": normalization_fp,
            }
        )
        decision_fp = sha256_json(
            {
                "config": policy_config,
                "intent_fingerprint": intent_fp,
                "slot_fingerprint": slot_fp,
            }
        )

        versions = {
            "normalization": {
                "components": {"config": sha256_json(normalization_config)},
                "fingerprint": normalization_fp,
            },
            "intent": {
                "components": {
                    "classifier_config": sha256_json(classifier_config),
                    "normalization_fingerprint": normalization_fp,
                    "training_data": snapshot.digests["training_file"],
                },
                "fingerprint": intent_fp,
            },
            "slots": {
                "components": {
                    "dictionary": snapshot.digests["dictionary_file"],
                    "dictionary_version": sha256_json(slot_extractor.actual_version),
                    "normalization_fingerprint": normalization_fp,
                    "slot_config": sha256_json(slot_config),
                },
                "fingerprint": slot_fp,
            },
            "decision": {
                "components": {
                    "intent_fingerprint": intent_fp,
                    "policy_config": sha256_json(policy_config),
                    "slot_fingerprint": slot_fp,
                },
                "fingerprint": decision_fp,
            },
        }

        self.snapshot = snapshot
        self.config = config
        self.scorer = scorer
        self.slot_extractor = slot_extractor
        self.versions = versions
        self.overall_fingerprint = _overall_from_versions(versions)

    def _parse_locked(self, sample: Any, faults: Any) -> dict[str, Any]:
        if not isinstance(sample, dict):
            raise RequestError("query payload must be an object")
        if not isinstance(faults, dict):
            raise RequestError("faults must be an object", query_id=sample.get("query_id"))

        query_id = sample.get("query_id")
        if not isinstance(query_id, str) or not query_id:
            raise RequestError("query_id must be a non-empty string")
        raw_query = sample.get("raw_query")
        if not isinstance(raw_query, str):
            raise RequestError("raw_query must be a string", query_id=query_id)
        channel = sample.get("channel")
        if channel is not None and not isinstance(channel, str):
            raise RequestError("channel must be a string when provided", query_id=query_id)
        history = sample.get("history_categories", [])
        if history is None:
            history = []
        if not isinstance(history, list) or not all(isinstance(item, str) for item in history):
            raise RequestError("history_categories must be a string list", query_id=query_id)

        normalization_config = self.config["normalization"]
        allowed_locales = normalization_config["allowed_locales"]
        locale = self._effective_locale(sample, faults)
        if locale not in allowed_locales:
            raise invalid_locale_error(locale, allowed_locales, query_id)

        max_chars = int(normalization_config["max_query_chars"])
        forced_length = faults.get("force_query_length")
        if forced_length is not None:
            if not isinstance(forced_length, int) or isinstance(forced_length, bool) or forced_length < 0:
                raise RequestError("faults.force_query_length must be a non-negative integer", query_id=query_id)
            if forced_length > max_chars:
                raise QueryError(
                    "E_QUERY_TOO_LONG",
                    "normalization",
                    "raw_query exceeds the configured character limit",
                    query_id=query_id,
                    details={"actual_chars": forced_length, "injected": True, "max_query_chars": max_chars},
                )

        cache_key = sha256_json({"faults": faults, "sample": sample})
        cached = self._cache.get(cache_key)
        stage_results: dict[str, Any] = {}
        stage_status: dict[str, Any] = {}

        def obtain(stage: str, builder: Any) -> Any:
            current = self.versions[stage]
            if cached is not None and cached["versions"][stage]["fingerprint"] == current["fingerprint"]:
                stage_results[stage] = cached["stages"][stage]
                stage_status[stage] = {"reason": "fingerprint_match", "status": "reused"}
                return stage_results[stage]

            stage_results[stage] = builder()
            if cached is None:
                status: dict[str, Any] = {"reason": "cold_cache", "status": "rebuilt"}
            else:
                previous = cached["versions"][stage]
                changed = sorted(
                    key
                    for key in set(previous["components"]) | set(current["components"])
                    if previous["components"].get(key) != current["components"].get(key)
                )
                status = {
                    "changed_components": changed,
                    "previous_fingerprint": previous["fingerprint"],
                    "reason": "fingerprint_changed",
                    "status": "rebuilt",
                }
            stage_status[stage] = status
            return stage_results[stage]

        normalized = obtain(
            "normalization",
            lambda: normalize_and_tokenize(raw_query, query_id=query_id, max_chars=max_chars),
        )
        token_texts = [token["text"] for token in normalized["tokens"]]

        classifier_result = obtain(
            "intent",
            lambda: self.scorer.score(  # type: ignore[union-attr]
                token_texts,
                query_id=query_id,
                inject_nan=bool(faults.get("nan_confidence") or faults.get("confidence") == "nan"),
            ),
        )
        slot_result = obtain(
            "slots",
            lambda: self.slot_extractor.extract(  # type: ignore[union-attr]
                normalized["normalized_query"],
                query_id=query_id,
                faults=faults,
            ),
        )
        decision = obtain(
            "decision",
            lambda: decide_intent(
                classifier_result,
                query_id=query_id,
                history_categories=list(history),
                channel=channel,
                config=self.config["policy"],
            ),
        )

        if cached is None:
            cache_state = "miss"
        elif all(item["status"] == "reused" for item in stage_status.values()):
            cache_state = "hit"
        else:
            cache_state = "partial"

        reused = [stage for stage in STAGES if stage_status[stage]["status"] == "reused"]
        rebuilt = [stage for stage in STAGES if stage_status[stage]["status"] == "rebuilt"]
        response = {
            "cache": {
                "cache_key": cache_key,
                "invalidated_stages": rebuilt,
                "rebuilt_stages": rebuilt,
                "reused_stages": reused,
                "stage_status": stage_status,
                "status": cache_state,
            },
            "channel": channel,
            "faults_applied": sorted(faults),
            "history_categories": list(history),
            "intent": {
                "candidates": classifier_result["candidates"],
                "confidence": decision["confidence"],
                "confidence_kind": decision["confidence_kind"],
                "fallback_trace": decision["fallback_trace"],
                "label": decision["label"],
                "label_zh": decision["label_zh"],
                "primary_confidence": decision["primary_confidence"],
                "source": decision["source"],
                "token_diagnostics": classifier_result["token_diagnostics"],
                "used_fallback_level": decision["used_fallback_level"],
            },
            "locale": locale,
            "normalization": {
                "normalized_query": normalized["normalized_query"],
                "offset_basis": normalized["offset_basis"],
                "token_count": len(normalized["tokens"]),
                "tokens": normalized["tokens"],
            },
            "query_id": query_id,
            "raw_query": raw_query,
            "schema_version": "qiu-result-v1",
            "slots": slot_result["slots"],
            "slot_diagnostics": slot_result["diagnostics"],
            "version_fingerprints": {stage: self.versions[stage]["fingerprint"] for stage in STAGES},
            "versions": self.versions,
        }

        self._cache[cache_key] = {
            "stages": stage_results,
            "versions": self.versions,
        }
        self._cache.move_to_end(cache_key)
        while len(self._cache) > self.cache_limit:
            self._cache.popitem(last=False)
        return response

    @staticmethod
    def _effective_locale(sample: dict[str, Any], faults: dict[str, Any]) -> Any:
        if faults.get("invalid_locale"):
            return faults.get("locale", "xx-INVALID")
        return faults.get("locale", sample.get("locale"))


def _overall_from_versions(versions: dict[str, Any]) -> str:
    if not versions:
        return ""
    return sha256_json({stage: versions[stage]["fingerprint"] for stage in STAGES})


def result_bytes(result: dict[str, Any]) -> bytes:
    return (canonical_json(result) + "\n").encode("utf-8")
