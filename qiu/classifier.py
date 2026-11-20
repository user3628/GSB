"""A deterministic multinomial naive Bayes intent scorer with feature weights."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any

import numpy as np

from .errors import QueryError
from .normalization import normalize_and_tokenize

INTENTS: tuple[str, ...] = ("navigation", "informational", "transactional", "local_service")
INTENT_LABELS_ZH = {
    "navigation": "导航",
    "informational": "信息",
    "transactional": "交易",
    "local_service": "本地服务",
}


def stable_float(value: float) -> float:
    rounded = round(float(value), 12)
    if not math.isfinite(rounded):
        raise QueryError(
            "E_NUMERIC",
            "intent",
            "non-finite numeric value encountered",
            details={"value": str(value)},
        )
    return 0.0 if rounded == 0.0 else rounded


class NaiveBayesIntentScorer:
    def __init__(self, training_records: list[dict[str, Any]], config: dict[str, Any], normalization_config: dict[str, Any]) -> None:
        self.config = config
        self.class_weights = _class_weights(config)
        self.feature_weights = _token_feature_weights(config)
        self.smoothing = float(config["smoothing"])
        self.nb_scale = float(config["nb_scale"])
        self.feature_scale = float(config["feature_scale"])

        class_docs: Counter[str] = Counter()
        token_counts: dict[str, Counter[str]] = {intent: Counter() for intent in INTENTS}
        token_totals: Counter[str] = Counter()
        vocabulary: set[str] = set()

        for index, record in enumerate(training_records):
            label = record.get("intent")
            if label not in INTENTS:
                raise ValueError(f"training record {index} has unsupported intent {label!r}")
            normalized = normalize_and_tokenize(
                record.get("raw_query"),
                query_id=record.get("query_id", f"training-{index}"),
                max_chars=int(normalization_config["max_query_chars"]),
            )
            tokens = [token["text"] for token in normalized["tokens"]]
            class_docs[label] += 1
            for token in tokens:
                token_counts[label][token] += 1
                token_totals[label] += 1
                vocabulary.add(token)

        if not training_records or any(class_docs[intent] == 0 for intent in INTENTS):
            raise ValueError("training data must contain every intent class")

        self.class_docs = class_docs
        self.token_counts = token_counts
        self.token_totals = token_totals
        self.vocabulary = tuple(sorted(vocabulary))
        self.vocabulary_size = len(self.vocabulary)
        total_docs = sum(class_docs.values())
        alpha = self.smoothing

        self.prior_log = {
            intent: math.log((class_docs[intent] + alpha) / (total_docs + alpha * len(INTENTS)))
            for intent in INTENTS
        }
        self.likelihood_log: dict[str, dict[str, float]] = {}
        denominator = {intent: token_totals[intent] + alpha * self.vocabulary_size for intent in INTENTS}
        for intent in INTENTS:
            self.likelihood_log[intent] = {
                token: math.log((token_counts[intent][token] + alpha) / denominator[intent])
                for token in self.vocabulary
            }
        self.unknown_log = {
            intent: math.log(alpha / denominator[intent])
            for intent in INTENTS
        }

    def model_summary(self) -> dict[str, Any]:
        return {
            "classes": list(INTENTS),
            "class_document_counts": {intent: self.class_docs[intent] for intent in INTENTS},
            "token_total_counts": {intent: self.token_totals[intent] for intent in INTENTS},
            "vocabulary_size": self.vocabulary_size,
        }

    def score(self, tokens: list[str], *, query_id: str | None, inject_nan: bool = False) -> dict[str, Any]:
        scores: list[float] = []
        candidates: list[dict[str, Any]] = []
        known_tokens = [token for token in tokens if token in self.likelihood_log[INTENTS[0]]]
        unknown_tokens = [token for token in tokens if token not in self.likelihood_log[INTENTS[0]]]

        for intent in INTENTS:
            likelihood = math.fsum(
                self.likelihood_log[intent].get(token, self.unknown_log[intent])
                for token in tokens
            )
            feature_raw = math.fsum(self.feature_weights[intent].get(token, 0.0) for token in tokens)
            class_weight = self.class_weights[intent]
            weighted_likelihood = class_weight * self.nb_scale * likelihood
            weighted_feature = class_weight * self.feature_scale * feature_raw
            score = self.prior_log[intent] + weighted_likelihood + weighted_feature
            scores.append(score)
            candidates.append(
                {
                    "components": {
                        "class_weight": stable_float(class_weight),
                        "feature_raw": stable_float(feature_raw),
                        "likelihood_log": stable_float(likelihood),
                        "prior_log": stable_float(self.prior_log[intent]),
                        "score": stable_float(score),
                        "weighted_feature": stable_float(weighted_feature),
                        "weighted_likelihood": stable_float(weighted_likelihood),
                    },
                    "intent": intent,
                    "label_zh": INTENT_LABELS_ZH[intent],
                }
            )

        values = np.asarray(scores, dtype=np.float64)
        if inject_nan:
            values = values.copy()
            values[0] = np.nan
        if not np.all(np.isfinite(values)):
            raise QueryError(
                "E_NUMERIC",
                "intent",
                "intent score contains NaN or infinity",
                query_id=query_id,
                details={"non_finite_intents": [INTENTS[i] for i, value in enumerate(values) if not np.isfinite(value)]},
            )

        shifted = values - np.max(values)
        exponentials = np.exp(shifted)
        probabilities = exponentials / np.sum(exponentials)
        for candidate, probability in zip(candidates, probabilities, strict=True):
            candidate["probability"] = stable_float(float(probability))

        best_index = max(range(len(INTENTS)), key=lambda index: (float(probabilities[index]), -index))
        best = candidates[best_index]
        return {
            "candidates": candidates,
            "confidence": best["probability"],
            "label": INTENTS[best_index],
            "label_zh": INTENT_LABELS_ZH[INTENTS[best_index]],
            "token_diagnostics": {
                "known": known_tokens,
                "unknown": unknown_tokens,
            },
        }


def _class_weights(config: dict[str, Any]) -> dict[str, float]:
    raw = config.get("class_weights", {})
    weights: dict[str, float] = {}
    for intent in INTENTS:
        value = float(raw.get(intent, 1.0))
        if not math.isfinite(value):
            raise ValueError(f"classifier.class_weights.{intent} must be finite")
        weights[intent] = value
    return weights


def _token_feature_weights(config: dict[str, Any]) -> dict[str, dict[str, float]]:
    raw = config.get("token_feature_weights", {})
    result: dict[str, dict[str, float]] = {intent: {} for intent in INTENTS}
    for intent, features in raw.items():
        if intent not in INTENTS:
            raise ValueError(f"unsupported token feature intent {intent!r}")
        if not isinstance(features, dict):
            raise ValueError("classifier.token_feature_weights entries must be objects")
        for token, weight in features.items():
            numeric = float(weight)
            if not math.isfinite(numeric):
                raise ValueError("token feature weights must be finite")
            result[intent][token] = numeric
    return result
