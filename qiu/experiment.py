"""Deterministic batch evaluation and byte-stable experiment summaries."""

from __future__ import annotations

import math
import random
from pathlib import Path
from typing import Any, Iterable

from .engine import QueryEngine
from .io import canonical_json, read_jsonl, sha256_bytes, sha256_json


def run_experiment(
    engine: QueryEngine,
    *,
    eval_path: str | Path,
    seed: int,
    bootstrap_samples: int = 200,
) -> dict[str, Any]:
    eval_bytes = Path(eval_path).read_bytes()
    dataset_fingerprint = sha256_bytes(eval_bytes)
    records = read_jsonl(eval_path)
    if not records:
        raise ValueError("evaluation set is empty")

    observations: list[dict[str, Any]] = []
    confusion: dict[str, dict[str, int]] = {
        intent: {prediction: 0 for prediction in _intents_from_records(records)} for intent in _intents_from_records(records)
    }
    fallback_levels = {"0": 0, "1": 0, "2": 0, "3": 0}

    for record in records:
        result = engine.parse(record)
        expected_intent = record["expected_intent"]
        predicted_intent = result["intent"]["label"]
        confusion.setdefault(expected_intent, {})
        confusion[expected_intent][predicted_intent] = confusion[expected_intent].get(predicted_intent, 0) + 1
        fallback_levels[str(result["intent"]["used_fallback_level"])] += 1

        expected_slots = {(item["type"], item["text"]) for item in record.get("expected_slots", [])}
        predicted_slots = {(item["type"], item["text"]) for item in result["slots"]}
        tp = len(expected_slots & predicted_slots)
        observations.append(
            {
                "expected_slots": len(expected_slots),
                "fb": int(result["intent"]["used_fallback_level"] > 0),
                "fp": len(predicted_slots - expected_slots),
                "intent_correct": int(predicted_intent == expected_intent),
                "predicted_slots": len(predicted_slots),
                "tp": tp,
            }
        )

    aggregate = _aggregate(observations)
    bootstrap = _bootstrap(observations, seed=seed, samples=bootstrap_samples)
    version_status = engine.version_status()
    config_fingerprint = version_status["config_fingerprint"]
    run_fingerprint = sha256_json(
        {
            "bootstrap_samples": bootstrap_samples,
            "config_fingerprint": config_fingerprint,
            "dataset_fingerprint": dataset_fingerprint,
            "schema_version": "qiu-experiment-v1",
            "seed": seed,
        }
    )

    return {
        "bootstrap_95ci": bootstrap,
        "config_fingerprint": config_fingerprint,
        "dataset_fingerprint": dataset_fingerprint,
        "fallback_level_counts": fallback_levels,
        "intent_confusion": {
            expected: {predicted: count for predicted, count in sorted(predictions.items())}
            for expected, predictions in sorted(confusion.items())
        },
        "metrics": aggregate,
        "run_fingerprint": run_fingerprint,
        "sample_count": len(records),
        "schema_version": "qiu-experiment-v1",
        "seed": seed,
        "stage_fingerprints": version_status["stage_fingerprints"],
    }


def experiment_bytes(summary: dict[str, Any]) -> bytes:
    return (canonical_json(summary) + "\n").encode("utf-8")


def _aggregate(observations: list[dict[str, Any]]) -> dict[str, Any]:
    correct = sum(item["intent_correct"] for item in observations)
    tp = sum(item["tp"] for item in observations)
    fp = sum(item["fp"] for item in observations)
    expected = sum(item["expected_slots"] for item in observations)
    predicted = sum(item["predicted_slots"] for item in observations)
    fallbacks = sum(item["fb"] for item in observations)
    precision = _ratio(tp, predicted)
    recall = _ratio(tp, expected)
    f1 = _ratio(2 * precision * recall, precision + recall)
    return {
        "fallback_trigger_rate": _metric(_ratio(fallbacks, len(observations))),
        "intent_accuracy": _metric(_ratio(correct, len(observations))),
        "intent_correct": correct,
        "sample_count": len(observations),
        "slot_expected": expected,
        "slot_f1": _metric(f1),
        "slot_false_negative": max(expected - tp, 0),
        "slot_false_positive": fp,
        "slot_predicted": predicted,
        "slot_precision": _metric(precision),
        "slot_recall": _metric(recall),
        "slot_true_positive": tp,
    }


def _bootstrap(observations: list[dict[str, Any]], *, seed: int, samples: int) -> dict[str, Any]:
    if samples <= 0:
        return {"confidence": 0.95, "resamples": 0}
    rng = random.Random(seed)
    accuracy_values: list[float] = []
    precision_values: list[float] = []
    recall_values: list[float] = []
    fallback_values: list[float] = []
    size = len(observations)
    for _ in range(samples):
        chosen = [observations[rng.randrange(size)] for _ in range(size)]
        aggregate = _aggregate(chosen)
        accuracy_values.append(aggregate["intent_accuracy"])
        precision_values.append(aggregate["slot_precision"])
        recall_values.append(aggregate["slot_recall"])
        fallback_values.append(aggregate["fallback_trigger_rate"])
    return {
        "confidence": 0.95,
        "fallback_trigger_rate_ci": _nearest_rank_interval(fallback_values),
        "intent_accuracy_ci": _nearest_rank_interval(accuracy_values),
        "resamples": samples,
        "slot_precision_ci": _nearest_rank_interval(precision_values),
        "slot_recall_ci": _nearest_rank_interval(recall_values),
    }


def _nearest_rank_interval(values: Iterable[float]) -> list[float]:
    ordered = sorted(values)
    count = len(ordered)
    if count == 0:
        return [0.0, 0.0]
    low_index = max(0, math.ceil(0.025 * count) - 1)
    high_index = min(count - 1, math.ceil(0.975 * count) - 1)
    return [_metric(ordered[low_index]), _metric(ordered[high_index])]


def _intents_from_records(records: list[dict[str, Any]]) -> list[str]:
    values = sorted(
        {
            value
            for record in records
            for value in (record.get("expected_intent"),)
            if isinstance(value, str)
        }
    )
    return values


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _metric(value: float) -> float:
    rounded = round(float(value), 12)
    return 0.0 if rounded == 0.0 else rounded
