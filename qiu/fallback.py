"""Auditable fallback policy for low-confidence intent predictions."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .classifier import INTENTS, INTENT_LABELS_ZH
from .errors import QueryError


def decide_intent(
    classifier_result: dict[str, Any],
    *,
    query_id: str | None,
    history_categories: list[str],
    channel: str | None,
    config: dict[str, Any],
) -> dict[str, Any]:
    threshold = float(config["confidence_threshold"])
    if threshold != threshold:
        raise QueryError(
            "E_NUMERIC",
            "policy",
            "confidence threshold is NaN",
            query_id=query_id,
            details={"threshold": "nan"},
        )

    trace: list[dict[str, Any]] = []
    primary_label = classifier_result["label"]
    primary_confidence = float(classifier_result["confidence"])
    if primary_confidence >= threshold:
        trace.append(
            {
                "confidence": primary_confidence,
                "level": 0,
                "reason": "confidence_at_or_above_threshold",
                "selected_intent": primary_label,
                "source": "primary_classifier",
                "status": "selected",
                "threshold": threshold,
            }
        )
        trace.extend(
            _skipped_tail(
                1,
                ["history_prior", "channel_default", "final_default"],
                "primary_classifier_confident",
            )
        )
        return _result(primary_label, primary_confidence, "primary_classifier", 0, trace, primary_confidence)

    trace.append(
        {
            "confidence": primary_confidence,
            "level": 0,
            "reason": "confidence_below_threshold",
            "selected_intent": None,
            "source": "primary_classifier",
            "status": "rejected",
            "threshold": threshold,
        }
    )

    category_map = config.get("category_intent_map", {})
    mapped = Counter()
    for category in history_categories:
        intent = category_map.get(category)
        if intent in INTENTS:
            mapped[intent] += 1
    if mapped:
        best_count = max(mapped.values())
        best_intent = next(intent for intent in INTENTS if mapped[intent] == best_count)
        confidence = best_count / sum(mapped.values())
        trace.append(
            {
                "confidence": round(confidence, 12),
                "level": 1,
                "matched_categories": [category for category in history_categories if category_map.get(category) == best_intent],
                "reason": "mapped_history_prior",
                "selected_intent": best_intent,
                "source": "history_prior",
                "status": "selected",
            }
        )
        trace.extend(_skipped_tail(2, ["channel_default", "final_default"], "resolved_by_higher_fallback"))
        return _result(best_intent, confidence, "history_prior", 1, trace, primary_confidence)

    trace.append(
        {
            "level": 1,
            "reason": "no_mapped_history",
            "selected_intent": None,
            "source": "history_prior",
            "status": "skipped",
        }
    )

    channel_defaults = config.get("channel_defaults", {})
    channel_intent = channel_defaults.get(channel) if isinstance(channel, str) else None
    if channel_intent in INTENTS:
        trace.append(
            {
                "channel": channel,
                "confidence": 1.0,
                "level": 2,
                "reason": "channel_default_matched",
                "selected_intent": channel_intent,
                "source": "channel_default",
                "status": "selected",
            }
        )
        trace.append(
            {
                "level": 3,
                "reason": "resolved_by_higher_fallback",
                "selected_intent": None,
                "source": "final_default",
                "status": "skipped",
            }
        )
        return _result(channel_intent, 1.0, "channel_default", 2, trace, primary_confidence)

    trace.append(
        {
            "channel": channel,
            "level": 2,
            "reason": "unmapped_channel",
            "selected_intent": None,
            "source": "channel_default",
            "status": "skipped",
        }
    )

    final_intent = config.get("final_default")
    if final_intent not in INTENTS:
        final_intent = "informational"
    trace.append(
        {
            "confidence": 1.0,
            "level": 3,
            "reason": "deterministic_final_default",
            "selected_intent": final_intent,
            "source": "final_default",
            "status": "selected",
        }
    )
    return _result(final_intent, 1.0, "final_default", 3, trace, primary_confidence)


def _skipped_tail(start_level: int, names: list[str], reason: str) -> list[dict[str, Any]]:
    return [
        {
            "level": start_level + offset,
            "reason": reason,
            "selected_intent": None,
            "source": name,
            "status": "skipped",
        }
        for offset, name in enumerate(names)
    ]


def _result(
    label: str,
    confidence: float,
    source: str,
    level: int,
    trace: list[dict[str, Any]],
    primary_confidence: float,
) -> dict[str, Any]:
    confidence_kind = {
        "channel_default": "rule",
        "final_default": "rule",
        "history_prior": "history_share",
        "primary_classifier": "softmax",
    }[source]
    return {
        "confidence": round(float(confidence), 12),
        "confidence_kind": confidence_kind,
        "fallback_trace": trace,
        "label": label,
        "label_zh": INTENT_LABELS_ZH[label],
        "primary_confidence": round(float(primary_confidence), 12),
        "source": source,
        "used_fallback_level": level,
    }
