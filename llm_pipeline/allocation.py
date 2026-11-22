"""Stratified largest-remainder allocation over (source, lang) layers."""

from __future__ import annotations

import re
from collections import defaultdict
from fractions import Fraction
from typing import Any, Callable

from .config import (
    ALLOCATE_ALGORITHM,
    make_layer_id,
    normalize_weights,
    weights_as_fractions,
)
from .errors import invalid_config
from .util import sha256_bytes
from .workspace import allocation_product_bytes


_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")


def estimate_tokens(text: str, method: str) -> int:
    if method == "whitespace":
        return len(text.split())
    if method == "whitespace_plus_cjk":
        # Diagnostic alternative used to prove token-estimator invalidation.
        return len(text.split()) + len(_CJK_RE.findall(text))
    raise invalid_config(
        "unsupported token estimator",
        {"allowed": ["whitespace", "whitespace_plus_cjk"], "actual": method},
    )


def _largest_remainder(
    total: int,
    weights: dict[tuple[str, str], Fraction],
) -> dict[tuple[str, str], int]:
    if total <= 0 or not weights:
        return {layer: 0 for layer in weights}
    weight_sum = sum(weights.values(), Fraction(0))
    if weight_sum <= 0:
        return {layer: 0 for layer in weights}
    quotas: dict[tuple[str, str], int] = {}
    remainders: list[tuple[Fraction, str, str]] = []
    for (source, lang), weight in weights.items():
        ideal = Fraction(total) * weight / weight_sum
        floor = ideal.numerator // ideal.denominator
        quotas[(source, lang)] = floor
        remainders.append((ideal - floor, source, lang))
    left = total - sum(quotas.values())
    # Exact fractional remainders; source/lang is the specified deterministic tie-break.
    for _remainder, source, lang in sorted(
        remainders, key=lambda item: (-item[0], item[1], item[2])
    )[:left]:
        quotas[(source, lang)] += 1
    return quotas


def apportion_with_capacity(
    target: int,
    weights: dict[tuple[str, str], Fraction],
    capacities: dict[tuple[str, str], int],
) -> dict[tuple[str, str], int]:
    """Largest remainder with capacity caps and deficit redistribution."""
    quotas = {layer: 0 for layer in weights}
    eligible = {
        layer: weight
        for layer, weight in weights.items()
        if weight > 0 and capacities.get(layer, 0) > 0
    }
    target = min(max(0, target), sum(capacities.get(layer, 0) for layer in eligible))
    remaining = target
    while remaining > 0 and eligible:
        round_quotas = _largest_remainder(remaining, eligible)
        distributed = 0
        saturated: list[tuple[str, str]] = []
        for layer in sorted(eligible, key=lambda item: (item[0], item[1])):
            capacity_left = capacities.get(layer, 0) - quotas[layer]
            assign = min(round_quotas.get(layer, 0), capacity_left)
            if assign <= 0 and capacity_left <= 0:
                saturated.append(layer)
                continue
            quotas[layer] += assign
            distributed += assign
            if quotas[layer] >= capacities.get(layer, 0):
                saturated.append(layer)
        if distributed == 0:
            break
        remaining -= distributed
        for layer in saturated:
            eligible.pop(layer, None)
    return quotas


def _status_for(available: int, weight: Fraction, input_count: int) -> str:
    if available == 0:
        if weight > 0:
            return "empty"
        return "empty_unweighted" if input_count else "unobserved"
    if weight <= 0:
        return "unweighted"
    return "active"


def allocate(
    normalized_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    check_stop: Callable[[], None] | None = None,
) -> dict[str, Any]:
    allocation_config = config["allocation"]
    target_requested = int(allocation_config["target_samples"])
    token_estimator = allocation_config["token_estimator"]
    weight_items = normalize_weights(allocation_config["weights"])
    if not weight_items:
        raise invalid_config("allocation.weights must contain at least one layer")
    weights = weights_as_fractions(weight_items)
    if target_requested > 0 and sum(weights.values(), Fraction(0)) <= 0:
        raise invalid_config("allocation.weights sum must be positive")

    kept_by_layer: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in normalized_payload["kept"]:
        kept_by_layer[(record["source"], record["lang"])].append(record)

    duplicate_counts: dict[tuple[str, str], int] = defaultdict(int)
    for item in normalized_payload["duplicates"]:
        duplicate_counts[(item["source"], item["lang"])] += 1
    empty_counts: dict[tuple[str, str], int] = defaultdict(int)
    for item in normalized_payload["empty_errors"]:
        empty_counts[(item["source"], item["lang"])] += 1
    input_counts: dict[tuple[str, str], int] = defaultdict(int)
    for item in normalized_payload.get("observed_layers", []):
        input_counts[(item["source"], item["lang"])] = int(item["input_count"])

    layers: set[tuple[str, str]] = set(weights)
    layers.update((record["source"], record["lang"]) for record in normalized_payload["kept"])
    layers.update(duplicate_counts)
    layers.update(empty_counts)
    layers.update(input_counts)

    capacities: dict[tuple[str, str], int] = {}
    token_totals: dict[tuple[str, str], int] = {}
    for layer in sorted(layers):
        records = kept_by_layer.get(layer, [])
        capacities[layer] = len(records)
        token_totals[layer] = sum(
            estimate_tokens(record["normalized_text"], token_estimator)
            for record in records
        )
        if check_stop is not None:
            check_stop()

    quotas = apportion_with_capacity(target_requested, weights, capacities)
    effective_target = min(
        target_requested,
        sum(capacities[layer] for layer in weights if weights[layer] > 0),
    )

    layer_rows: list[dict[str, Any]] = []
    total_weight = sum(weights.values(), Fraction(0))
    for source, lang in sorted(layers):
        layer = (source, lang)
        weight = weights.get(layer, Fraction(0))
        ideal = (
            Fraction(target_requested) * weight / total_weight
            if total_weight > 0
            else Fraction(0)
        )
        available = capacities.get(layer, 0)
        allocated = quotas.get(layer, 0)
        status = _status_for(available, weight, input_counts.get(layer, 0))
        layer_rows.append(
            {
                "layer_id": make_layer_id(source, lang),
                "source": source,
                "lang": lang,
                "weight": float(weight),
                "input_count": input_counts.get(layer, 0),
                "available": available,
                "duplicate_count": duplicate_counts.get(layer, 0),
                "empty_error_count": empty_counts.get(layer, 0),
                "token_estimator": token_estimator,
                "token_count": token_totals.get(layer, 0),
                "ideal": f"{float(ideal):.12f}",
                "allocated": allocated,
                "status": status,
                "capacity_limited": status == "active" and ideal > allocated,
            }
        )
        if check_stop is not None:
            check_stop()

    total_allocated = sum(row["allocated"] for row in layer_rows)
    total_available = sum(capacities.get(layer, 0) for layer in weights if weights[layer] > 0)
    payload: dict[str, Any] = {
        "algorithm": ALLOCATE_ALGORITHM,
        "target_requested": target_requested,
        "target_effective": effective_target,
        "capacity_limited": target_requested > total_available,
        "token_estimator": token_estimator,
        "weights": weight_items,
        "layers": layer_rows,
        "allocation": {row["layer_id"]: row["allocated"] for row in layer_rows},
        "total_allocated": total_allocated,
        "total_available_positive_weight": total_available,
    }
    payload["allocation_digest"] = sha256_bytes(allocation_product_bytes(payload))
    return payload
