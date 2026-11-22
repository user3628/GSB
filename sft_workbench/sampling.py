"""Stratified proportional sampling with the largest remainder method.

Strata are (source, lang) pairs of the kept samples. Per-stratum sample
counts and token estimates are collected, target weights are applied, and
the integer allocation is computed with the largest remainder method;
remainder ties are broken deterministically by ascending stratum key.
Allocations are capped by the available count per stratum and leftover
budget is redistributed in the same deterministic order.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import InternalSample
from .textnorm import normalize_sample_text, word_tokens


@dataclass
class SamplingConfig:
    total_budget: int = 12
    target_weights: dict[str, float] = field(default_factory=dict)
    default_weight: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_budget": self.total_budget,
            "target_weights": dict(self.target_weights),
            "default_weight": self.default_weight,
        }


def stratum_key(sample: InternalSample) -> str:
    return f"{sample.source}|{sample.lang}"


def estimate_tokens(sample: InternalSample) -> int:
    text = normalize_sample_text(sample.instruction, sample.input, sample.output)
    return len(word_tokens(text))


def largest_remainder_allocate(
    weights: dict[str, float], budget: int, caps: dict[str, int]
) -> dict[str, int]:
    keys = sorted(weights)
    allocation = {key: 0 for key in keys}
    if budget <= 0:
        return allocation
    total_weight = sum(weights[key] for key in keys)
    if total_weight <= 0:
        return allocation
    exact = {key: weights[key] / total_weight * budget for key in keys}
    for key in keys:
        allocation[key] = min(int(exact[key]), caps.get(key, 0))
    leftover = budget - sum(allocation.values())
    order = sorted(keys, key=lambda key: (-(exact[key] - int(exact[key])), key))
    cursor = 0
    while leftover > 0:
        progressed = False
        for _ in range(len(order)):
            key = order[cursor % len(order)]
            cursor += 1
            if allocation[key] < caps.get(key, 0):
                allocation[key] += 1
                leftover -= 1
                progressed = True
                break
        if not progressed:
            break
    return allocation


def stratified_mixture(
    kept: list[InternalSample],
    scores: list[dict[str, Any]],
    config: SamplingConfig,
) -> dict[str, Any]:
    score_by_id = {record["sample_id"]: record["score"] for record in scores}
    strata: dict[str, dict[str, Any]] = {}
    for sample in kept:
        key = stratum_key(sample)
        bucket = strata.setdefault(key, {"count": 0, "token_estimate": 0, "sample_ids": []})
        bucket["count"] += 1
        bucket["token_estimate"] += estimate_tokens(sample)
        bucket["sample_ids"].append(sample.sample_id)
    for key in strata:
        strata[key]["sample_ids"].sort()

    weights = {
        key: float(config.target_weights.get(key, config.default_weight)) for key in strata
    }
    caps = {key: strata[key]["count"] for key in strata}
    allocation = largest_remainder_allocate(weights, config.total_budget, caps)

    mixture: list[str] = []
    for key in sorted(strata):
        members = [s for s in kept if stratum_key(s) == key]
        members.sort(key=lambda s: (-score_by_id.get(s.sample_id, 0.0), s.sample_id))
        take = allocation.get(key, 0)
        mixture.extend(s.sample_id for s in members[:take])

    return {
        "strata": {key: strata[key] for key in sorted(strata)},
        "weights": {key: weights[key] for key in sorted(weights)},
        "allocation": {key: allocation[key] for key in sorted(allocation)},
        "mixture": mixture,
    }
