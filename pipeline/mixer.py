"""Stratified mixing: largest-remainder allocation + seeded sampling.

NumPy is used for the allocation math; all orderings are explicit so the
result is deterministic for a given input and seed.
"""

import random
from collections import defaultdict

import numpy as np


def layer_key(source, lang):
    return "%s|%s" % (source, lang)


def largest_remainder_allocation(weights, availability, total):
    """Allocate `total` samples across layers by largest remainder.

    weights/availability: dicts keyed by layer. Allocation never exceeds
    availability; leftover budget is redistributed one-by-one to layers
    with positive weight and spare capacity, ordered by (-remainder,
    layer key). Returns {layer: count} for every layer in `availability`.
    """
    layers = sorted(availability)
    avail = np.array([max(0, int(availability[k])) for k in layers],
                     dtype=np.int64)
    weight = np.array([max(0.0, float(weights.get(k, 0.0))) for k in layers],
                      dtype=np.float64)
    alloc = np.zeros(len(layers), dtype=np.int64)
    budget = max(0, int(total))
    weight_sum = weight.sum()
    if budget == 0 or weight_sum <= 0.0:
        return dict(zip(layers, (int(v) for v in alloc)))

    quota = budget * weight / weight_sum
    base = np.minimum(np.floor(quota).astype(np.int64), avail)
    alloc += base
    leftover = budget - int(alloc.sum())

    remainders = quota - np.floor(quota)
    # Deterministic priority: larger remainder first, ties by layer key
    # (layers are already sorted by key, stable sort keeps that order).
    order = np.argsort(-remainders, kind="stable")
    while leftover > 0:
        progressed = False
        for pos in order:
            if leftover == 0:
                break
            if weight[pos] > 0.0 and alloc[pos] < avail[pos]:
                alloc[pos] += 1
                leftover -= 1
                progressed = True
        if not progressed:
            break
    return dict(zip(layers, (int(v) for v in alloc)))


def stratified_mix(records, weights, default_weight, total, seed):
    """Select a deterministic mixed subset of deduped records.

    records: list of dicts with source/lang/tokens and a stable `order`.
    Returns (selected_records_in_original_order, stats_dict).
    """
    groups = defaultdict(list)
    for rec in records:
        groups[layer_key(rec["source"], rec["lang"])].append(rec)
    for key in groups:
        groups[key].sort(key=lambda rec: rec["order"])

    layers = sorted(groups)
    availability = {key: len(groups[key]) for key in layers}
    effective_weights = {key: float(weights.get(key, default_weight))
                         for key in layers}
    allocation = largest_remainder_allocation(effective_weights,
                                              availability, total)

    selected = []
    layer_stats = {}
    for key in layers:
        pool = groups[key]
        take = min(allocation[key], len(pool))
        rng = random.Random("%s|%s" % (seed, key))
        chosen_idx = sorted(rng.sample(range(len(pool)), take)) if take else []
        chosen = [pool[i] for i in chosen_idx]
        selected.extend(chosen)
        layer_stats[key] = {
            "available": len(pool),
            "available_tokens": int(sum(rec["tokens"] for rec in pool)),
            "weight": effective_weights[key],
            "allocated": take,
            "selected_tokens": int(sum(rec["tokens"] for rec in chosen)),
            "selected_ids": [rec["id"] for rec in
                             sorted(chosen, key=lambda r: r["order"])],
        }
    selected.sort(key=lambda rec: rec["order"])
    stats = {
        "layers": layer_stats,
        "total_requested": int(total),
        "total_selected": int(len(selected)),
        "total_selected_tokens": int(sum(rec["tokens"] for rec in selected)),
    }
    return selected, stats
