"""Stage 4: stratified mixture sampling with largest-remainder allocation."""
from __future__ import annotations

import math
import random

from .hashing import canonical_json, seeded_hash64, stable_hash64, tokenize

STAGE = "sampling"


def _layer_key(source: str, lang: str) -> str:
    return f"{source}::{lang}"


def largest_remainder(weights: dict[str, float], capacities: dict[str, int],
                       total_seats: int | None = None) -> tuple[dict[str, int], list[dict]]:
    """Allocate ``total_seats`` by target weights with largest-remainder rule.

    Exact quotas are floored; leftover seats go to layers with the largest
    remainders, broken by lexical layer-key order. Per-layer capacity acts as a
    hard upper bound; seats freed by clipping are redistributed to eligible
    layers. Default ``total_seats`` is the number of available samples.
    """
    diagnostics: list[dict] = []
    if total_seats is None:
        total_seats = sum(capacities.values())
    allocations = {layer: 0 for layer in capacities}
    weight_sum = sum(weights.values())
    if weight_sum <= 0:
        diagnostics.append({
            "code": "E_ALLOCATION_INVALID", "layer": None,
            "message": "sum of target weights is zero",
        })
        return allocations, diagnostics

    active = [layer for layer in capacities if weights.get(layer, 0.0) > 0.0]
    for layer in capacities:
        if weights.get(layer, 0.0) == 0.0:
            diagnostics.append({
                "code": "E_ALLOCATION_INVALID", "layer": layer,
                "message": "layer target weight is zero",
            })
        elif capacities[layer] == 0:
            diagnostics.append({
                "code": "E_ALLOCATION_INVALID", "layer": layer,
                "message": "layer has no available samples",
            })

    quotas = {layer: total_seats * weights[layer] / weight_sum for layer in active}

    def floor_quota(layer: str) -> int:
        value = quotas[layer]
        rounded = round(value)
        if abs(value - rounded) < 1e-9:
            return rounded
        return math.floor(value)

    for layer in active:
        allocations[layer] = min(floor_quota(layer), capacities[layer])

    def remainder(layer: str) -> float:
        return quotas[layer] - floor_quota(layer)

    order = sorted(active, key=lambda layer: (-remainder(layer), layer))
    remaining = total_seats - sum(allocations.values())
    while remaining > 0:
        progressed = False
        for layer in order:
            if remaining == 0:
                break
            if allocations[layer] < capacities[layer]:
                allocations[layer] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            break

    for layer in active:
        if capacities[layer] == 0 and allocations[layer] != 0:
            diagnostics.append({
                "code": "E_ALLOCATION_INVALID", "layer": layer,
                "message": "allocation requested for empty layer",
            })
    return allocations, diagnostics


def run(features: dict, normalized: dict, cfg_raw: dict) -> dict:
    target_weights: dict[str, float] = cfg_raw["sampling"]["target_weights"]
    sampling_seed = int(cfg_raw["sampling"]["sampling_seed"])

    norm_by_id = {item["pair_id"]: item for item in normalized["records"]}
    layers: dict[str, list[str]] = {}
    token_totals: dict[str, int] = {}
    for record in features["records"]:
        pair_id = record["pair_id"]
        base = norm_by_id[pair_id]
        layer = _layer_key(base["source"], base["lang"])
        layers.setdefault(layer, []).append(pair_id)
        token_totals[layer] = (
            token_totals.get(layer, 0)
            + len(tokenize(base["norm_prompt"]))
            + len(tokenize(base["norm_chosen"]))
            + len(tokenize(base["norm_rejected"]))
        )

    all_layers = sorted(set(layers) | set(target_weights))
    capacities = {layer: len(layers.get(layer, [])) for layer in all_layers}
    allocations, diagnostics = largest_remainder(
        target_weights, capacities, total_seats=sum(capacities.values()))

    layer_reports = []
    selected: list[str] = []
    for layer in all_layers:
        pair_ids = sorted(layers.get(layer, []))
        take = allocations.get(layer, 0)
        rng = random.Random(seeded_hash64(f"{layer}@{sampling_seed}", sampling_seed))
        chosen = sorted(rng.sample(pair_ids, take)) if take else []
        selected.extend(chosen)
        source, lang = layer.split("::", 1)
        layer_reports.append({
            "layer": layer,
            "source": source,
            "lang": lang,
            "target_weight": target_weights.get(layer, 0.0),
            "available_pairs": capacities[layer],
            "token_estimate": token_totals.get(layer, 0),
            "allocated": take,
            "sampled_pair_ids": chosen,
        })

    selected = sorted(set(selected))
    mixture_summary = {
        "total_available": sum(capacities.values()),
        "total_allocated": sum(allocations.values()),
        "total_selected": len(selected),
        "layer_count": len(all_layers),
        "selected_pair_ids": selected,
    }
    diagnostics.sort(key=lambda item: (str(item.get("layer") or ""), item["code"]))
    return {
        "stage": STAGE,
        "layers": layer_reports,
        "mixture_summary": mixture_summary,
        "diagnostics": diagnostics,
        "stats": {"layers": len(all_layers), "selected": len(selected)},
    }


def artifact_digest(result: dict) -> str:
    payload = {
        "layers": [
            {key: item[key] for key in (
                "layer", "target_weight", "available_pairs", "token_estimate",
                "allocated", "sampled_pair_ids")}
            for item in result["layers"]
        ],
        "mixture_summary": result["mixture_summary"],
        "diagnostics": result["diagnostics"],
    }
    return stable_hash64(canonical_json(payload))
