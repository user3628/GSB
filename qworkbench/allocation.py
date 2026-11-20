from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_FLOOR
from fractions import Fraction
from typing import Any, Callable, Mapping, Sequence

from .config import AllocationConfig


def _fraction_string(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def _weight_for_layer(
    source: str,
    lang: str,
    config: AllocationConfig,
) -> Fraction:
    source_weight = Fraction(str(config.source_weights.get(source, config.default_weight)))
    lang_weight = Fraction(str(config.lang_weights.get(lang, config.default_weight)))
    return source_weight * lang_weight


def _score_rank_units(value: float) -> int:
    decimal = Decimal(str(value)) * Decimal(10**15)
    return int(decimal.to_integral_value(rounding=ROUND_FLOOR))


def allocate_and_sample(
    score_records: Sequence[Mapping[str, Any]],
    config: AllocationConfig,
    stop_checker: Callable[[], None] | None = None,
) -> dict[str, Any]:
    check = stop_checker or (lambda: None)
    by_layer: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for record in score_records:
        by_layer[(str(record["source"]), str(record["lang"]))].append(record)
        check()
    layers: list[dict[str, Any]] = []
    for (source, lang), records in sorted(by_layer.items()):
        ordered_records = sorted(
            records,
            key=lambda record: (-_score_rank_units(float(record["quality_score"])), str(record["id"])),
        )
        check()
        weight = _weight_for_layer(source, lang, config)
        token_estimate = sum(
            int(
                record.get(
                    "whitespace_sample_tokens",
                    record["whitespace_output_tokens"],
                )
            )
            for record in ordered_records
        )
        layers.append(
            {
                "source": source,
                "lang": lang,
                "records": ordered_records,
                "available": len(ordered_records),
                "token_estimate": token_estimate,
                "weight": weight,
            }
        )

    positive_layers = [layer for layer in layers if layer["weight"] > 0]
    total_weight = sum((layer["weight"] for layer in positive_layers), Fraction(0))
    positive_capacity = sum(layer["available"] for layer in positive_layers)
    if total_weight <= 0 or positive_capacity <= 0:
        target_n = 0
    else:
        target_n = min(config.target_size, positive_capacity)

    for layer in layers:
        if layer["weight"] <= 0 or total_weight <= 0:
            layer["ideal_quota"] = Fraction(0)
        else:
            layer["ideal_quota"] = Fraction(target_n) * layer["weight"] / total_weight
        base = min(layer["available"], int(layer["ideal_quota"].numerator // layer["ideal_quota"].denominator))
        layer["allocated"] = base

    remaining = target_n - sum(layer["allocated"] for layer in layers)
    while remaining > 0:
        candidates = [
            layer
            for layer in layers
            if layer["weight"] > 0 and layer["allocated"] < layer["available"]
        ]
        if not candidates:
            break
        candidates.sort(
            key=lambda layer: (
                -(layer["ideal_quota"] - layer["allocated"]),
                layer["source"],
                layer["lang"],
            )
        )
        candidates[0]["allocated"] += 1
        remaining -= 1

    selected: list[dict[str, Any]] = []
    layer_details: list[dict[str, Any]] = []
    for layer in layers:
        allocated = int(layer["allocated"])
        chosen = layer["records"][:allocated]
        for rank, record in enumerate(chosen, 1):
            selected.append(
                {
                    **dict(record),
                    "allocation_rank": rank,
                    "layer_source_weight": layer["weight"].numerator
                    / layer["weight"].denominator,
                }
            )
        layer_details.append(
            {
                "source": layer["source"],
                "lang": layer["lang"],
                "retained_count": layer["available"],
                "token_estimate": layer["token_estimate"],
                "target_weight": float(layer["weight"]),
                "target_weight_fraction": _fraction_string(layer["weight"]),
                "ideal_quota": float(layer["ideal_quota"]),
                "ideal_quota_fraction": _fraction_string(layer["ideal_quota"]),
                "allocated": allocated,
                "actual_sampled": len(chosen),
                "unfilled": allocated - len(chosen),
            }
        )
        check()
    selected.sort(
        key=lambda record: (
            str(record["source"]),
            str(record["lang"]),
            -_score_rank_units(float(record["quality_score"])),
            str(record["id"]),
        )
    )
    actual_sampled = len(selected)
    summary = {
        "target_size": config.target_size,
        "effective_target": target_n,
        "retained_samples": len(score_records),
        "positive_weight_available": positive_capacity,
        "actual_sampled": actual_sampled,
        "unfilled_target": target_n - actual_sampled,
        "token_estimate_total": sum(layer["token_estimate"] for layer in layers),
        "token_estimate_sampled": sum(
            int(
                record.get(
                    "whitespace_sample_tokens",
                    record["whitespace_output_tokens"],
                )
            )
            for record in selected
        ),
        "layer_count": len(layers),
        "allocated_by_source": {},
        "allocated_by_lang": {},
        "tie_break": "largest deficit; then source asc, lang asc at equal remainder",
    }
    source_counts: dict[str, int] = defaultdict(int)
    lang_counts: dict[str, int] = defaultdict(int)
    for record in selected:
        source_counts[str(record["source"])] += 1
        lang_counts[str(record["lang"])] += 1
    summary["allocated_by_source"] = dict(sorted(source_counts.items()))
    summary["allocated_by_lang"] = dict(sorted(lang_counts.items()))
    allocation = {
        "target_size": config.target_size,
        "effective_target": target_n,
        "total_weight_fraction": _fraction_string(total_weight),
        "tie_break": summary["tie_break"],
        "layers": layer_details,
    }
    return {"records": selected, "summary": summary, "allocation": allocation}
