from __future__ import annotations

from typing import Any

from .numeric import format_measure, nearest_rank, rescale
from .schemas import MeasureType


def new_aggregate(measure_type: MeasureType) -> dict[str, Any]:
    scale = measure_type.scale or 0
    return {
        "count": 0,
        "non_null_count": 0,
        "sum_scaled": None,
        "scale": scale,
        "min_scaled": None,
        "max_scaled": None,
        "values": [],
        "input_types": [{"kind": measure_type.kind, "scale": scale}],
    }


def _widen(aggregate: dict[str, Any], target_scale: int) -> None:
    current = int(aggregate["scale"])
    if target_scale <= current:
        return
    for key in ("sum_scaled", "min_scaled", "max_scaled"):
        if aggregate[key] is not None:
            aggregate[key] = rescale(aggregate[key], current, target_scale)
    aggregate["scale"] = target_scale


def merge_value(
    aggregate: dict[str, Any],
    scaled_value: int | None,
    source_scale: int,
    declared_kind: str,
    declared_scale: int,
) -> None:
    aggregate["count"] = int(aggregate["count"]) + 1
    _widen(aggregate, declared_scale)
    input_type = {"kind": declared_kind, "scale": declared_scale}
    if input_type not in aggregate["input_types"]:
        aggregate["input_types"].append(input_type)

    if scaled_value is None:
        return

    aggregate["non_null_count"] = int(aggregate["non_null_count"]) + 1
    value = rescale(scaled_value, source_scale, int(aggregate["scale"]))

    if aggregate["sum_scaled"] is None:
        aggregate["sum_scaled"] = value
    else:
        aggregate["sum_scaled"] = int(aggregate["sum_scaled"]) + value

    if aggregate["min_scaled"] is None or value < int(aggregate["min_scaled"]):
        aggregate["min_scaled"] = value
    if aggregate["max_scaled"] is None or value > int(aggregate["max_scaled"]):
        aggregate["max_scaled"] = value

    aggregate["values"].append([scaled_value, source_scale])


def output_aggregate(aggregate: dict[str, Any], quantiles: list[float] | None = None) -> dict[str, Any]:
    scale = int(aggregate["scale"])
    sum_scaled = aggregate["sum_scaled"]
    min_scaled = aggregate["min_scaled"]
    max_scaled = aggregate["max_scaled"]
    payload: dict[str, Any] = {
        "count": int(aggregate["count"]),
        "non_null_count": int(aggregate["non_null_count"]),
        "sum": format_measure(sum_scaled, scale),
        "sum_scaled": None if sum_scaled is None else str(sum_scaled),
        "min": format_measure(min_scaled, scale),
        "max": format_measure(max_scaled, scale),
        "scale": scale,
        "aggregate_type": "integer" if scale == 0 else "decimal",
        "sum_method": "exact_fixed_point_integer",
        "rounding": "none",
        "input_types": aggregate.get("input_types", []),
        "numeric_rule": "all values are rescaled exactly to the widest scale; no floating point and no rounding",
    }
    if quantiles:
        values = [(int(pair[0]), int(pair[1])) for pair in aggregate.get("values", [])]
        payload["quantiles"] = [nearest_rank(values, q, scale) for q in quantiles]
        payload["quantile_method"] = "nearest_rank_exact"
    if payload["non_null_count"] == 0:
        payload["all_null"] = True
    return payload
