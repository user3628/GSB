from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

import numpy as np

from .errors import EngineError
from .hashing import MAX_PARTITIONS, deterministic_partition_ids, estimate_partition_bytes
from .precision import (
    MERGE_CHUNK_SIZE,
    UNIT_ROUNDOFF,
    CompensatedSum,
    fast_precision_bound,
    high_precision_bound,
    kahan_sum,
    merge_compensated_chunks,
    naive_sum,
)
from .snapshot import Snapshot


PartitionCallback = Callable[[int, "PartitionStats", list["PartialAggregate"]], None]
PartitionStartCallback = Callable[[int], None]
PhaseCallback = Callable[[str], None]


@dataclass(frozen=True)
class HotspotKey:
    key: int
    records_in_partition: int
    share_of_partition: float
    share_of_input: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "records_in_partition": self.records_in_partition,
            "share_of_partition": self.share_of_partition,
            "share_of_input": self.share_of_input,
        }


@dataclass(frozen=True)
class PartitionStats:
    partition_id: int
    left_count: int
    right_count: int
    total_count: int
    estimated_bytes: int
    share: float
    skew: bool
    hotspots: tuple[HotspotKey, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "left_count": self.left_count,
            "right_count": self.right_count,
            "total_count": self.total_count,
            "estimated_bytes": self.estimated_bytes,
            "share": self.share,
            "skew": self.skew,
            "hotspot_keys": [item.to_dict() for item in self.hotspots],
        }


@dataclass(frozen=True)
class PartialAggregate:
    partition_id: int
    group_key: int
    kahan_sum_value: float
    compensation: float
    naive_sum_value: float
    count: int
    non_null_count: int
    sum_abs: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "group_key": self.group_key,
            "kahan_sum_value": self.kahan_sum_value,
            "compensation": self.compensation,
            "naive_sum_value": self.naive_sum_value,
            "count": self.count,
            "non_null_count": self.non_null_count,
            "sum_abs": self.sum_abs,
        }


@dataclass(frozen=True)
class AggregateResult:
    group_key: int
    sum_value: float
    mean_value: float | None
    count: int
    non_null_count: int
    sum_abs: float
    error_bound: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_key": self.group_key,
            "sum_value": self.sum_value,
            "mean_value": self.mean_value,
            "count": self.count,
            "non_null_count": self.non_null_count,
            "sum_abs": self.sum_abs,
            "error_bound": _finite_or_none(self.error_bound),
        }


@dataclass(frozen=True)
class ExecutionResult:
    partitions: list[PartitionStats]
    partials: list[PartialAggregate]
    aggregates: list[AggregateResult]
    precision_contract: dict[str, Any]
    selected_path: str
    operations: int
    joined_rows: int
    total_input_rows: int
    skew_limit: float
    any_skew: bool
    join_type: str | None = None
    measure: str | None = None


@dataclass
class _Contribution:
    key: int
    left_index: int
    right_index: int
    value: float
    is_null: bool
    partition_id: int


def _finite_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return float(value) if math.isfinite(value) else None


def _bound_payload(bound: float) -> dict[str, Any]:
    return {
        "estimated_max_abs_error": _finite_or_none(bound),
        "unbounded": not math.isfinite(bound),
    }


def _sum_abs(values: Iterable[float]) -> float:
    try:
        return math.fsum(abs(value) for value in values)
    except OverflowError:
        return math.inf


def _validate_common(partitions: int, skew_limit: float, skew_action: str) -> None:
    if isinstance(partitions, bool) or not isinstance(partitions, int):
        raise EngineError("E_INVALID_REQUEST", "partitions must be an integer")
    if partitions < 1 or partitions > MAX_PARTITIONS:
        raise EngineError(
            "E_INVALID_REQUEST",
            f"partitions must be between 1 and {MAX_PARTITIONS}",
            {"partitions": partitions},
        )
    if isinstance(skew_limit, bool) or not isinstance(skew_limit, (int, float)):
        raise EngineError("E_INVALID_REQUEST", "skew_limit must be numeric")
    if not 0.0 < float(skew_limit) <= 1.0:
        raise EngineError(
            "E_INVALID_REQUEST", "skew_limit must be in (0, 1]", {"skew_limit": skew_limit}
        )
    if skew_action not in {"report", "reject"}:
        raise EngineError(
            "E_INVALID_REQUEST",
            "skew_action must be 'report' or 'reject'",
            {"skew_action": skew_action},
        )


def analyze_partitions(
    left: Snapshot,
    right: Snapshot,
    *,
    partitions: int,
    skew_limit: float = 0.35,
    hotspot_limit: int = 10,
) -> tuple[list[PartitionStats], np.ndarray, np.ndarray]:
    _validate_common(partitions, skew_limit, "report")
    left_parts = deterministic_partition_ids(left.keys, partitions)
    right_parts = deterministic_partition_ids(right.keys, partitions)
    left_counts = np.bincount(left_parts, minlength=partitions)
    right_counts = np.bincount(right_parts, minlength=partitions)
    total_input = int(left.row_count + right.row_count)
    stats: list[PartitionStats] = []
    for partition_id in range(partitions):
        left_indices = np.flatnonzero(left_parts == partition_id)
        right_indices = np.flatnonzero(right_parts == partition_id)
        lc = int(left_counts[partition_id])
        rc = int(right_counts[partition_id])
        total = lc + rc
        share = (total / total_input) if total_input else 0.0
        combined_keys = np.concatenate((left.keys[left_indices], right.keys[right_indices]))
        hotspots: list[HotspotKey] = []
        if combined_keys.size:
            unique_keys, counts = np.unique(combined_keys, return_counts=True)
            order = sorted(
                range(len(unique_keys)), key=lambda i: (-int(counts[i]), int(unique_keys[i]))
            )[: max(1, int(hotspot_limit))]
            hotspots = [
                HotspotKey(
                    key=int(unique_keys[i]),
                    records_in_partition=int(counts[i]),
                    share_of_partition=(float(counts[i]) / total) if total else 0.0,
                    share_of_input=(float(counts[i]) / total_input) if total_input else 0.0,
                )
                for i in order
            ]
        stats.append(
            PartitionStats(
                partition_id=partition_id,
                left_count=lc,
                right_count=rc,
                total_count=total,
                estimated_bytes=estimate_partition_bytes(lc, rc),
                share=share,
                skew=share > float(skew_limit),
                hotspots=tuple(hotspots),
            )
        )
    return stats, left_parts, right_parts


def _raise_if_skew(stats: Iterable[PartitionStats], skew_limit: float, action: str) -> None:
    skew = [item for item in stats if item.skew]
    if not skew or action != "reject":
        return
    raise EngineError(
        "E_SKEW_LIMIT",
        "one or more shuffle partitions exceed the declared skew limit",
        {
            "skew_limit": float(skew_limit),
            "skew_partitions": [
                {
                    "partition_id": item.partition_id,
                    "share": item.share,
                    "total_count": item.total_count,
                    "hotspot_keys": [hotspot.to_dict() for hotspot in item.hotspots],
                }
                for item in skew
            ],
        },
    )


def _partition_partials(
    partition_id: int, contributions: list[_Contribution]
) -> list[PartialAggregate]:
    if not contributions:
        return []
    contributions.sort(key=lambda c: (c.key, c.left_index, c.right_index))
    partials: list[PartialAggregate] = []
    start = 0
    while start < len(contributions):
        key = contributions[start].key
        end = start + 1
        while end < len(contributions) and contributions[end].key == key:
            end += 1
        group = contributions[start:end]
        values = [item.value for item in group if not item.is_null]
        compensated = kahan_sum(values)
        fast = naive_sum(values)
        partials.append(
            PartialAggregate(
                partition_id=partition_id,
                group_key=key,
                kahan_sum_value=compensated.total,
                compensation=compensated.compensation,
                naive_sum_value=fast,
                count=len(group),
                non_null_count=len(values),
                sum_abs=_sum_abs(values),
            )
        )
        start = end
    return partials


def _joined_contributions_for_partition(
    left: Snapshot,
    right: Snapshot,
    left_indices: np.ndarray,
    right_indices: np.ndarray,
    *,
    partition_id: int,
    join_type: str,
    measure: str,
) -> list[_Contribution]:
    if left_indices.size == 0:
        return []
    left_order = left_indices[np.argsort(left.keys[left_indices], kind="stable")]
    right_order = right_indices[np.argsort(right.keys[right_indices], kind="stable")]
    right_by_key: dict[int, list[int]] = {}
    for global_index in right_order.tolist():
        right_by_key.setdefault(int(right.keys[global_index]), []).append(int(global_index))
    result: list[_Contribution] = []
    for left_index in left_order.tolist():
        key = int(left.keys[left_index])
        matches = right_by_key.get(key)
        if not matches:
            if join_type != "left":
                continue
            if measure == "left":
                value = float(left.measures[left_index])
                is_null = bool(left.nulls[left_index])
            else:
                value = 0.0
                is_null = True
            result.append(
                _Contribution(
                    key=key,
                    left_index=int(left_index),
                    right_index=-1,
                    value=value,
                    is_null=is_null,
                    partition_id=partition_id,
                )
            )
            continue
        for right_index in matches:
            if measure == "left":
                value = float(left.measures[left_index])
                is_null = bool(left.nulls[left_index])
            elif measure == "right":
                value = float(right.measures[right_index])
                is_null = bool(right.nulls[right_index])
            else:
                is_null = bool(left.nulls[left_index] or right.nulls[right_index])
                value = (
                    0.0
                    if is_null
                    else float(left.measures[left_index]) + float(right.measures[right_index])
                )
            result.append(
                _Contribution(
                    key=key,
                    left_index=int(left_index),
                    right_index=int(right_index),
                    value=value,
                    is_null=is_null,
                    partition_id=partition_id,
                )
            )
    result.sort(key=lambda item: (item.key, item.left_index, item.right_index))
    return result


def _group_metadata(contributions: list[_Contribution]) -> dict[int, tuple[int, int, float]]:
    metadata: dict[int, tuple[int, int, float]] = {}
    start = 0
    while start < len(contributions):
        key = contributions[start].key
        end = start + 1
        while end < len(contributions) and contributions[end].key == key:
            end += 1
        group = contributions[start:end]
        values = [item.value for item in group if not item.is_null]
        metadata[key] = (len(group), len(values), _sum_abs(values))
        start = end
    return metadata


def _select_precision_path(
    *,
    mode: str,
    target: float | None,
    max_operations: int | None,
    high_bound: float,
    fast_bound: float,
    high_operations: int,
    fast_operations: int,
) -> tuple[str, dict[str, Any]]:
    if mode not in {"auto", "high", "fast"}:
        raise EngineError(
            "E_INVALID_REQUEST", "precision mode must be auto, high, or fast", {"mode": mode}
        )
    if target is not None and (not math.isfinite(target) or target < 0.0):
        raise EngineError(
            "E_INVALID_REQUEST", "target_abs_error must be finite and non-negative"
        )
    if max_operations is not None and (
        isinstance(max_operations, bool) or not isinstance(max_operations, int)
        or max_operations < 0
    ):
        raise EngineError(
            "E_INVALID_REQUEST", "max_operations must be a non-negative integer"
        )
    high_applicable = target is None or high_bound <= target
    fast_applicable = target is None or fast_bound <= target
    if mode == "high" and not high_applicable:
        raise EngineError(
            "E_PRECISION_BUDGET",
            "the requested precision target cannot be guaranteed by the high-precision path",
            {
                "requested_mode": mode,
                "target_abs_error": target,
                "high_path_bound": _bound_payload(high_bound),
                "fast_path_bound": _bound_payload(fast_bound),
            },
        )
    if mode == "fast" and not fast_applicable:
        raise EngineError(
            "E_PRECISION_BUDGET",
            "the fast path cannot satisfy the requested precision target",
            {
                "requested_mode": mode,
                "target_abs_error": target,
                "high_path_bound": _bound_payload(high_bound),
                "fast_path_bound": _bound_payload(fast_bound),
                "recommended_mode": "high" if high_applicable else "none",
            },
        )
    if mode == "high" and max_operations is not None and high_operations > max_operations:
        raise EngineError(
            "E_PRECISION_BUDGET",
            "computation budget is insufficient for the high-precision path",
            {
                "max_operations": max_operations,
                "high_operations": high_operations,
                "fast_operations": fast_operations,
            },
        )
    if mode == "fast" and max_operations is not None and fast_operations > max_operations:
        raise EngineError(
            "E_PRECISION_BUDGET",
            "computation budget is insufficient for the fast path",
            {
                "max_operations": max_operations,
                "high_operations": high_operations,
                "fast_operations": fast_operations,
            },
        )
    if mode == "fast":
        selected = "fast"
    elif mode == "high":
        selected = "high"
    elif target is not None and fast_applicable:
        selected = "fast"
    elif high_applicable:
        selected = "high"
    elif fast_applicable:
        selected = "fast"
    else:
        raise EngineError(
            "E_PRECISION_BUDGET",
            "neither execution path can satisfy the requested precision target",
            {
                "target_abs_error": target,
                "high_path_bound": _bound_payload(high_bound),
                "fast_path_bound": _bound_payload(fast_bound),
            },
        )
    if selected == "high" and max_operations is not None and high_operations > max_operations:
        if fast_operations <= max_operations and fast_applicable:
            selected = "fast"
        else:
            raise EngineError(
                "E_PRECISION_BUDGET",
                "computation budget cannot accommodate either applicable path",
                {
                    "max_operations": max_operations,
                    "high_operations": high_operations,
                    "fast_operations": fast_operations,
                    "target_abs_error": target,
                },
            )
    contract = {
        "requested_mode": mode,
        "selected_path": selected,
        "target_abs_error": target,
        "max_operations": max_operations,
        "unit_roundoff": UNIT_ROUNDOFF,
        "merge_chunk_size": MERGE_CHUNK_SIZE,
        "high_path": {
            "estimated_operations": high_operations,
            "formula": "(4u + 16*N*u^2) * sum(abs(x_i))",
            "bound": _bound_payload(high_bound),
            "meets_target": high_applicable,
            "conclusion": (
                "compensated canonical merge; partition-count-independent and preferred when "
                "the target is tighter than the fast bound"
            ),
        },
        "fast_path": {
            "estimated_operations": fast_operations,
            "formula": "(N*u/(1-N*u)) * sum(abs(x_i))",
            "bound": _bound_payload(fast_bound),
            "meets_target": fast_applicable,
            "conclusion": (
                "single-pass naive summation; lower cost but its bound can exceed the target "
                "under cancellation or wide magnitude spread"
            ),
        },
        "meets_target": target is None
        or (high_bound if selected == "high" else fast_bound) <= target,
    }
    return selected, contract


def _aggregate_high(contributions: list[_Contribution]) -> tuple[list[AggregateResult], float]:
    contributions.sort(key=lambda item: (item.key, item.left_index, item.right_index))
    metadata = _group_metadata(contributions)
    results: list[AggregateResult] = []
    start = 0
    max_bound = 0.0
    while start < len(contributions):
        key = contributions[start].key
        end = start + 1
        while end < len(contributions) and contributions[end].key == key:
            end += 1
        values = [item.value for item in contributions[start:end] if not item.is_null]
        chunk_partials = [
            kahan_sum(values[i : i + MERGE_CHUNK_SIZE])
            for i in range(0, len(values), MERGE_CHUNK_SIZE)
        ]
        merged = merge_compensated_chunks(chunk_partials)
        count, non_null_count, sum_abs = metadata[key]
        bound = high_precision_bound(non_null_count, sum_abs)
        mean = (merged.total / non_null_count) if non_null_count else None
        results.append(
            AggregateResult(
                group_key=key,
                sum_value=merged.total,
                mean_value=mean,
                count=count,
                non_null_count=non_null_count,
                sum_abs=sum_abs,
                error_bound=bound,
            )
        )
        max_bound = max(max_bound, bound)
        start = end
    return results, max_bound


def _aggregate_fast(partials: list[PartialAggregate]) -> tuple[list[AggregateResult], float]:
    by_key: dict[int, list[PartialAggregate]] = {}
    for partial in partials:
        by_key.setdefault(partial.group_key, []).append(partial)
    results: list[AggregateResult] = []
    max_bound = 0.0
    for key in sorted(by_key):
        group = sorted(by_key[key], key=lambda item: item.partition_id)
        total = 0.0
        for partial in group:
            total += partial.naive_sum_value
        count = sum(item.count for item in group)
        non_null_count = sum(item.non_null_count for item in group)
        sum_abs = _sum_abs(item.sum_abs for item in group)
        bound = fast_precision_bound(non_null_count, sum_abs)
        mean = (total / non_null_count) if non_null_count else None
        results.append(
            AggregateResult(
                group_key=key,
                sum_value=total,
                mean_value=mean,
                count=count,
                non_null_count=non_null_count,
                sum_abs=sum_abs,
                error_bound=bound,
            )
        )
        max_bound = max(max_bound, bound)
    return results, max_bound


def execute_shuffle(
    left: Snapshot,
    right: Snapshot,
    *,
    partitions: int,
    skew_limit: float = 0.35,
    skew_action: str = "report",
    on_partition_start: PartitionStartCallback | None = None,
    on_partition_complete: PartitionCallback | None = None,
) -> ExecutionResult:
    _validate_common(partitions, skew_limit, skew_action)
    stats, _, _ = analyze_partitions(
        left, right, partitions=partitions, skew_limit=skew_limit
    )
    _raise_if_skew(stats, skew_limit, skew_action)
    for item in stats:
        if on_partition_start is not None:
            on_partition_start(item.partition_id)
        if on_partition_complete is not None:
            on_partition_complete(item.partition_id, item, [])
    contract = {
        "requested_mode": "shuffle-only",
        "selected_path": "shuffle",
        "target_abs_error": None,
        "max_operations": None,
        "high_path": {"conclusion": "not applicable to shuffle-only requests"},
        "fast_path": {"conclusion": "not applicable to shuffle-only requests"},
        "meets_target": True,
    }
    return ExecutionResult(
        partitions=stats,
        partials=[],
        aggregates=[],
        precision_contract=contract,
        selected_path="shuffle",
        operations=int(left.row_count + right.row_count),
        joined_rows=0,
        total_input_rows=int(left.row_count + right.row_count),
        skew_limit=float(skew_limit),
        any_skew=any(item.skew for item in stats),
    )


def execute_join(
    left: Snapshot,
    right: Snapshot,
    *,
    partitions: int,
    join_type: str = "inner",
    measure: str = "left",
    skew_limit: float = 0.35,
    skew_action: str = "report",
    precision_mode: str = "auto",
    target_abs_error: float | None = None,
    max_operations: int | None = None,
    on_partition_start: PartitionStartCallback | None = None,
    on_partition_complete: PartitionCallback | None = None,
    on_merge_start: PhaseCallback | None = None,
) -> ExecutionResult:
    if join_type not in {"inner", "left"}:
        raise EngineError(
            "E_INVALID_REQUEST", "join_type must be 'inner' or 'left'", {"join_type": join_type}
        )
    if measure not in {"left", "right", "both"}:
        raise EngineError(
            "E_INVALID_REQUEST",
            "measure must be 'left', 'right', or 'both'",
            {"measure": measure},
        )
    _validate_common(partitions, skew_limit, skew_action)
    if precision_mode not in {"auto", "high", "fast"}:
        raise EngineError(
            "E_INVALID_REQUEST",
            "precision mode must be auto, high, or fast",
            {"mode": precision_mode},
        )
    if target_abs_error is not None and (
        not isinstance(target_abs_error, (int, float))
        or isinstance(target_abs_error, bool)
        or not math.isfinite(float(target_abs_error))
        or float(target_abs_error) < 0.0
    ):
        raise EngineError(
            "E_INVALID_REQUEST", "target_abs_error must be finite and non-negative"
        )
    if max_operations is not None and (
        isinstance(max_operations, bool)
        or not isinstance(max_operations, int)
        or max_operations < 0
    ):
        raise EngineError(
            "E_INVALID_REQUEST", "max_operations must be a non-negative integer"
        )
    stats, left_parts, right_parts = analyze_partitions(
        left, right, partitions=partitions, skew_limit=skew_limit
    )
    _raise_if_skew(stats, skew_limit, skew_action)
    all_contributions: list[_Contribution] = []
    all_partials: list[PartialAggregate] = []
    for item in stats:
        if on_partition_start is not None:
            on_partition_start(item.partition_id)
        left_indices = np.flatnonzero(left_parts == item.partition_id)
        right_indices = np.flatnonzero(right_parts == item.partition_id)
        contributions = _joined_contributions_for_partition(
            left,
            right,
            left_indices,
            right_indices,
            partition_id=item.partition_id,
            join_type=join_type,
            measure=measure,
        )
        partials = _partition_partials(item.partition_id, contributions)
        all_contributions.extend(contributions)
        all_partials.extend(partials)
        if on_partition_complete is not None:
            on_partition_complete(item.partition_id, item, partials)
    all_contributions.sort(key=lambda item: (item.key, item.left_index, item.right_index))
    metadata = _group_metadata(all_contributions)
    high_bound = max(
        (high_precision_bound(non_null, sum_abs) for _, non_null, sum_abs in metadata.values()),
        default=0.0,
    )
    fast_bound = max(
        (fast_precision_bound(non_null, sum_abs) for _, non_null, sum_abs in metadata.values()),
        default=0.0,
    )
    joined_rows = len(all_contributions)
    max_partition_share = max((item.share for item in stats), default=0.0)
    ideal_share = 1.0 / partitions
    skew_excess = max(0.0, max_partition_share - ideal_share)
    skew_penalty = int(joined_rows * skew_excess)
    partition_overhead = partitions * 64
    high_operations = joined_rows * 4 + len(metadata) * 4 + partition_overhead + skew_penalty
    fast_operations = joined_rows + len(metadata) * 2 + partition_overhead // 2 + skew_penalty
    selected, contract = _select_precision_path(
        mode=precision_mode,
        target=float(target_abs_error) if target_abs_error is not None else None,
        max_operations=max_operations,
        high_bound=high_bound,
        fast_bound=fast_bound,
        high_operations=high_operations,
        fast_operations=fast_operations,
    )
    if on_merge_start is not None:
        on_merge_start(selected)
    if selected == "high":
        aggregates, actual_bound = _aggregate_high(all_contributions)
        operations = high_operations
    else:
        aggregates, actual_bound = _aggregate_fast(all_partials)
        operations = fast_operations
    for aggregate in aggregates:
        if not math.isfinite(aggregate.sum_value) or (
            aggregate.mean_value is not None and not math.isfinite(aggregate.mean_value)
        ):
            raise EngineError(
                "E_NUMERIC_OVERFLOW",
                "aggregate overflowed finite float64 range",
                {"group_key": aggregate.group_key},
            )
    contract["observed_max_group_error_bound"] = _bound_payload(actual_bound)
    contract["joined_row_count"] = joined_rows
    contract["group_count"] = len(aggregates)
    contract["cost_model"] = {
        "partitions": partitions,
        "max_partition_share": max_partition_share,
        "ideal_partition_share": ideal_share,
        "skew_excess": skew_excess,
        "skew_penalty_operations": skew_penalty,
        "partition_overhead_operations": partition_overhead,
        "max_group_sum_abs": max((value[2] for value in metadata.values()), default=0.0),
        "note": (
            "partition count adds fixed overhead; skew excess adds a per-row cost penalty; "
            "sum_abs controls the numerical error bounds"
        ),
    }
    contract["row_count_semantics"] = (
        "count includes joined rows with null measures; non_null_count is the mean denominator"
    )
    return ExecutionResult(
        partitions=stats,
        partials=all_partials,
        aggregates=aggregates,
        precision_contract=contract,
        selected_path=selected,
        operations=operations,
        joined_rows=joined_rows,
        total_input_rows=int(left.row_count + right.row_count),
        skew_limit=float(skew_limit),
        any_skew=any(item.skew for item in stats),
        join_type=join_type,
        measure=measure,
    )
