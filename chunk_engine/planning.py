from __future__ import annotations

import bisect
from dataclasses import dataclass
from math import ceil
from typing import Any, Iterable, Sequence

from .errors import (
    BusinessError,
    E_INVALID_REQUEST,
    E_KEY_SET_TOO_LARGE,
    E_MEMORY_BUDGET_EXCEEDED,
)
from .snapshot import Snapshot


@dataclass(frozen=True)
class FetchTask:
    task_no: int
    block_start: int
    block_end: int
    block_nos: tuple[int, ...]
    key_start: int
    key_end: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_no": self.task_no,
            "block_start": self.block_start,
            "block_end": self.block_end,
            "block_nos": list(self.block_nos),
            "key_start": self.key_start,
            "key_end": self.key_end,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "FetchTask":
        return cls(
            task_no=int(raw["task_no"]),
            block_start=int(raw["block_start"]),
            block_end=int(raw["block_end"]),
            block_nos=tuple(int(value) for value in raw["block_nos"]),
            key_start=int(raw["key_start"]),
            key_end=int(raw["key_end"]),
        )


@dataclass(frozen=True)
class SplitRecord:
    block_no: int
    column: str
    row_count: int
    estimated_block_bytes: int
    effective_task_budget_bytes: int
    bytes_per_row: int
    chunk_rows: int
    subblock_count: int
    chunk_estimated_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "block_no": self.block_no,
            "column": self.column,
            "row_count": self.row_count,
            "estimated_block_bytes": self.estimated_block_bytes,
            "effective_task_budget_bytes": self.effective_task_budget_bytes,
            "bytes_per_row": self.bytes_per_row,
            "chunk_rows": self.chunk_rows,
            "subblock_count": self.subblock_count,
            "chunk_estimated_bytes": self.chunk_estimated_bytes,
            "reason": "single decoded block exceeds the per-task budget; decode row subranges",
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SplitRecord":
        return cls(
            block_no=int(raw["block_no"]),
            column=str(raw["column"]),
            row_count=int(raw["row_count"]),
            estimated_block_bytes=int(raw["estimated_block_bytes"]),
            effective_task_budget_bytes=int(raw["effective_task_budget_bytes"]),
            bytes_per_row=int(raw["bytes_per_row"]),
            chunk_rows=int(raw["chunk_rows"]),
            subblock_count=int(raw["subblock_count"]),
            chunk_estimated_bytes=int(raw["chunk_estimated_bytes"]),
        )


@dataclass(frozen=True)
class ExecutionPlan:
    keys: tuple[int, ...]
    columns: tuple[str, ...]
    hit_blocks: tuple[int, ...]
    fetch_tasks: tuple[FetchTask, ...]
    selected_path: str
    parallel_blocks: int
    peak_memory_estimate_bytes: int
    remaining_budget_bytes: int
    single_task_budget_bytes: int
    effective_task_budget_bytes: int
    reason: str
    split_records: tuple[SplitRecord, ...]
    block_estimates: tuple[tuple[int, int], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "keys": list(self.keys),
            "columns": list(self.columns),
            "hit_blocks": list(self.hit_blocks),
            "fetch_tasks": [task.to_dict() for task in self.fetch_tasks],
            "selected_path": self.selected_path,
            "parallel_blocks": self.parallel_blocks,
            "peak_memory_estimate_bytes": self.peak_memory_estimate_bytes,
            "remaining_budget_bytes": self.remaining_budget_bytes,
            "single_task_budget_bytes": self.single_task_budget_bytes,
            "effective_task_budget_bytes": self.effective_task_budget_bytes,
            "reason": self.reason,
            "split_records": [record.to_dict() for record in self.split_records],
            "block_estimates": [
                {"block_no": block_no, "estimated_bytes": estimate}
                for block_no, estimate in self.block_estimates
            ],
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ExecutionPlan":
        return cls(
            keys=tuple(int(value) for value in raw["keys"]),
            columns=tuple(str(value) for value in raw["columns"]),
            hit_blocks=tuple(int(value) for value in raw["hit_blocks"]),
            fetch_tasks=tuple(FetchTask.from_dict(item) for item in raw["fetch_tasks"]),
            selected_path=str(raw["selected_path"]),
            parallel_blocks=int(raw["parallel_blocks"]),
            peak_memory_estimate_bytes=int(raw["peak_memory_estimate_bytes"]),
            remaining_budget_bytes=int(raw["remaining_budget_bytes"]),
            single_task_budget_bytes=int(raw["single_task_budget_bytes"]),
            effective_task_budget_bytes=int(raw["effective_task_budget_bytes"]),
            reason=str(raw["reason"]),
            split_records=tuple(SplitRecord.from_dict(item) for item in raw.get("split_records", [])),
            block_estimates=tuple(
                (int(item["block_no"]), int(item["estimated_bytes"])) for item in raw.get("block_estimates", [])
            ),
        )

    def effective_peak(self) -> int:
        return self.peak_memory_estimate_bytes


def normalize_keys(raw_keys: Any, max_keys: int) -> tuple[int, ...]:
    if raw_keys is None:
        raw_keys = []
    if not isinstance(raw_keys, (list, tuple)):
        raise BusinessError(E_INVALID_REQUEST, "keys must be an array", {"field": "keys"}, 400)
    if len(raw_keys) > max_keys:
        raise BusinessError(
            E_KEY_SET_TOO_LARGE,
            "key set exceeds the configured limit",
            {"key_count": len(raw_keys), "max_keys": max_keys},
            413,
        )
    result: set[int] = set()
    for value in raw_keys:
        if isinstance(value, bool) or not isinstance(value, int):
            raise BusinessError(
                E_INVALID_REQUEST,
                "every primary key must be an int64 value",
                {"value": value},
                400,
            )
        if value < -(2**63) or value > 2**63 - 1:
            raise BusinessError(
                E_INVALID_REQUEST,
                "primary key is outside the int64 range",
                {"value": value},
                400,
            )
        result.add(value)
    return tuple(sorted(result))


def normalize_columns(snapshot: Snapshot, raw_columns: Any) -> tuple[str, ...]:
    if raw_columns is None:
        raw_columns = [snapshot.primary_key]
    if not isinstance(raw_columns, (list, tuple)):
        raise BusinessError(E_INVALID_REQUEST, "columns must be an array", {"field": "columns"}, 400)
    requested: list[str] = []
    for value in raw_columns:
        if not isinstance(value, str):
            raise BusinessError(E_INVALID_REQUEST, "column names must be strings", {"value": value}, 400)
        if value not in snapshot.columns:
            raise BusinessError(
                E_INVALID_REQUEST,
                "requested column does not exist",
                {"column": value, "available_columns": list(snapshot.columns)},
                400,
            )
        if value not in requested:
            requested.append(value)
    # The primary key is always projected so every row retains a stable order.
    if snapshot.primary_key not in requested:
        requested.insert(0, snapshot.primary_key)
    elif requested[0] != snapshot.primary_key:
        requested.remove(snapshot.primary_key)
        requested.insert(0, snapshot.primary_key)
    return tuple(requested)


def locate_hit_blocks(snapshot: Snapshot, keys: Sequence[int]) -> tuple[int, ...]:
    hit: set[int] = set()
    for key in keys:
        block = snapshot.block_for_key(key)
        if block is not None:
            hit.add(block.block_no)
    return tuple(sorted(hit))


def build_fetch_tasks(snapshot: Snapshot, keys: Sequence[int], hit_blocks: Sequence[int]) -> tuple[FetchTask, ...]:
    if not hit_blocks:
        return ()
    keys_by_block: dict[int, list[int]] = {block_no: [] for block_no in hit_blocks}
    for key in keys:
        block = snapshot.block_for_key(key)
        if block is not None:
            keys_by_block[block.block_no].append(key)
    groups: list[list[int]] = []
    for block_no in hit_blocks:
        if groups and block_no == groups[-1][-1] + 1:
            groups[-1].append(block_no)
        else:
            groups.append([block_no])
    tasks: list[FetchTask] = []
    for task_no, group in enumerate(groups):
        group_keys = [key for block_no in group for key in keys_by_block[block_no]]
        tasks.append(
            FetchTask(
                task_no=task_no,
                block_start=group[0],
                block_end=group[-1],
                block_nos=tuple(group),
                key_start=min(group_keys),
                key_end=max(group_keys),
            )
        )
    # Verify the task intervals are disjoint and cover every hit block.
    for left, right in zip(tasks, tasks[1:]):
        if left.key_end >= right.key_start or left.block_end >= right.block_start:
            raise AssertionError("fetch task ranges overlap")
    covered = {block_no for task in tasks for block_no in task.block_nos}
    if covered != set(hit_blocks):
        raise AssertionError("fetch tasks do not cover all hit blocks")
    return tuple(tasks)


def _target_key_count(snapshot: Snapshot, block_no: int, keys: Sequence[int]) -> int:
    block = snapshot.primary_column.blocks[block_no]
    left = bisect.bisect_left(keys, block.key_min)
    right = bisect.bisect_right(keys, block.key_max)
    return max(0, right - left)


def _split_for_budget(
    snapshot: Snapshot,
    block_no: int,
    column: str,
    budget: int,
) -> tuple[int, SplitRecord | None]:
    meta = snapshot.column_block(column, block_no)
    estimate = meta.decompressed_bytes
    if estimate <= budget:
        return estimate, None
    bytes_per_row = max(1, ceil(estimate / meta.row_count))
    if bytes_per_row > budget:
        raise BusinessError(
            E_MEMORY_BUDGET_EXCEEDED,
            "a single decoded row exceeds the available task budget",
            {
                "block_no": block_no,
                "column": column,
                "bytes_per_row": bytes_per_row,
                "effective_task_budget_bytes": budget,
            },
            507,
        )
    chunk_rows = max(1, budget // bytes_per_row)
    if chunk_rows >= meta.row_count:
        chunk_rows = meta.row_count - 1
    if chunk_rows <= 0:
        raise BusinessError(
            E_MEMORY_BUDGET_EXCEEDED,
            "block cannot be split into a non-empty subblock within budget",
            {
                "block_no": block_no,
                "column": column,
                "effective_task_budget_bytes": budget,
            },
            507,
        )
    chunk_bytes = min(estimate, chunk_rows * bytes_per_row)
    record = SplitRecord(
        block_no=block_no,
        column=column,
        row_count=meta.row_count,
        estimated_block_bytes=estimate,
        effective_task_budget_bytes=budget,
        bytes_per_row=bytes_per_row,
        chunk_rows=chunk_rows,
        subblock_count=ceil(meta.row_count / chunk_rows),
        chunk_estimated_bytes=chunk_bytes,
    )
    return chunk_bytes, record


def plan_query(
    snapshot: Snapshot,
    raw_keys: Any,
    raw_columns: Any,
    *,
    max_keys: int,
    total_memory_budget_bytes: int,
    remaining_memory_budget_bytes: int,
    single_task_budget_bytes: int,
    max_parallel_blocks: int,
    execution_mode: str = "auto",
) -> ExecutionPlan:
    if total_memory_budget_bytes <= 0:
        raise BusinessError(E_INVALID_REQUEST, "memory budget must be positive")
    remaining = max(0, min(total_memory_budget_bytes, remaining_memory_budget_bytes))
    if execution_mode not in ("auto", "stream", "batch"):
        raise BusinessError(E_INVALID_REQUEST, "execution_mode must be auto, stream, or batch")
    keys = normalize_keys(raw_keys, max_keys)
    columns = normalize_columns(snapshot, raw_columns)
    hit_blocks = locate_hit_blocks(snapshot, keys)
    tasks = build_fetch_tasks(snapshot, keys, hit_blocks)

    if not hit_blocks:
        return ExecutionPlan(
            keys=keys,
            columns=columns,
            hit_blocks=(),
            fetch_tasks=(),
            selected_path="stream",
            parallel_blocks=1,
            peak_memory_estimate_bytes=0,
            remaining_budget_bytes=remaining,
            single_task_budget_bytes=single_task_budget_bytes,
            effective_task_budget_bytes=0,
            reason="empty key set or no keys fall inside snapshot block ranges",
            split_records=(),
            block_estimates=(),
        )
    if remaining <= 0:
        raise BusinessError(
            E_MEMORY_BUDGET_EXCEEDED,
            "no memory budget remains for decoded blocks",
            {"required_at_least_bytes": 1, "remaining_bytes": remaining},
            507,
        )
    effective_task_budget = min(single_task_budget_bytes, remaining)
    split_records: list[SplitRecord] = []
    block_estimates: list[tuple[int, int]] = []
    for block_no in hit_blocks:
        estimate = 0
        for column in columns:
            column_estimate, split = _split_for_budget(snapshot, block_no, column, effective_task_budget)
            estimate += column_estimate
            if split is not None:
                split_records.append(split)
        # Rows for matched keys, stable-key bookkeeping, and decoder temporaries.
        target_keys = _target_key_count(snapshot, block_no, keys)
        estimate += target_keys * 96 + 1024
        block_estimates.append((block_no, estimate))

    max_block_estimate = max(estimate for _, estimate in block_estimates)
    if max_block_estimate > remaining:
        raise BusinessError(
            E_MEMORY_BUDGET_EXCEEDED,
            "one partitioned block still exceeds the remaining memory budget",
            {
                "peak_estimate_bytes": max_block_estimate,
                "remaining_bytes": remaining,
                "effective_task_budget_bytes": effective_task_budget,
            },
            507,
        )
    fit_lanes = max(1, min(max_parallel_blocks, len(hit_blocks), remaining // max_block_estimate))
    if execution_mode == "stream":
        path = "stream"
        parallel = 1
        peak = max_block_estimate
        reason = "execution_mode=stream requested by caller"
    elif execution_mode == "batch":
        path = "batch"
        parallel = fit_lanes
        peak = parallel * max_block_estimate
        reason = "execution_mode=batch requested by caller and all blocks fit the remaining budget"
    elif fit_lanes >= 2:
        path = "batch"
        parallel = fit_lanes
        peak = parallel * max_block_estimate
        reason = (
            f"{len(hit_blocks)} hit blocks fit {parallel} parallel decode lanes within "
            f"{remaining} remaining bytes; multi-block batching amortizes decode and commit overhead"
        )
    else:
        path = "stream"
        parallel = 1
        peak = max_block_estimate
        reason = (
            f"only {fit_lanes} decode lane fits within {remaining} remaining bytes; "
            "streaming avoids speculative parallel residency"
        )
    if peak > remaining:
        raise BusinessError(
            E_MEMORY_BUDGET_EXCEEDED,
            "selected execution path exceeds the remaining memory budget",
            {
                "selected_path": path,
                "peak_estimate_bytes": peak,
                "remaining_bytes": remaining,
            },
            507,
        )
    return ExecutionPlan(
        keys=keys,
        columns=columns,
        hit_blocks=hit_blocks,
        fetch_tasks=tasks,
        selected_path=path,
        parallel_blocks=parallel,
        peak_memory_estimate_bytes=peak,
        remaining_budget_bytes=remaining,
        single_task_budget_bytes=single_task_budget_bytes,
        effective_task_budget_bytes=effective_task_budget,
        reason=reason,
        split_records=tuple(split_records),
        block_estimates=tuple(block_estimates),
    )


def keys_for_block(keys: Iterable[int], snapshot: Snapshot, block_no: int) -> tuple[int, ...]:
    block = snapshot.primary_column.blocks[block_no]
    return tuple(key for key in keys if block.key_min <= key <= block.key_max)
