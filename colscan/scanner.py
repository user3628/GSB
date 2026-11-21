from __future__ import annotations

import hashlib
import math
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np

from .errors import BusinessError, error
from .predicate import evaluate_predicate, filter_indexes, predicate_columns
from .repository import StateStore, compact_json
from .snapshot import Snapshot, decode_column_block, snapshot_from_document
from .types import json_scalar


@dataclass(frozen=True)
class ScanBudget:
    max_concurrent_tasks: int
    per_task_memory_bytes: int
    round_time_budget_ms: float
    queue_capacity: int
    degrade_mode: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "per_task_memory_bytes": self.per_task_memory_bytes,
            "round_time_budget_ms": self.round_time_budget_ms,
            "queue_capacity": self.queue_capacity,
            "degrade_mode": self.degrade_mode,
        }

    @property
    def round_time_budget_us(self) -> float:
        return self.round_time_budget_ms * 1000.0


@dataclass(frozen=True)
class QuerySpec:
    columns: tuple[str, ...]
    predicate: dict[str, Any] | None
    block_start: int
    block_end: int
    offset: int
    limit: int | None
    budget: ScanBudget

    @property
    def needed_columns(self) -> tuple[str, ...]:
        names = list(self.columns)
        for name in sorted(predicate_columns(self.predicate)):
            if name not in names:
                names.append(name)
        return tuple(names)


@dataclass(frozen=True)
class WorkItem:
    block_seq: int
    start: int
    end: int

    @property
    def count(self) -> int:
        return self.end - self.start

    @property
    def work_id(self) -> str:
        return f"b{self.block_seq:04d}:{self.start}-{self.end}"


@dataclass(frozen=True)
class WorkEstimate:
    memory_bytes: int
    decompress_us: int
    raw_size: int
    compressed_size: int
    column_breakdown: tuple[dict[str, Any], ...]


def _segment_rows(snapshot: Snapshot, block_seq: int, needed_columns: Sequence[str]) -> int:
    sizes = []
    for column in needed_columns:
        block = snapshot.block(column, block_seq)
        sizes.append(int(block.encoding_meta.get("segment_rows", 0)))
    positive = [value for value in sizes if value > 0]
    return min(positive) if positive else 1


def estimate_work(snapshot: Snapshot, item: WorkItem, needed_columns: Sequence[str]) -> WorkEstimate:
    memory = 512
    decompress_us = 60.0
    total_raw = 0
    total_compressed = 0
    breakdown: list[dict[str, Any]] = []
    for column_name in needed_columns:
        block = snapshot.block(column_name, item.block_seq)
        fraction = item.count / block.row_count if block.row_count else 0.0
        raw_size = max(1, int(math.ceil(block.raw_size * fraction)))
        compressed_size = max(1, int(math.ceil(block.compressed_size * fraction)))
        total_raw += raw_size
        total_compressed += compressed_size
        memory += raw_size + item.count * 16
        if block.encoding == "fixed":
            decompress_us += 10 + compressed_size * 0.90 + raw_size * 0.02
        elif block.encoding == "rle":
            runs = max(1, min(item.count, int(block.encoding_meta.get("run_count", block.row_count))))
            memory += runs * 12
            decompress_us += 20 + compressed_size * 1.40 + raw_size * 0.04 + runs * 3
        elif block.encoding == "dict":
            dict_size = max(1, min(item.count, int(block.encoding_meta.get("dict_size", 1))))
            memory += dict_size * 32 + 64
            decompress_us += 30 + compressed_size * 2.00 + raw_size * 0.06 + dict_size * 4
        else:
            decompress_us += compressed_size * 2.0 + raw_size * 0.10
        decompress_us += item.count * 1.5
        breakdown.append(
            {
                "column": column_name,
                "encoding": block.encoding,
                "raw_size": raw_size,
                "compressed_size": compressed_size,
                "compression_ratio": block.compression_ratio,
                "encoding_meta": block.encoding_meta,
            }
        )
    return WorkEstimate(
        memory_bytes=int(memory),
        decompress_us=int(math.ceil(decompress_us)),
        raw_size=total_raw,
        compressed_size=total_compressed,
        column_breakdown=tuple(breakdown),
    )


def _split_item(item: WorkItem, segment_rows: int) -> tuple[WorkItem, WorkItem] | None:
    if item.count <= 1:
        return None
    midpoint = (item.start + item.end) // 2
    boundary = ((midpoint + segment_rows - 1) // segment_rows) * segment_rows
    candidates = [candidate for candidate in (boundary, boundary - segment_rows) if item.start < candidate < item.end]
    if not candidates:
        return None
    split_at = min(candidates, key=lambda candidate: abs(candidate - midpoint))
    return WorkItem(item.block_seq, item.start, split_at), WorkItem(item.block_seq, split_at, item.end)


class ScanEngine:
    def __init__(self, store: StateStore, *, task_delay_ms: int | None = None) -> None:
        self.store = store
        if task_delay_ms is None:
            try:
                task_delay_ms = int(os.environ.get("COLSCAN_TASK_DELAY_MS", "0"))
            except ValueError:
                task_delay_ms = 0
        self.task_delay_ms = max(0, task_delay_ms)

    def execute(self, query: dict[str, Any]) -> None:
        query_id = str(query["query_id"])
        try:
            self.store.set_query_status(query_id, "RUNNING")
            snapshot = snapshot_from_document(
                self.store.snapshot_document(int(query["snapshot_version"])),
                verify_checksum=True,
            )
            spec = self._spec_from_request(query["request"])
            self._execute_inner(query_id, snapshot, spec)
        except BusinessError as exc:
            self.store.set_query_status(query_id, "FAILED", error_payload=exc.to_dict()["error"])
        except Exception as exc:
            self.store.set_query_status(
                query_id,
                "FAILED",
                error_payload={"code": "E_INTERNAL", "message": str(exc), "details": {"type": type(exc).__name__}},
            )

    def _spec_from_request(self, request: dict[str, Any]) -> QuerySpec:
        budget_data = request["budget"]
        return QuerySpec(
            columns=tuple(request["columns"]),
            predicate=request.get("predicate"),
            block_start=int(request["block_start"]),
            block_end=int(request["block_end"]),
            offset=int(request.get("offset", 0)),
            limit=request.get("limit"),
            budget=ScanBudget(
                max_concurrent_tasks=int(budget_data["max_concurrent_tasks"]),
                per_task_memory_bytes=int(budget_data["per_task_memory_bytes"]),
                round_time_budget_ms=float(budget_data["round_time_budget_ms"]),
                queue_capacity=int(budget_data["queue_capacity"]),
                degrade_mode=str(budget_data["degrade_mode"]),
            ),
        )

    def _execute_inner(self, query_id: str, snapshot: Snapshot, spec: QuerySpec) -> None:
        shards = self.store.shards(query_id)
        pending_blocks = [shard for shard in shards if shard["status"] != "COMMITTED"]
        for shard in pending_blocks:
            self.store.claim_shard(query_id, int(shard["block_seq"]))
        planned: list[WorkItem] = []
        decisions: list[dict[str, Any]] = []
        expected_by_block: dict[int, int] = {}
        for shard in pending_blocks:
            block_seq = int(shard["block_seq"])
            items, plan_decisions = self._plan_block(snapshot, block_seq, spec, query_id)
            if items:
                expected_by_block[block_seq] = len(items)
                planned.extend(items)
            decisions.extend(plan_decisions)
        self.store.add_decisions(query_id, decisions)
        if planned:
            self._schedule(query_id, snapshot, spec, planned, expected_by_block)
        shards_after = self.store.shards(query_id)
        failed = [shard for shard in shards_after if shard["status"] == "FAILED"]
        if failed:
            payload = failed[0]["error"] or {"code": "E_INTERNAL", "message": "scan shard failed"}
            raise BusinessError(
                str(payload.get("code", "E_INTERNAL")),
                str(payload.get("message", "scan shard failed")),
                details=dict(payload.get("details", {})),
            )
        uncommitted = [shard for shard in shards_after if shard["status"] != "COMMITTED"]
        if uncommitted:
            raise error(
                "E_STATE_CONFLICT",
                "scan ended with uncommitted shards",
                http_status=409,
                details={"shards": [shard["block_seq"] for shard in uncommitted]},
            )
        self._finalize(query_id, snapshot, spec)

    def _plan_block(
        self,
        snapshot: Snapshot,
        block_seq: int,
        spec: QuerySpec,
        query_id: str,
    ) -> tuple[list[WorkItem], list[dict[str, Any]]]:
        row_block = snapshot.row_blocks[block_seq]
        segment_rows = _segment_rows(snapshot, block_seq, spec.needed_columns)
        stack = [WorkItem(block_seq, 0, row_block.row_count)]
        accepted: list[WorkItem] = []
        decisions: list[dict[str, Any]] = []
        while stack:
            item = stack.pop()
            estimate = estimate_work(snapshot, item, spec.needed_columns)
            exceeds_memory = estimate.memory_bytes > spec.budget.per_task_memory_bytes
            exceeds_round = estimate.decompress_us > spec.budget.round_time_budget_us
            if not exceeds_memory and not exceeds_round:
                accepted.append(item)
                continue
            split_result = _split_item(item, segment_rows)
            if split_result is not None:
                left, right = split_result
                decisions.append(
                    {
                        "query_id": query_id,
                        "round": 0,
                        "phase": "plan",
                        "action": "split",
                        "reason": (
                            "estimated_memory_exceeds_per_task_budget"
                            if exceeds_memory
                            else "estimated_decompress_time_exceeds_round_budget"
                        ),
                        "block_seq": block_seq,
                        "block_id": row_block.block_id,
                        "work_id": item.work_id,
                        "row_range": [item.start, item.end],
                        "split_ranges": [[left.start, left.end], [right.start, right.end]],
                        "estimated_memory_bytes": estimate.memory_bytes,
                        "estimated_decompress_us": estimate.decompress_us,
                        "estimated_raw_size": estimate.raw_size,
                        "estimated_compressed_size": estimate.compressed_size,
                        "budget": spec.budget.to_dict(),
                        "columns": list(estimate.column_breakdown),
                        "affected_blocks": [row_block.block_id],
                        "affected_work_ids": [left.work_id, right.work_id],
                    }
                )
                stack.append(right)
                stack.append(left)
                continue
            if spec.budget.degrade_mode == "strict":
                code = "E_TASK_MEMORY_EXCEEDED" if exceeds_memory else "E_ROUND_BUDGET_EXCEEDED"
                raise error(
                    code,
                    "block cannot be split further to satisfy the scan budget",
                    details={
                        "block_id": row_block.block_id,
                        "work_id": item.work_id,
                        "estimated_memory_bytes": estimate.memory_bytes,
                        "estimated_decompress_us": estimate.decompress_us,
                        "budget": spec.budget.to_dict(),
                    },
                )
            decisions.append(
                {
                    "query_id": query_id,
                    "round": 0,
                    "phase": "plan",
                    "action": "admit_forced_overrun",
                    "reason": "minimum_segment_granularity_exceeds_budget",
                    "block_seq": block_seq,
                    "block_id": row_block.block_id,
                    "work_id": item.work_id,
                    "row_range": [item.start, item.end],
                    "estimated_memory_bytes": estimate.memory_bytes,
                    "estimated_decompress_us": estimate.decompress_us,
                    "budget": spec.budget.to_dict(),
                    "columns": list(estimate.column_breakdown),
                    "affected_blocks": [row_block.block_id],
                }
            )
            accepted.append(item)
        accepted.sort(key=lambda item: (item.block_seq, item.start, item.end))
        return accepted, decisions

    def _schedule(
        self,
        query_id: str,
        snapshot: Snapshot,
        spec: QuerySpec,
        planned: list[WorkItem],
        expected_by_block: dict[int, int],
    ) -> None:
        pending = sorted(planned, key=lambda item: (item.block_seq, item.start, item.end))
        received: dict[int, list[dict[str, Any]]] = {block_seq: [] for block_seq in expected_by_block}
        completed_counts: dict[int, int] = {block_seq: 0 for block_seq in expected_by_block}
        round_no = 0
        with ThreadPoolExecutor(
            max_workers=spec.budget.max_concurrent_tasks,
            thread_name_prefix=f"scan-{query_id[:12]}",
        ) as pool:
            while pending:
                round_no += 1
                admitted: list[WorkItem] = []
                deferred: list[WorkItem] = []
                admitted_blocks: set[int] = set()
                used_time_us = 0
                decisions: list[dict[str, Any]] = []
                for item in pending:
                    estimate = estimate_work(snapshot, item, spec.needed_columns)
                    block = snapshot.block(spec.needed_columns[0], item.block_seq)
                    if item.block_seq in admitted_blocks:
                        deferred.append(item)
                        action, reason = "queue", "logical_block_already_scanned_this_round"
                    elif len(admitted) >= spec.budget.max_concurrent_tasks:
                        deferred.append(item)
                        action, reason = "queue", "max_concurrent_tasks_reached"
                    elif admitted and used_time_us + estimate.decompress_us > spec.budget.round_time_budget_us:
                        deferred.append(item)
                        action, reason = "queue", "round_time_budget_would_be_exceeded"
                    else:
                        admitted.append(item)
                        admitted_blocks.add(item.block_seq)
                        used_time_us += estimate.decompress_us
                        action, reason = "admit", "within_memory_concurrency_time_budget"
                        if estimate.decompress_us > spec.budget.round_time_budget_us:
                            action, reason = "admit_forced_overrun", "minimum_segment_exceeds_round_time_budget"
                    decisions.append(self._decision(query_id, round_no, action, reason, item, block.block_id, estimate, used_time_us, spec))
                if not admitted and pending:
                    item = pending[0]
                    deferred.remove(item)
                    admitted.append(item)
                    admitted_blocks.add(item.block_seq)
                    estimate = estimate_work(snapshot, item, spec.needed_columns)
                    used_time_us += estimate.decompress_us
                    block_id = snapshot.block(spec.needed_columns[0], item.block_seq).block_id
                    decisions.append(
                        self._decision(
                            query_id,
                            round_no,
                            "admit_forced_overrun",
                            "no_item_fit_after_admission",
                            item,
                            block_id,
                            estimate,
                            used_time_us,
                            spec,
                        )
                    )
                if len(deferred) > spec.budget.queue_capacity:
                    self.store.add_decisions(query_id, decisions)
                    raise error(
                        "E_SCAN_QUEUE_FULL",
                        "bounded scan waiting queue is full",
                        http_status=429,
                        details={
                            "waiting_tasks": len(deferred),
                            "queue_capacity": spec.budget.queue_capacity,
                            "round": round_no,
                        },
                    )
                self.store.add_decisions(query_id, decisions)
                futures: dict[Future[list[dict[str, Any]]], WorkItem] = {
                    pool.submit(self._scan_work_item, snapshot, spec, item): item for item in admitted
                }
                round_errors: list[BaseException] = []
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        rows = future.result()
                    except BaseException as exc:
                        round_errors.append(exc)
                        payload = exc.to_dict()["error"] if isinstance(exc, BusinessError) else {
                            "code": "E_INTERNAL",
                            "message": str(exc),
                            "details": {},
                        }
                        self.store.mark_shard_failed(query_id, item.block_seq, payload)
                        continue
                    received[item.block_seq].extend(rows)
                    completed_counts[item.block_seq] += 1
                    if completed_counts[item.block_seq] == expected_by_block[item.block_seq]:
                        self._commit_block(query_id, snapshot, item.block_seq, received[item.block_seq])
                if round_errors:
                    first = round_errors[0]
                    if isinstance(first, BusinessError):
                        raise first
                    raise error("E_INTERNAL", str(first), details={"type": type(first).__name__})
                pending = deferred

    @staticmethod
    def _decision(
        query_id: str,
        round_no: int,
        action: str,
        reason: str,
        item: WorkItem,
        block_id: str,
        estimate: WorkEstimate,
        used_time_us: int,
        spec: QuerySpec,
    ) -> dict[str, Any]:
        return {
            "query_id": query_id,
            "round": round_no,
            "phase": "schedule",
            "action": action,
            "reason": reason,
            "block_seq": item.block_seq,
            "block_id": block_id,
            "work_id": item.work_id,
            "row_range": [item.start, item.end],
            "estimated_memory_bytes": estimate.memory_bytes,
            "estimated_decompress_us": estimate.decompress_us,
            "estimated_raw_size": estimate.raw_size,
            "estimated_compressed_size": estimate.compressed_size,
            "round_time_used_us": used_time_us,
            "budget": spec.budget.to_dict(),
            "columns": list(estimate.column_breakdown),
            "affected_blocks": [block_id],
        }

    def _scan_work_item(self, snapshot: Snapshot, spec: QuerySpec, item: WorkItem) -> list[dict[str, Any]]:
        if self.task_delay_ms:
            time.sleep(self.task_delay_ms / 1000.0)
        arrays: dict[str, np.ndarray] = {}
        null_masks: dict[str, np.ndarray] = {}
        dtype_map: dict[str, str] = {}
        for column_name in spec.needed_columns:
            values, nulls = decode_column_block(snapshot, column_name, item.block_seq, (item.start, item.end))
            arrays[column_name] = values
            null_masks[column_name] = nulls
            dtype_map[column_name] = snapshot.column(column_name).dtype
        mask = evaluate_predicate(spec.predicate, arrays, null_masks, dtype_map)
        rows: list[dict[str, Any]] = []
        for local_index in filter_indexes(mask):
            output_values = []
            for column_name in spec.columns:
                if bool(null_masks[column_name][local_index]):
                    output_values.append(None)
                else:
                    output_values.append(json_scalar(dtype_map[column_name], arrays[column_name][local_index]))
            rows.append({"row_offset": item.start + int(local_index), "values": output_values})
        return rows

    def _commit_block(
        self,
        query_id: str,
        snapshot: Snapshot,
        block_seq: int,
        rows: list[dict[str, Any]],
    ) -> None:
        rows.sort(key=lambda row: int(row["row_offset"]))
        result = {
            "block_seq": block_seq,
            "block_id": snapshot.row_blocks[block_seq].block_id,
            "row_count": len(rows),
            "rows": rows,
        }
        intent_hash = hashlib.sha256(compact_json(result).encode("utf-8")).hexdigest()
        self.store.record_intent(query_id, block_seq, intent_hash)
        self.store.commit_shard_result(query_id, block_seq, result)

    def _finalize(self, query_id: str, snapshot: Snapshot, spec: QuerySpec) -> None:
        merged_rows: list[list[Any]] = []
        for shard_result in self.store.shard_results(query_id):
            for row in sorted(shard_result["result"]["rows"], key=lambda item: int(item["row_offset"])):
                merged_rows.append(row["values"])
        result = {
            "columns": [{"name": name, "dtype": snapshot.column(name).dtype} for name in spec.columns],
            "rows": merged_rows,
            "order_key": ["block_seq", "row_offset"],
        }
        self.store.finalize_query(query_id, result=result, total_matched=len(merged_rows))

