from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable

from .domain import EngineConfig, Layout, ObjectHealth, ObjectLayout, Trace, TraceRequest
from .health import bitmap_to_hex, evaluate_object_health


@dataclass
class TaskSpec:
    task_id: str
    request_id: str
    object_id: str
    shard_index: int
    expected_digest: str
    bytes: int
    priority: int
    arrival_ms: int
    duration_us: int
    request_ids: set[str] = field(default_factory=set)
    wave: int = 0
    estimated_start_us: int = 0
    estimated_end_us: int = 0

    @property
    def key(self) -> tuple[str, int]:
        return self.object_id, self.shard_index

    def to_store(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "request_id": self.request_id,
            "object_id": self.object_id,
            "shard_index": self.shard_index,
            "bytes": self.bytes,
            "priority": self.priority,
            "arrival_ms": self.arrival_ms,
            "wave": self.wave,
            "estimated_start_us": self.estimated_start_us,
            "estimated_end_us": self.estimated_end_us,
            "duration_us": self.duration_us,
            "expected_digest": self.expected_digest,
        }


@dataclass
class ReusableShard:
    object_id: str
    shard_index: int
    digest: str
    task_id: str
    round_id: str


@dataclass
class RequestAnalysis:
    request: TraceRequest
    obj: ObjectLayout
    health: ObjectHealth
    decision: dict[str, Any]
    rebuild_candidate: bool = False
    missing_requested: tuple[int, ...] = ()


@dataclass
class PlanningResult:
    decisions: list[dict[str, Any]]
    tasks: list[TaskSpec]
    task_latency_by_request: dict[str, int]
    estimated_makespan_us: int
    consumed_bytes: int


def _task_id(layout_version: str, object_id: str, shard_index: int, expected_digest: str) -> str:
    payload = f"{layout_version}\0{object_id}\0{shard_index}\0{expected_digest}".encode("utf-8")
    return "rt-" + hashlib.sha256(payload).hexdigest()[:20]


def _task_duration(bytes_count: int, config: EngineConfig) -> int:
    bandwidth = config.rebuild_bandwidth_bytes_per_us
    transfer = (bytes_count + bandwidth - 1) // bandwidth
    return config.rebuild_setup_us + transfer + config.rebuild_per_shard_latency_us


def _read_overhead(request: TraceRequest, config: EngineConfig, replica_fill_count: int) -> int:
    return (
        config.read_base_latency_us
        + len(request.requested_shards) * config.read_per_shard_latency_us
        + replica_fill_count * config.replica_read_latency_us
        + len(request.requested_shards) * config.merge_per_shard_latency_us
    )


def _reason_text(reason_codes: tuple[str, ...]) -> list[str]:
    mapping = {
        "NODE_UNAVAILABLE": "节点不可用",
        "DISK_FAULT": "磁盘故障",
        "DIGEST_MISMATCH": "摘要不一致",
        "SHARD_MISSING": "分片缺失",
    }
    return [mapping.get(code, code) for code in reason_codes]


def _replica_filled_shards(obj: ObjectLayout, health: ObjectHealth, requested: tuple[int, ...]) -> list[int]:
    filled = []
    for index in requested:
        shard = health.shard(index)
        if not shard.readable or not obj.shards[index].replicas:
            continue
        primary_id = obj.shards[index].replicas[0].replica_id
        if shard.selected_replica_id != primary_id:
            filled.append(index)
    return filled


def _base_decision(
    *,
    request: TraceRequest,
    obj: ObjectLayout,
    health: ObjectHealth,
    config: EngineConfig,
    replica_fill: list[int],
) -> dict[str, Any]:
    unreadable = [
        {
            "shard_index": index,
            "reason_codes": list(health.shard(index).reason_codes),
            "reason_text": _reason_text(health.shard(index).reason_codes),
        }
        for index in request.requested_shards
        if not health.shard(index).readable
    ]
    return {
        "request_id": request.request_id,
        "seq": request.seq,
        "arrival_ms": request.arrival_ms,
        "object_id": obj.object_id,
        "priority": request.priority,
        "tolerable_latency_us": request.tolerable_latency_us,
        "requested_bitmap": bitmap_to_hex(request.shard_bitmap),
        "readable_bitmap": bitmap_to_hex(health.readable_bitmap),
        "requested_shards": list(request.requested_shards),
        "readable_shard_count": health.readable_count,
        "k": obj.k,
        "m": obj.m,
        "unreadable_requested_shards": unreadable,
        "replica_filled_shards": replica_fill,
        "shard_health": [shard.to_dict() for shard in health.shards],
        "strategy": None,
        "outcome": None,
        "error": None,
        "estimated_latency_us": None,
        "rebuilt_shards": [],
        "reused_reconstructed_shards": [],
        "basis": {},
    }


def _fail_decision(decision: dict[str, Any], code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
    decision["strategy"] = "DIRECT_FAIL"
    decision["outcome"] = "FAILED"
    decision["error"] = {"code": code, "message": message, "details": details or {}}


def _schedule(tasks: list[TaskSpec], concurrency: int) -> tuple[int, dict[str, tuple[int, int, int]]]:
    schedule: dict[str, tuple[int, int, int]] = {}
    wave_start = 0
    makespan = 0
    for offset in range(0, len(tasks), concurrency):
        wave_tasks = tasks[offset : offset + concurrency]
        wave_duration = max((task.duration_us for task in wave_tasks), default=0)
        for task in wave_tasks:
            schedule[task.task_id] = (offset // concurrency, wave_start, wave_start + task.duration_us)
        wave_start += wave_duration
        makespan = wave_start
    return makespan, schedule


def plan_read_repair(
    *,
    layout: Layout,
    trace: Trace,
    reusable_lookup: Callable[[str, int, str], ReusableShard | None],
) -> PlanningResult:
    config = trace.options
    health_cache: dict[str, ObjectHealth] = {}
    analyses: list[RequestAnalysis] = []
    for request in trace.requests:
        obj = layout.objects[request.object_id]
        health = health_cache.get(obj.object_id)
        if health is None:
            health = evaluate_object_health(layout, obj)
            health_cache[obj.object_id] = health
        replica_fill = _replica_filled_shards(obj, health, request.requested_shards)
        decision = _base_decision(request=request, obj=obj, health=health, config=config, replica_fill=replica_fill)
        missing = tuple(index for index in request.requested_shards if not health.shard(index).readable)
        overhead = _read_overhead(request, config, len(replica_fill))
        if not missing:
            strategy = "REPLICA_FILL" if replica_fill else "DIRECT_READ"
            decision["strategy"] = strategy
            decision["outcome"] = "SUCCESS"
            decision["estimated_latency_us"] = overhead
            decision["basis"] = {
                "rule": "REPLICA_FILL_SUFFICIENT" if replica_fill else "ALL_REQUESTED_SHARDS_READABLE",
                "priority": request.priority,
                "arrival_ms": request.arrival_ms,
                "readable_shards": health.readable_count,
                "k": obj.k,
                "replica_fill_shards": replica_fill,
                "estimated_latency_us": overhead,
                "tolerable_latency_us": request.tolerable_latency_us,
                "rebuild_bytes": 0,
                "explanation": "其余完整副本足以补齐，禁止触发纠删码重建。" if replica_fill else "请求分片均可直接读取。",
            }
            analyses.append(RequestAnalysis(request=request, obj=obj, health=health, decision=decision))
            continue
        if health.readable_count < obj.k:
            decision["estimated_latency_us"] = overhead
            _fail_decision(
                decision,
                "E_UNRECOVERABLE",
                "可读分片少于 k，请求不可恢复",
                details={"readable_shards": health.readable_count, "k": obj.k, "request_id": request.request_id},
            )
            decision["basis"] = {
                "rule": "READABLE_SHARDS_BELOW_K",
                "priority": request.priority,
                "readable_shards": health.readable_count,
                "k": obj.k,
                "missing_requested_shards": list(missing),
                "explanation": "存活分片不足以形成 k 个独立方程，不尝试非法重建。",
            }
            analyses.append(RequestAnalysis(request=request, obj=obj, health=health, decision=decision))
            continue
        analyses.append(
            RequestAnalysis(
                request=request,
                obj=obj,
                health=health,
                decision=decision,
                rebuild_candidate=True,
                missing_requested=missing,
            )
        )

    candidates = [analysis for analysis in analyses if analysis.rebuild_candidate]
    ordered_candidates = sorted(
        candidates,
        key=lambda item: (-item.request.priority, item.request.arrival_ms, item.request.seq, item.request.request_id),
    )
    priority_rank = {item.request.request_id: rank + 1 for rank, item in enumerate(ordered_candidates)}

    still_candidates: list[RequestAnalysis] = []
    for analysis in ordered_candidates:
        request = analysis.request
        obj = analysis.obj
        overhead = _read_overhead(request, config, len(analysis.decision["replica_filled_shards"]))
        min_rebuild_duration = _task_duration(obj.shard_size_bytes, config)
        minimum_latency = overhead + min_rebuild_duration
        analysis.decision["estimated_latency_us"] = minimum_latency
        if minimum_latency > request.tolerable_latency_us:
            _fail_decision(
                analysis.decision,
                "E_LATENCY_GUARD",
                "即使立即开始重建也会超出可容忍读延迟，禁止选择重建策略",
                details={
                    "estimated_latency_us": minimum_latency,
                    "tolerable_latency_us": request.tolerable_latency_us,
                },
            )
            analysis.decision["basis"] = {
                "rule": "LATENCY_GUARD_MINIMUM_REBUILD",
                "priority": request.priority,
                "priority_rank": priority_rank[request.request_id],
                "estimated_latency_us": minimum_latency,
                "tolerable_latency_us": request.tolerable_latency_us,
                "rebuild_bytes": 0,
                "explanation": "重建下界已超过读延迟预算，快速失败。",
            }
        else:
            still_candidates.append(analysis)

    selected_tasks: list[TaskSpec] = []
    tasks_by_key: dict[tuple[str, int], TaskSpec] = {}
    selected_request_ids: set[str] = set()
    total_bytes = 0
    capacity = config.rebuild_concurrency + config.rebuild_queue_capacity
    queue_full_exhausted = False
    budget_exhausted = False
    time_exhausted = False

    for analysis in still_candidates:
        request = analysis.request
        obj = analysis.obj
        overhead = _read_overhead(request, config, len(analysis.decision["replica_filled_shards"]))
        known_tasks: list[TaskSpec] = []
        new_tasks: list[TaskSpec] = []
        persisted_reuse: list[dict[str, Any]] = []
        for shard_index in analysis.missing_requested:
            key = (obj.object_id, shard_index)
            existing = tasks_by_key.get(key)
            if existing is not None:
                known_tasks.append(existing)
                continue
            expected_digest = obj.shard(shard_index).digest
            reusable = reusable_lookup(obj.object_id, shard_index, expected_digest)
            if reusable is not None:
                persisted_reuse.append(
                    {
                        "shard_index": shard_index,
                        "source_task_id": reusable.task_id,
                        "source_round_id": reusable.round_id,
                        "digest": reusable.digest,
                    }
                )
                continue
            task = TaskSpec(
                task_id=_task_id(layout.layout_version, obj.object_id, shard_index, expected_digest),
                request_id=request.request_id,
                object_id=obj.object_id,
                shard_index=shard_index,
                expected_digest=expected_digest,
                bytes=obj.shard(shard_index).size_bytes,
                priority=request.priority,
                arrival_ms=request.arrival_ms,
                duration_us=_task_duration(obj.shard(shard_index).size_bytes, config),
            )
            task.request_ids.add(request.request_id)
            new_tasks.append(task)

        new_bytes = sum(task.bytes for task in new_tasks)
        remaining_before = config.round_byte_budget - total_bytes
        priority_basis = {
            "priority": request.priority,
            "priority_rank": priority_rank[request.request_id],
            "arrival_ms": request.arrival_ms,
            "missing_requested_shards": list(analysis.missing_requested),
            "replica_filled_shards": analysis.decision["replica_filled_shards"],
            "rebuild_bytes": new_bytes,
            "remaining_byte_budget_before": remaining_before,
            "reused_reconstructed_shards": [item["shard_index"] for item in persisted_reuse],
        }

        if new_tasks and queue_full_exhausted:
            _fail_decision(
                analysis.decision,
                "E_REBUILD_QUEUE_FULL",
                "有界重建等待队列已满，任务未入队",
                details={"capacity": capacity, "concurrency": config.rebuild_concurrency, "queue_capacity": config.rebuild_queue_capacity},
            )
            analysis.decision["basis"] = {
                "rule": "REBUILD_QUEUE_FULL",
                **priority_basis,
                "explanation": "更高优先级任务已占满并发槽与等待队列。",
            }
            continue
        if new_tasks and len(selected_tasks) + len(new_tasks) > capacity:
            queue_full_exhausted = True
            _fail_decision(
                analysis.decision,
                "E_REBUILD_QUEUE_FULL",
                "有界重建等待队列已满，任务未入队",
                details={"capacity": capacity, "selected_tasks": len(selected_tasks), "new_tasks": len(new_tasks)},
            )
            analysis.decision["basis"] = {
                "rule": "REBUILD_QUEUE_FULL",
                **priority_basis,
                "explanation": "超出全局并发上限的任务需进入有界等待队列，但队列容量不足。",
            }
            continue
        if new_tasks and budget_exhausted:
            _fail_decision(
                analysis.decision,
                "E_REBUILD_SHED",
                "单轮重建字节预算已耗尽，请求降级快速失败",
                details={"remaining_bytes": remaining_before, "required_bytes": new_bytes},
            )
            analysis.decision["basis"] = {
                "rule": "BYTE_BUDGET_SHED",
                **priority_basis,
                "explanation": "按优先级顺序降级：更高优先级请求已消耗完单轮字节预算。",
            }
            continue
        if new_tasks and total_bytes + new_bytes > config.round_byte_budget:
            budget_exhausted = True
            _fail_decision(
                analysis.decision,
                "E_REBUILD_SHED",
                "单轮重建字节预算不足，请求降级快速失败",
                details={"remaining_bytes": remaining_before, "required_bytes": new_bytes},
            )
            analysis.decision["basis"] = {
                "rule": "BYTE_BUDGET_SHED",
                **priority_basis,
                "explanation": "所需重建字节超过本轮剩余预算，按优先级确定性降级。",
            }
            continue
        if new_tasks and time_exhausted:
            _fail_decision(
                analysis.decision,
                "E_REBUILD_SHED",
                "单轮时间预算已耗尽，请求降级快速失败",
                details={"time_budget_us": config.round_time_budget_us},
            )
            analysis.decision["basis"] = {
                "rule": "TIME_BUDGET_SHED",
                **priority_basis,
                "explanation": "更高优先级任务已耗尽本轮虚拟重建时间预算。",
            }
            continue

        tentative_tasks = selected_tasks + new_tasks
        makespan, schedule = _schedule(tentative_tasks, config.rebuild_concurrency)
        dependency_ends = [schedule[task.task_id][2] for task in known_tasks + new_tasks]
        dependency_end = max(dependency_ends, default=0)
        estimated_latency = overhead + dependency_end
        if estimated_latency > request.tolerable_latency_us:
            _fail_decision(
                analysis.decision,
                "E_LATENCY_GUARD",
                "含排队时间后的端到端估计延迟超出可容忍读延迟，禁止选择重建策略",
                details={
                    "estimated_latency_us": estimated_latency,
                    "tolerable_latency_us": request.tolerable_latency_us,
                    "dependency_end_us": dependency_end,
                },
            )
            analysis.decision["estimated_latency_us"] = estimated_latency
            analysis.decision["basis"] = {
                "rule": "LATENCY_GUARD_SCHEDULED",
                **priority_basis,
                "estimated_latency_us": estimated_latency,
                "tolerable_latency_us": request.tolerable_latency_us,
                "explanation": "加入当前并行队列后会破坏读延迟上限，降级快速失败。",
            }
            continue
        if new_tasks and makespan > config.round_time_budget_us:
            time_exhausted = True
            _fail_decision(
                analysis.decision,
                "E_REBUILD_SHED",
                "单轮时间预算不足，请求降级快速失败",
                details={"estimated_makespan_us": makespan, "time_budget_us": config.round_time_budget_us},
            )
            analysis.decision["estimated_latency_us"] = estimated_latency
            analysis.decision["basis"] = {
                "rule": "TIME_BUDGET_SHED",
                **priority_basis,
                "estimated_makespan_us": makespan,
                "time_budget_us": config.round_time_budget_us,
                "explanation": "并入该任务会使本轮虚拟重建时长超出时间预算。",
            }
            continue

        selected_tasks = tentative_tasks
        for task in new_tasks:
            tasks_by_key[task.key] = task
        total_bytes += new_bytes
        selected_request_ids.add(request.request_id)
        for task in known_tasks + new_tasks:
            task.request_ids.add(request.request_id)
        _, current_schedule = _schedule(selected_tasks, config.rebuild_concurrency)
        for task in selected_tasks:
            wave, start, end = current_schedule[task.task_id]
            task.wave = wave
            task.estimated_start_us = start
            task.estimated_end_us = end
        request_ends = [current_schedule[task.task_id][2] for task in known_tasks + new_tasks]
        request_end = max(request_ends, default=0)
        estimated_latency = overhead + request_end
        analysis.decision["strategy"] = "EC_REBUILD"
        analysis.decision["outcome"] = "PENDING"
        analysis.decision["estimated_latency_us"] = estimated_latency
        analysis.decision["rebuilt_shards"] = list(analysis.missing_requested)
        analysis.decision["reused_reconstructed_shards"] = [
            item["shard_index"] for item in persisted_reuse
        ]
        analysis.decision["basis"] = {
            "rule": "EC_REBUILD_ADMITTED",
            **priority_basis,
            "known_task_ids": [task.task_id for task in known_tasks],
            "new_task_ids": [task.task_id for task in new_tasks],
            "persisted_reuse": persisted_reuse,
            "scheduled_end_us": request_end,
            "estimated_latency_us": estimated_latency,
            "tolerable_latency_us": request.tolerable_latency_us,
            "byte_budget_after": config.round_byte_budget - total_bytes,
            "estimated_makespan_us": makespan,
            "explanation": "冗余副本不足；在并发、字节与时间预算内选择纠删码重建。",
        }

    makespan, schedule = _schedule(selected_tasks, config.rebuild_concurrency)
    for task in selected_tasks:
        wave, start, end = schedule[task.task_id]
        task.wave = wave
        task.estimated_start_us = start
        task.estimated_end_us = end
    task_latency_by_request: dict[str, int] = {}
    for analysis in analyses:
        if analysis.request.request_id not in selected_request_ids:
            continue
        request = analysis.request
        dependency_tasks = [
            task
            for task in selected_tasks
            if task.object_id == analysis.obj.object_id and task.shard_index in analysis.missing_requested
        ]
        request_end = max((task.estimated_end_us for task in dependency_tasks), default=0)
        overhead = _read_overhead(request, config, len(analysis.decision["replica_filled_shards"]))
        task_latency_by_request[request.request_id] = overhead + request_end
        analysis.decision["estimated_latency_us"] = overhead + request_end

    decisions = [analysis.decision for analysis in analyses]
    decisions.sort(key=lambda item: (item["seq"], item["request_id"]))
    return PlanningResult(
        decisions=decisions,
        tasks=selected_tasks,
        task_latency_by_request=task_latency_by_request,
        estimated_makespan_us=makespan,
        consumed_bytes=total_bytes,
    )


def canonical_decision_digest(layout_version: str, decisions: list[dict[str, Any]]) -> str:
    canonical = {
        "layout_version": layout_version,
        "decisions": decisions,
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()
