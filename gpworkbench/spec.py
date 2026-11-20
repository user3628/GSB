"""节点 / 配额组 / 模型 / 请求规格的解析与归一化。

解析全程仅使用整数运算；任何一条规格非法都会抛出 E_SPEC_INVALID，
由调用方保证在持久化事务外失败，不写入任何状态。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .errors import E_NOT_FOUND, BusinessError, SpecValidationError
from .units import parse_compute, parse_memory, parse_non_negative_int, parse_positive_int

HEALTH_HEALTHY = "HEALTHY"
HEALTH_DRAIN = "DRAIN"
HEALTH_DOWN = "DOWN"
ALLOWED_HEALTH = frozenset((HEALTH_HEALTHY, HEALTH_DRAIN, HEALTH_DOWN))


@dataclass(frozen=True)
class QuotaGroupSpec:
    group_id: str
    memory_limit: int
    compute_limit: int


@dataclass(frozen=True)
class NodeSpec:
    node_id: str
    total_memory: int
    total_compute: int
    used_memory: int
    group_id: str
    health: str

    @property
    def free_memory(self) -> int:
        return self.total_memory - self.used_memory

    @property
    def free_compute(self) -> int:
        return self.total_compute


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    cost_per_sample: int
    max_batch_samples: int


@dataclass(frozen=True)
class RequestSpec:
    request_id: str
    service_name: str
    model_id: str
    memory_need: int
    compute_need: int
    samples_per_batch: int
    latency_limit_ms: int
    priority: int
    group_id: str
    seq: int


@dataclass(frozen=True)
class WorkloadSpec:
    groups: dict[str, QuotaGroupSpec]
    nodes: dict[str, NodeSpec]
    models: dict[str, ModelSpec]
    requests: list[RequestSpec] = field(default_factory=list)
    source_path: str | None = None


def _require_mapping(obj, where: str) -> dict:
    if not isinstance(obj, dict):
        raise SpecValidationError(f"{where} 必须是对象")
    return obj


def _require_str(record: dict, key: str, where: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SpecValidationError(f"{where}.{key} 必须是非空字符串", key, value)
    return value.strip()


def _parse_groups(raw_groups) -> dict[str, QuotaGroupSpec]:
    if not isinstance(raw_groups, list) or not raw_groups:
        raise SpecValidationError("quota_groups 必须是非空数组", "quota_groups")
    result: dict[str, QuotaGroupSpec] = {}
    for index, raw in enumerate(raw_groups):
        where = f"quota_groups[{index}]"
        record = _require_mapping(raw, where)
        group_id = _require_str(record, "group_id", where)
        if group_id in result:
            raise SpecValidationError(f"配额组 {group_id} 重复声明", "group_id", group_id)
        result[group_id] = QuotaGroupSpec(
            group_id=group_id,
            memory_limit=parse_memory(record.get("memory_limit"), f"{where}.memory_limit"),
            compute_limit=parse_compute(record.get("compute_limit"), f"{where}.compute_limit"),
        )
    return result


def _parse_nodes(raw_nodes, groups: dict[str, QuotaGroupSpec]) -> dict[str, NodeSpec]:
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise SpecValidationError("nodes 必须是非空数组", "nodes")
    result: dict[str, NodeSpec] = {}
    for index, raw in enumerate(raw_nodes):
        where = f"nodes[{index}]"
        record = _require_mapping(raw, where)
        node_id = _require_str(record, "node_id", where)
        if node_id in result:
            raise SpecValidationError(f"节点 {node_id} 重复声明", "node_id", node_id)
        group_id = _require_str(record, "group_id", where)
        if group_id not in groups:
            raise SpecValidationError(
                f"节点 {node_id} 引用了未声明的配额组 {group_id}", "group_id", group_id
            )
        health = _require_str(record, "health", where)
        if health not in ALLOWED_HEALTH:
            raise SpecValidationError(
                f"节点 {node_id} 健康状态非法: {health}", "health", health
            )
        total_memory = parse_memory(record.get("total_memory"), f"{where}.total_memory")
        if total_memory <= 0:
            raise SpecValidationError(f"节点 {node_id} 总显存必须为正", "total_memory", total_memory)
        used_memory = parse_memory(record.get("used_memory"), f"{where}.used_memory")
        total_compute = parse_compute(record.get("total_compute"), f"{where}.total_compute")
        if used_memory > total_memory:
            raise SpecValidationError(
                f"节点 {node_id} 已占用显存 {used_memory} 超过总显存 {total_memory}",
                "used_memory",
                record.get("used_memory"),
            )
        result[node_id] = NodeSpec(
            node_id=node_id,
            total_memory=total_memory,
            total_compute=total_compute,
            used_memory=used_memory,
            group_id=group_id,
            health=health,
        )
    return result


def _parse_models(raw_models) -> dict[str, ModelSpec]:
    if not isinstance(raw_models, list) or not raw_models:
        raise SpecValidationError("models 必须是非空数组", "models")
    result: dict[str, ModelSpec] = {}
    for index, raw in enumerate(raw_models):
        where = f"models[{index}]"
        record = _require_mapping(raw, where)
        model_id = _require_str(record, "model_id", where)
        if model_id in result:
            raise SpecValidationError(f"模型 {model_id} 重复声明", "model_id", model_id)
        cost = parse_non_negative_int(record.get("cost_per_sample"), f"{where}.cost_per_sample")
        max_batch = parse_positive_int(record.get("max_batch_samples"), f"{where}.max_batch_samples")
        result[model_id] = ModelSpec(
            model_id=model_id,
            cost_per_sample=cost,
            max_batch_samples=max_batch,
        )
    return result


def _parse_requests(
    raw_requests,
    groups: dict[str, QuotaGroupSpec],
    models: dict[str, ModelSpec],
) -> list[RequestSpec]:
    if not isinstance(raw_requests, list):
        raise SpecValidationError("requests 必须是数组", "requests")
    result: list[RequestSpec] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_requests):
        where = f"requests[{index}]"
        record = _require_mapping(raw, where)
        request_id = _require_str(record, "request_id", where)
        if request_id in seen:
            raise SpecValidationError(f"请求 {request_id} 在样本内重复", "request_id", request_id)
        seen.add(request_id)
        group_id = _require_str(record, "group_id", where)
        if group_id not in groups:
            raise SpecValidationError(
                f"请求 {request_id} 引用了未声明的配额组 {group_id}", "group_id", group_id
            )
        model_id = _require_str(record, "model_id", where)
        if model_id not in models:
            raise SpecValidationError(
                f"请求 {request_id} 引用了未声明的模型 {model_id}", "model_id", model_id
            )
        service_name = _require_str(record, "service_name", where)
        samples = parse_positive_int(record.get("samples_per_batch"), f"{where}.samples_per_batch")
        if samples > models[model_id].max_batch_samples:
            raise SpecValidationError(
                f"请求 {request_id} 的样本数 {samples} 超过模型 {model_id} 批上限 "
                f"{models[model_id].max_batch_samples}",
                "samples_per_batch",
                samples,
            )
        priority = parse_non_negative_int(record.get("priority"), f"{where}.priority")
        memory_need = parse_memory(record.get("memory_need"), f"{where}.memory_need")
        if memory_need <= 0:
            raise SpecValidationError(
                f"请求 {request_id} 显存需求必须为正", "memory_need", memory_need
            )
        result.append(
            RequestSpec(
                request_id=request_id,
                service_name=service_name,
                model_id=model_id,
                memory_need=memory_need,
                compute_need=parse_compute(record.get("compute_need"), f"{where}.compute_need"),
                samples_per_batch=samples,
                latency_limit_ms=parse_non_negative_int(
                    record.get("latency_limit_ms"), f"{where}.latency_limit_ms"
                ),
                priority=priority,
                group_id=group_id,
                seq=index,
            )
        )
    return result


def parse_workload(payload: dict, source_path: str | None = None) -> WorkloadSpec:
    """解析完整样本；任一错误都抛 SpecValidationError，且不触碰数据库。"""
    _require_mapping(payload, "workload")
    groups = _parse_groups(payload.get("quota_groups"))
    nodes = _parse_nodes(payload.get("nodes"), groups)
    models = _parse_models(payload.get("models"))
    requests = _parse_requests(payload.get("requests", []), groups, models)

    # 跨记录一致性检查：请求预留算力不得超出所属组上限（静态上界）
    for request in requests:
        group = groups[request.group_id]
        if request.compute_need > group.compute_limit:
            raise SpecValidationError(
                f"请求 {request.request_id} 算力需求 {request.compute_need} 超过组 "
                f"{group.group_id} 算力上限 {group.compute_limit}",
                "compute_need",
                request.compute_need,
            )
        if request.memory_need > group.memory_limit:
            raise SpecValidationError(
                f"请求 {request.request_id} 显存需求超过组 {group.group_id} 显存上限",
                "memory_need",
                request.memory_need,
            )
    return WorkloadSpec(
        groups=groups,
        nodes=nodes,
        models=models,
        requests=requests,
        source_path=source_path,
    )


def load_workload_file(path: str | Path) -> WorkloadSpec:
    file_path = Path(path)
    if not file_path.is_file():
        raise BusinessError(E_NOT_FOUND, f"样本文件不存在: {file_path}")
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SpecValidationError(f"样本 JSON 解析失败: {exc}", "file", str(file_path)) from exc
    return parse_workload(payload, source_path=str(file_path))
