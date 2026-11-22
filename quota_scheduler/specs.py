"""Deterministic JSON sample loading and normalization."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import (
    E_MODEL_UNKNOWN,
    E_NODE_UNKNOWN,
    E_QUOTA_GROUP_UNKNOWN,
    E_VALIDATION,
    SchedulerError,
    spec_error,
)
from .units import checked_product, parse_memory_bytes, parse_millis


@dataclass(frozen=True)
class QuotaGroupSpec:
    group_id: str
    memory_limit_bytes: int
    compute_limit_millis: int


@dataclass(frozen=True)
class NodeSpec:
    node_id: str
    total_memory_bytes: int
    available_compute_millis: int
    occupied_memory_bytes: int
    occupied_compute_millis: int
    quota_group: str
    healthy: bool


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    max_batch_samples: int
    unit_sample_cost_millis: int


@dataclass(frozen=True)
class RequestSpec:
    request_id: str
    service_name: str
    model_id: str
    memory_bytes: int
    samples_per_batch: int
    expected_latency_millis: int
    priority: int
    quota_group: str
    node_id: str | None = None
    compute_required_millis: int | None = None
    submission_hint: int = 0
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class EngineConfig:
    batch_strategy_threshold: int = 3
    batch_strategy: str = "auto"
    global_max_parallel_batches: int = 2
    round_time_budget_millis: int = 1_000
    execution_queue_capacity: int = 8
    preemption_enabled: bool = True
    batch_sample_target: int = 0
    strict_batching: bool = False

    def merged(self, overrides: Mapping[str, Any] | None) -> "EngineConfig":
        values = self.to_dict()
        if overrides:
            values.update({k: v for k, v in overrides.items() if v is not None})
        return EngineConfig.from_mapping(values)

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_strategy_threshold": self.batch_strategy_threshold,
            "batch_strategy": self.batch_strategy,
            "global_max_parallel_batches": self.global_max_parallel_batches,
            "round_time_budget_millis": self.round_time_budget_millis,
            "execution_queue_capacity": self.execution_queue_capacity,
            "preemption_enabled": self.preemption_enabled,
            "batch_sample_target": self.batch_sample_target,
            "strict_batching": self.strict_batching,
        }

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "EngineConfig":
        data = data or {}

        def positive_int(key: str, default: int, *, allow_zero: bool = False) -> int:
            value = data.get(key, default)
            if isinstance(value, bool) or not isinstance(value, int):
                raise spec_error(f"config.{key}", "value must be an integer")
            if value < 0 or (value == 0 and not allow_zero):
                raise spec_error(
                    f"config.{key}", "value must be positive" if not allow_zero else "must be >= 0"
                )
            return value

        strategy = str(data.get("batch_strategy", "auto")).strip().lower()
        if strategy not in {"auto", "greedy", "submission", "memory", "memory_descending"}:
            raise spec_error(
                "config.batch_strategy",
                "strategy must be auto, greedy/submission, or memory/memory_descending",
            )
        if strategy == "submission":
            strategy = "greedy"
        if strategy == "memory":
            strategy = "memory_descending"
        preemption = data.get("preemption_enabled", True)
        if not isinstance(preemption, bool):
            raise spec_error("config.preemption_enabled", "value must be boolean")
        strict = data.get("strict_batching", False)
        if not isinstance(strict, bool):
            raise spec_error("config.strict_batching", "value must be boolean")
        budget = data.get(
            "round_time_budget_millis",
            data.get("round_time_budget", 1_000),
        )
        return cls(
            batch_strategy_threshold=positive_int(
                "batch_strategy_threshold", 3, allow_zero=True
            ),
            batch_strategy=strategy,
            global_max_parallel_batches=positive_int(
                "global_max_parallel_batches", 2
            ),
            round_time_budget_millis=parse_millis(
                budget, "config.round_time_budget_millis"
            ),
            execution_queue_capacity=positive_int(
                "execution_queue_capacity", 8, allow_zero=True
            ),
            preemption_enabled=preemption,
            batch_sample_target=positive_int(
                "batch_sample_target", 0, allow_zero=True
            ),
            strict_batching=strict,
        )


@dataclass(frozen=True)
class SampleBundle:
    quota_groups: tuple[QuotaGroupSpec, ...]
    nodes: tuple[NodeSpec, ...]
    models: tuple[ModelSpec, ...]
    requests: tuple[RequestSpec, ...]
    config: EngineConfig

    def group_map(self) -> dict[str, QuotaGroupSpec]:
        return {item.group_id: item for item in self.quota_groups}

    def node_map(self) -> dict[str, NodeSpec]:
        return {item.node_id: item for item in self.nodes}

    def model_map(self) -> dict[str, ModelSpec]:
        return {item.model_id: item for item in self.models}


def _read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise spec_error(label, f"sample file does not exist: {path}") from None
    except json.JSONDecodeError as exc:
        raise spec_error(
            label,
            f"invalid JSON in {path.name}",
            line=exc.lineno,
            column=exc.colno,
        ) from None
    except OSError as exc:
        raise spec_error(label, f"cannot read {path}: {exc}") from None


def _items(data: Any, keys: Iterable[str], field_name: str) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if value is not None:
                if not isinstance(value, list):
                    raise spec_error(field_name, f"{key} must be a JSON array")
                return value
        raise spec_error(field_name, f"expected array field, one of {list(keys)}")
    raise spec_error(field_name, "expected a JSON object or array")


def _pick(
    item: Mapping[str, Any],
    aliases: Iterable[str],
    field_name: str,
    *,
    required: bool = True,
    default: Any = None,
) -> Any:
    for key in aliases:
        if key in item and item[key] is not None:
            return item[key]
    if required:
        raise spec_error(field_name, f"missing required field {aliases[0]!r}")
    return default


def _string(value: Any, field_name: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise spec_error(field_name, "value must be a string", value=value)
    result = value.strip()
    if not result and not allow_empty:
        raise spec_error(field_name, "value must not be empty")
    return result


def _positive_int(value: Any, field_name: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise spec_error(field_name, "value must be an integer", value=value)
    if value < 0 or (value == 0 and not allow_zero):
        raise spec_error(
            field_name,
            "value must be >= 0" if allow_zero else "value must be positive",
            value=value,
        )
    return value


def _parse_healthy(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"healthy", "ready", "ok", "true", "1", "up"}:
            return True
        if normalized in {"unhealthy", "not_ready", "notready", "false", "0", "down", "degraded"}:
            return False
    raise spec_error(
        field_name,
        "health must be a boolean or a recognised health status",
        value=value,
    )


def parse_quota_groups(data: Any) -> tuple[QuotaGroupSpec, ...]:
    raw_items = _items(data, ("quota_groups", "groups"), "quota_groups")
    result: list[QuotaGroupSpec] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_items):
        prefix = f"quota_groups[{index}]"
        if not isinstance(item, dict):
            raise spec_error(prefix, "quota group must be an object")
        group_id = _string(
            _pick(item, ("group_id", "id", "name"), f"{prefix}.group_id"),
            f"{prefix}.group_id",
        )
        if group_id in seen:
            raise spec_error(f"{prefix}.group_id", "duplicate quota group id", value=group_id)
        seen.add(group_id)
        memory = parse_memory_bytes(
            _pick(
                item,
                ("memory_limit", "memory_limit_bytes", "memory"),
                f"{prefix}.memory_limit",
            ),
            f"{prefix}.memory_limit",
        )
        compute = parse_millis(
            _pick(
                item,
                ("compute_limit", "compute_limit_millis", "compute"),
                f"{prefix}.compute_limit",
            ),
            f"{prefix}.compute_limit",
        )
        result.append(QuotaGroupSpec(group_id, memory, compute))
    return tuple(result)


def parse_nodes(
    data: Any, known_groups: set[str] | None = None
) -> tuple[NodeSpec, ...]:
    raw_items = _items(data, ("nodes", "gpu_nodes"), "nodes")
    result: list[NodeSpec] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_items):
        prefix = f"nodes[{index}]"
        if not isinstance(item, dict):
            raise spec_error(prefix, "node must be an object")
        node_id = _string(
            _pick(item, ("node_id", "id", "name"), f"{prefix}.node_id"),
            f"{prefix}.node_id",
        )
        if node_id in seen:
            raise spec_error(f"{prefix}.node_id", "duplicate node id", value=node_id)
        seen.add(node_id)
        total_memory = parse_memory_bytes(
            _pick(
                item,
                ("total_memory", "total_memory_bytes", "memory_total"),
                f"{prefix}.total_memory",
            ),
            f"{prefix}.total_memory",
        )
        available_compute = parse_millis(
            _pick(
                item,
                (
                    "available_compute",
                    "available_compute_millis",
                    "compute_capacity",
                    "compute_share",
                ),
                f"{prefix}.available_compute",
            ),
            f"{prefix}.available_compute",
        )
        occupied_memory = parse_memory_bytes(
            _pick(
                item,
                ("occupied_memory", "occupied_memory_bytes", "used_memory"),
                f"{prefix}.occupied_memory",
                required=False,
                default=0,
            ),
            f"{prefix}.occupied_memory",
        )
        occupied_compute = parse_millis(
            _pick(
                item,
                ("occupied_compute", "occupied_compute_millis", "used_compute"),
                f"{prefix}.occupied_compute",
                required=False,
                default=0,
            ),
            f"{prefix}.occupied_compute",
        )
        if occupied_memory > total_memory:
            raise spec_error(
                f"{prefix}.occupied_memory",
                "occupied memory exceeds total memory",
                occupied_memory_bytes=occupied_memory,
                total_memory_bytes=total_memory,
            )
        if occupied_compute > available_compute:
            raise spec_error(
                f"{prefix}.occupied_compute",
                "occupied compute exceeds available compute",
                occupied_compute_millis=occupied_compute,
                available_compute_millis=available_compute,
            )
        group = _string(
            _pick(item, ("quota_group", "quota_group_id", "group"), f"{prefix}.quota_group"),
            f"{prefix}.quota_group",
        )
        if known_groups is not None and group not in known_groups:
            raise SchedulerError(
                E_QUOTA_GROUP_UNKNOWN,
                f"node {node_id} references unknown quota group {group!r}",
                field=f"{prefix}.quota_group",
                details={"quota_group": group},
            )
        health_value = _pick(
            item,
            ("healthy", "health", "health_status", "status"),
            f"{prefix}.healthy",
            required=False,
            default=True,
        )
        healthy = _parse_healthy(health_value, f"{prefix}.healthy")
        result.append(
            NodeSpec(
                node_id=node_id,
                total_memory_bytes=total_memory,
                available_compute_millis=available_compute,
                occupied_memory_bytes=occupied_memory,
                occupied_compute_millis=occupied_compute,
                quota_group=group,
                healthy=healthy,
            )
        )
    return tuple(result)


def parse_models(data: Any) -> tuple[ModelSpec, ...]:
    raw_items = _items(data, ("models",), "models")
    result: list[ModelSpec] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_items):
        prefix = f"models[{index}]"
        if not isinstance(item, dict):
            raise spec_error(prefix, "model must be an object")
        model_id = _string(
            _pick(item, ("model_id", "id", "name"), f"{prefix}.model_id"),
            f"{prefix}.model_id",
        )
        if model_id in seen:
            raise spec_error(f"{prefix}.model_id", "duplicate model id", value=model_id)
        seen.add(model_id)
        max_samples = _positive_int(
            _pick(
                item,
                ("max_batch_samples", "max_samples", "batch_limit"),
                f"{prefix}.max_batch_samples",
            ),
            f"{prefix}.max_batch_samples",
        )
        unit_cost = parse_millis(
            _pick(
                item,
                (
                    "unit_sample_cost",
                    "unit_sample_cost_millis",
                    "sample_cost",
                    "unit_cost",
                ),
                f"{prefix}.unit_sample_cost",
            ),
            f"{prefix}.unit_sample_cost",
        )
        if unit_cost <= 0:
            raise spec_error(
                f"{prefix}.unit_sample_cost", "unit sample cost must be positive"
            )
        result.append(ModelSpec(model_id, max_samples, unit_cost))
    return tuple(result)


def parse_request(
    item: Any,
    index: int = 0,
    *,
    known_groups: set[str] | None = None,
    known_models: set[str] | None = None,
    known_nodes: set[str] | None = None,
) -> RequestSpec:
    prefix = f"requests[{index}]"
    if not isinstance(item, dict):
        raise spec_error(prefix, "request must be an object")
    request_id = _string(
        _pick(item, ("request_id", "id"), f"{prefix}.request_id"),
        f"{prefix}.request_id",
    )
    service_name = _string(
        _pick(item, ("service_name", "service"), f"{prefix}.service_name"),
        f"{prefix}.service_name",
    )
    model_id = _string(
        _pick(item, ("model_id", "model"), f"{prefix}.model_id"),
        f"{prefix}.model_id",
    )
    if known_models is not None and model_id not in known_models:
        raise SchedulerError(
            E_MODEL_UNKNOWN,
            f"request {request_id} references unknown model {model_id!r}",
            field=f"{prefix}.model_id",
            details={"model_id": model_id},
        )
    memory = parse_memory_bytes(
        _pick(
            item,
            ("memory", "memory_demand", "memory_bytes"),
            f"{prefix}.memory",
        ),
        f"{prefix}.memory",
    )
    samples = _positive_int(
        _pick(
            item,
            ("samples_per_batch", "batch_samples", "samples"),
            f"{prefix}.samples_per_batch",
        ),
        f"{prefix}.samples_per_batch",
    )
    latency = parse_millis(
        _pick(
            item,
            ("expected_latency", "expected_latency_millis", "latency_limit"),
            f"{prefix}.expected_latency",
        ),
        f"{prefix}.expected_latency",
    )
    if latency <= 0:
        raise spec_error(
            f"{prefix}.expected_latency", "expected latency must be positive"
        )
    priority = _positive_int(
        _pick(item, ("priority",), f"{prefix}.priority"),
        f"{prefix}.priority",
        allow_zero=True,
    )
    group = _string(
        _pick(item, ("quota_group", "quota_group_id", "group"), f"{prefix}.quota_group"),
        f"{prefix}.quota_group",
    )
    if known_groups is not None and group not in known_groups:
        raise SchedulerError(
            E_QUOTA_GROUP_UNKNOWN,
            f"request {request_id} references unknown quota group {group!r}",
            field=f"{prefix}.quota_group",
            details={"quota_group": group},
        )
    node_id_value = _pick(
        item,
        ("node_id", "gpu_node", "node"),
        f"{prefix}.node_id",
        required=False,
        default=None,
    )
    node_id = None if node_id_value is None else _string(
        node_id_value, f"{prefix}.node_id"
    )
    if known_nodes is not None and node_id is not None and node_id not in known_nodes:
        raise SchedulerError(
            E_NODE_UNKNOWN,
            f"request {request_id} references unknown node {node_id!r}",
            field=f"{prefix}.node_id",
            details={"node_id": node_id},
        )
    compute_value = _pick(
        item,
        ("compute_required", "compute_required_millis"),
        f"{prefix}.compute_required",
        required=False,
        default=None,
    )
    compute_required = (
        None
        if compute_value is None
        else parse_millis(compute_value, f"{prefix}.compute_required")
    )
    if compute_required is not None and compute_required <= 0:
        raise spec_error(
            f"{prefix}.compute_required", "compute requirement must be positive"
        )
    hint_value = item.get("submission_seq", item.get("submission_index", index))
    submission_hint = _positive_int(
        hint_value, f"{prefix}.submission_seq", allow_zero=True
    )
    return RequestSpec(
        request_id=request_id,
        service_name=service_name,
        model_id=model_id,
        node_id=node_id,
        memory_bytes=memory,
        samples_per_batch=samples,
        expected_latency_millis=latency,
        priority=priority,
        quota_group=group,
        compute_required_millis=compute_required,
        submission_hint=submission_hint,
        raw=dict(item),
    )


def parse_requests(
    data: Any,
    *,
    known_groups: set[str],
    known_models: set[str],
    known_nodes: set[str],
) -> tuple[RequestSpec, ...]:
    raw_items = _items(data, ("requests", "trace"), "requests")
    return tuple(
        parse_request(
            item,
            index,
            known_groups=known_groups,
            known_models=known_models,
            known_nodes=known_nodes,
        )
        for index, item in enumerate(raw_items)
    )


def parse_config(data: Any) -> EngineConfig:
    if data is None:
        return EngineConfig()
    if not isinstance(data, dict):
        raise spec_error("config", "config must be a JSON object")
    # Allow either a direct object or {"scheduler": {...}}.
    if "scheduler" in data and isinstance(data["scheduler"], dict):
        merged = dict(data)
        merged.update(data["scheduler"])
        merged.pop("scheduler", None)
        data = merged
    return EngineConfig.from_mapping(data)


def _find_file(directory: Path, names: Iterable[str]) -> Path | None:
    for name in names:
        candidate = directory / name
        if candidate.is_file():
            return candidate
    return None


def load_bundle(directory: str | Path) -> SampleBundle:
    """Load and cross-validate all deterministic local samples."""

    base = Path(directory)
    if not base.is_dir():
        raise spec_error("samples", f"sample directory does not exist: {base}")
    group_path = _find_file(base, ("quota_groups.json", "quota-groups.json", "groups.json"))
    node_path = _find_file(base, ("nodes.json", "gpu_nodes.json"))
    model_path = _find_file(base, ("models.json",))
    request_path = _find_file(base, ("requests.json", "request_trace.json", "trace.json"))
    config_path = _find_file(base, ("config.json", "scheduler.json"))
    if group_path is None:
        raise spec_error("quota_groups", "quota group sample file is missing")
    if node_path is None:
        raise spec_error("nodes", "node sample file is missing")
    if model_path is None:
        raise spec_error("models", "model sample file is missing")

    groups = parse_quota_groups(_read_json(group_path, "quota_groups"))
    group_ids = {group.group_id for group in groups}
    nodes = parse_nodes(_read_json(node_path, "nodes"), group_ids)
    models = parse_models(_read_json(model_path, "models"))
    config = (
        parse_config(_read_json(config_path, "config"))
        if config_path is not None
        else EngineConfig()
    )
    model_ids = {model.model_id for model in models}
    node_ids = {node.node_id for node in nodes}
    requests = (
        parse_requests(
            _read_json(request_path, "requests"),
            known_groups=group_ids,
            known_models=model_ids,
            known_nodes=node_ids,
        )
        if request_path is not None
        else tuple()
    )
    _validate_bundle(groups, nodes, models)
    return SampleBundle(groups, nodes, models, requests, config)


def _validate_bundle(
    groups: tuple[QuotaGroupSpec, ...],
    nodes: tuple[NodeSpec, ...],
    models: tuple[ModelSpec, ...],
) -> None:
    if not groups:
        raise spec_error("quota_groups", "at least one quota group is required")
    if not nodes:
        raise spec_error("nodes", "at least one node is required")
    if not models:
        raise spec_error("models", "at least one model is required")
    group_map = {item.group_id: item for item in groups}
    base_memory: dict[str, int] = {key: 0 for key in group_map}
    base_compute: dict[str, int] = {key: 0 for key in group_map}
    for node in nodes:
        base_memory[node.quota_group] += node.occupied_memory_bytes
        base_compute[node.quota_group] += node.occupied_compute_millis
    for group in groups:
        if base_memory[group.group_id] > group.memory_limit_bytes:
            raise spec_error(
                f"quota_groups[{group.group_id}].memory_limit",
                "member node occupied memory already exceeds quota group limit",
                occupied_memory_bytes=base_memory[group.group_id],
                memory_limit_bytes=group.memory_limit_bytes,
            )
        if base_compute[group.group_id] > group.compute_limit_millis:
            raise spec_error(
                f"quota_groups[{group.group_id}].compute_limit",
                "member node occupied compute already exceeds quota group limit",
                occupied_compute_millis=base_compute[group.group_id],
                compute_limit_millis=group.compute_limit_millis,
            )


def request_compute_requirement(spec: RequestSpec, model: ModelSpec) -> int:
    if spec.compute_required_millis is not None:
        return spec.compute_required_millis
    return checked_product(
        spec.samples_per_batch,
        model.unit_sample_cost_millis,
        f"requests[{spec.request_id}].samples_per_batch",
        "request compute requirement",
    )


def request_from_payload(
    payload: Mapping[str, Any],
    *,
    known_groups: set[str],
    known_models: set[str],
    known_nodes: set[str],
    index: int = 0,
) -> RequestSpec:
    if not isinstance(payload, Mapping):
        raise SchedulerError(E_VALIDATION, "request payload must be an object")
    return parse_request(
        payload,
        index,
        known_groups=known_groups,
        known_models=known_models,
        known_nodes=known_nodes,
    )


def spec_json(value: Any) -> Any:
    """Convert frozen spec objects to JSON-friendly dictionaries."""

    if hasattr(value, "__dataclass_fields__"):
        return {
            key: spec_json(getattr(value, key))
            for key in value.__dataclass_fields__
            if key != "raw"
        }
    if isinstance(value, tuple):
        return [spec_json(item) for item in value]
    if isinstance(value, dict):
        return {key: spec_json(item) for key, item in value.items()}
    return value

