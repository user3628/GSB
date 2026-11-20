from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable

from app.errors import SpecInvalidError, TraceInvalidError
from app.units import (
    MAX_I64,
    QuantityError,
    checked_add,
    checked_mul,
    format_bytes,
    parse_arrival_us,
    parse_bytes,
    parse_duration_us,
    us_to_ms_x100,
)

ALGORITHM_VERSION = "batch-fixedpoint-v1"


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    memory_bytes: int
    cost_us: int
    max_batch_samples: int

    def as_dict(self) -> dict[str, int | str]:
        return {
            "model_id": self.model_id,
            "sample_memory_bytes": self.memory_bytes,
            "sample_memory_display": format_bytes(self.memory_bytes),
            "sample_cost_us": self.cost_us,
            "max_batch_samples": self.max_batch_samples,
        }


@dataclass(frozen=True)
class RequestSpec:
    request_id: str
    seq: int
    arrival_us: int
    model_id: str
    samples: int
    expected_latency_us: int
    priority: int

    @property
    def arrival_ms_x1000(self) -> int:
        return self.arrival_us

    def as_dict(self) -> dict[str, int | str]:
        return {
            "request_id": self.request_id,
            "seq": self.seq,
            "arrival_us": self.arrival_us,
            "arrival_ms_fixed_0_001": us_to_ms_x100(self.arrival_us) * 10,
            "model_id": self.model_id,
            "samples": self.samples,
            "expected_batch_latency_us": self.expected_latency_us,
            "expected_batch_latency_ms_fixed_0_001": us_to_ms_x100(self.expected_latency_us) * 10,
            "priority": self.priority,
        }


@dataclass(frozen=True)
class MemoryRegionSpec:
    start_bytes: int
    size_bytes: int
    state: str
    label: str

    @property
    def end_bytes(self) -> int:
        return self.start_bytes + self.size_bytes

    def as_dict(self) -> dict[str, int | str]:
        return {
            "start_bytes": self.start_bytes,
            "size_bytes": self.size_bytes,
            "end_bytes": self.end_bytes,
            "state": self.state,
            "label": self.label,
        }


@dataclass(frozen=True)
class NodeSpec:
    total_memory_bytes: int
    regions: tuple[MemoryRegionSpec, ...]
    requested_block_bytes: int | None = None

    @property
    def free_bytes(self) -> int:
        return sum(region.size_bytes for region in self.regions if region.state == "free")

    @property
    def largest_free_bytes(self) -> int:
        return max((r.size_bytes for r in self.regions if r.state == "free"), default=0)

    def as_dict(self) -> dict[str, Any]:
        return {
            "total_memory_bytes": self.total_memory_bytes,
            "total_memory_display": format_bytes(self.total_memory_bytes),
            "free_bytes": self.free_bytes,
            "free_display": format_bytes(self.free_bytes),
            "largest_free_bytes": self.largest_free_bytes,
            "largest_free_display": format_bytes(self.largest_free_bytes),
            "regions": [region.as_dict() for region in self.regions],
        }


@dataclass(frozen=True)
class NormalizedTrace:
    trace_id: str
    name: str
    models: dict[str, ModelSpec]
    requests: tuple[RequestSpec, ...]
    node: NodeSpec
    source_schema: str
    payload_digest: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "name": self.name,
            "algorithm_version": ALGORITHM_VERSION,
            "source_schema": self.source_schema,
            "payload_digest": self.payload_digest,
            "request_count": len(self.requests),
            "total_samples": sum(request.samples for request in self.requests),
            "models": [model.as_dict() for model in self.models.values()],
            "node": self.node.as_dict(),
            "requests": [request.as_dict() for request in self.requests],
        }


def _first(mapping: dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return default


def _require_mapping(value: Any, field: str, errors: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append({"field": field, "message": "must be an object"})
        return {}
    return value


def _parse_positive_int(value: Any, field: str, errors: list[dict[str, Any]]) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not an integer")
        result = int(value)
        if str(value).strip() != str(result) and not isinstance(value, int):
            raise ValueError("must be an integer")
        if result <= 0:
            raise ValueError("must be positive")
        if result > MAX_I64:
            raise ValueError("overflows signed 64-bit range")
        return result
    except (TypeError, ValueError) as exc:
        errors.append({"field": field, "message": str(exc)})
        return 0


def _parse_priority(value: Any, field: str, errors: list[dict[str, Any]]) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not an integer priority")
        result = int(value)
        if str(value).strip() != str(result) and not isinstance(value, int):
            raise ValueError("must be an integer priority")
        if result < 0:
            raise ValueError("must be non-negative")
        return result
    except (TypeError, ValueError) as exc:
        errors.append({"field": field, "message": str(exc)})
        return 0


def _parse_models(raw_models: Any, errors: list[dict[str, Any]]) -> dict[str, ModelSpec]:
    if not isinstance(raw_models, list) or not raw_models:
        errors.append({"field": "models", "message": "must be a non-empty array"})
        return {}
    models: dict[str, ModelSpec] = {}
    for index, raw in enumerate(raw_models):
        field = f"models[{index}]"
        item = _require_mapping(raw, field, errors)
        model_id = item.get("model_id", item.get("id"))
        if not isinstance(model_id, str) or not model_id.strip():
            errors.append({"field": f"{field}.model_id", "message": "must be a non-empty string"})
            continue
        model_id = model_id.strip()
        if model_id in models:
            errors.append({"field": f"{field}.model_id", "message": f"duplicate model id {model_id!r}"})
            continue
        try:
            memory_bytes = parse_bytes(
                _first(item, ("sample_memory_bytes", "sample_memory", "memory_bytes")),
                f"{field}.sample_memory_bytes",
            )
        except QuantityError as exc:
            errors.append({"field": f"{field}.sample_memory_bytes", "message": str(exc)})
            memory_bytes = 0
        try:
            cost_us = parse_duration_us(
                _first(item, ("sample_cost_us", "sample_cost", "cost_us", "compute_us")),
                f"{field}.sample_cost_us",
            )
        except QuantityError as exc:
            errors.append({"field": f"{field}.sample_cost_us", "message": str(exc)})
            cost_us = 0
        max_batch_samples = _parse_positive_int(
            _first(item, ("max_batch_samples", "max_samples")),
            f"{field}.max_batch_samples",
            errors,
        )
        if memory_bytes <= 0:
            errors.append({"field": f"{field}.sample_memory_bytes", "message": "must be greater than zero"})
        if cost_us <= 0:
            errors.append({"field": f"{field}.sample_cost_us", "message": "must be greater than zero"})
        if model_id and memory_bytes > 0 and cost_us > 0 and max_batch_samples > 0:
            models[model_id] = ModelSpec(model_id, memory_bytes, cost_us, max_batch_samples)
    return models


def _parse_regions(
    raw_regions: Any,
    total_memory_bytes: int,
    errors: list[dict[str, Any]],
) -> tuple[MemoryRegionSpec, ...]:
    if raw_regions is None:
        return (MemoryRegionSpec(0, total_memory_bytes, "free", "node-free"),)
    if not isinstance(raw_regions, list) or not raw_regions:
        errors.append({"field": "node.memory_regions", "message": "must be a non-empty array"})
        return ()
    parsed: list[MemoryRegionSpec] = []
    for index, raw in enumerate(raw_regions):
        field = f"node.memory_regions[{index}]"
        item = _require_mapping(raw, field, errors)
        try:
            start = parse_bytes(_first(item, ("start_bytes", "start", "offset_bytes")), f"{field}.start_bytes")
            size = parse_bytes(_first(item, ("size_bytes", "size", "length_bytes")), f"{field}.size_bytes")
        except QuantityError as exc:
            errors.append({"field": field, "message": str(exc)})
            continue
        state = str(item.get("state", "free")).lower()
        if state not in {"free", "reserved", "occupied"}:
            errors.append({"field": f"{field}.state", "message": "must be free, reserved, or occupied"})
            continue
        if size <= 0:
            errors.append({"field": f"{field}.size_bytes", "message": "must be greater than zero"})
            continue
        if start + size > total_memory_bytes:
            errors.append({"field": field, "message": "region exceeds node total memory"})
            continue
        parsed.append(
            MemoryRegionSpec(
                start_bytes=start,
                size_bytes=size,
                state=state,
                label=str(item.get("label", f"region-{index}")),
            )
        )
    if errors:
        return ()
    parsed.sort(key=lambda region: (region.start_bytes, region.end_bytes))
    for previous, current in zip(parsed, parsed[1:]):
        if current.start_bytes < previous.end_bytes:
            errors.append(
                {
                    "field": "node.memory_regions",
                    "message": f"regions overlap: {previous.label} and {current.label}",
                }
            )
            return ()
    filled: list[MemoryRegionSpec] = []
    cursor = 0
    for region in parsed:
        if region.start_bytes > cursor:
            filled.append(MemoryRegionSpec(cursor, region.start_bytes - cursor, "reserved", "unmapped-gap"))
        filled.append(region)
        cursor = region.end_bytes
    if cursor < total_memory_bytes:
        filled.append(MemoryRegionSpec(cursor, total_memory_bytes - cursor, "reserved", "unmapped-tail"))
    return tuple(filled)


def _parse_node(raw: Any, errors: list[dict[str, Any]]) -> NodeSpec:
    node = _require_mapping(raw, "node", errors)
    try:
        total = parse_bytes(
            _first(node, ("total_memory_bytes", "total_memory", "node_memory_bytes")),
            "node.total_memory_bytes",
        )
    except QuantityError as exc:
        errors.append({"field": "node.total_memory_bytes", "message": str(exc)})
        total = 0
    if total <= 0:
        errors.append({"field": "node.total_memory_bytes", "message": "must be greater than zero"})
    block: int | None = None
    raw_block = _first(node, ("bitmap_block_bytes", "block_bytes"))
    if raw_block is not None:
        try:
            block = parse_bytes(raw_block, "node.bitmap_block_bytes")
        except QuantityError as exc:
            errors.append({"field": "node.bitmap_block_bytes", "message": str(exc)})
    raw_regions = node.get("memory_regions", node.get("regions"))
    regions = _parse_regions(raw_regions, total, errors) if total > 0 else ()
    return NodeSpec(total_memory_bytes=total, regions=regions, requested_block_bytes=block)


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_trace_payload(payload: Any) -> NormalizedTrace:
    errors: list[dict[str, Any]] = []
    root = _require_mapping(payload, "$", errors)
    raw_trace = root.get("trace", root)
    trace = _require_mapping(raw_trace, "trace", errors)
    raw_models = root.get("models")
    raw_node = root.get("node")
    if isinstance(raw_models, dict):
        raw_node = raw_node or raw_models.get("node")
        raw_models = raw_models.get("models")
    if raw_models is None:
        raw_models = root.get("model_catalog")
    if raw_node is None:
        raw_node = trace.get("node")
    models = _parse_models(raw_models, errors)
    node = _parse_node(raw_node, errors)

    trace_id = trace.get("trace_id", trace.get("id"))
    if not isinstance(trace_id, str) or not trace_id.strip():
        trace_id = "trace-" + hashlib.sha256(_canonical(trace).encode("utf-8")).hexdigest()[:16]
    else:
        trace_id = trace_id.strip()
    name = str(trace.get("name", trace_id))
    source_schema = str(root.get("schema_version", trace.get("schema_version", "1")))

    raw_requests = trace.get("requests")
    if not isinstance(raw_requests, list) or not raw_requests:
        errors.append({"field": "trace.requests", "message": "must be a non-empty array"})
        raw_requests = []

    requests: list[RequestSpec] = []
    seen_request_ids: set[str] = set()
    previous_arrival: int | None = None
    monotonic_errors: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_requests):
        field = f"trace.requests[{index}]"
        item = _require_mapping(raw, field, errors)
        request_id = item.get("request_id", item.get("id"))
        record_errors_before = len(errors)
        if not isinstance(request_id, str) or not request_id.strip():
            errors.append({"field": f"{field}.request_id", "message": "must be a non-empty string"})
            request_id = ""
        else:
            request_id = request_id.strip()
            if request_id in seen_request_ids:
                errors.append({"field": f"{field}.request_id", "message": "duplicate request id"})
            seen_request_ids.add(request_id)
        model_id = item.get("model_id")
        if not isinstance(model_id, str) or not model_id.strip():
            errors.append({"field": f"{field}.model_id", "message": "must be a non-empty string"})
            model_id = ""
        else:
            model_id = model_id.strip()
            if model_id not in models:
                errors.append(
                    {
                        "field": f"{field}.model_id",
                        "message": f"unknown model id {model_id!r}",
                        "request_id": request_id or None,
                    }
                )
        samples = _parse_positive_int(
            _first(item, ("samples", "sample_count", "batch_samples")),
            f"{field}.samples",
            errors,
        )
        priority = _parse_priority(item.get("priority"), f"{field}.priority", errors)
        if "arrival_us" in item:
            try:
                arrival_us = int(item["arrival_us"])
                if isinstance(item["arrival_us"], bool) or arrival_us < 0:
                    raise ValueError("must be a non-negative integer microsecond value")
            except (TypeError, ValueError) as exc:
                errors.append({"field": f"{field}.arrival_us", "message": str(exc)})
                arrival_us = 0
        else:
            try:
                arrival_us = parse_arrival_us(
                    _first(item, ("arrival_ms", "arrival_at_ms", "arrival")),
                    f"{field}.arrival_ms",
                )
            except QuantityError as exc:
                errors.append({"field": f"{field}.arrival_ms", "message": str(exc)})
                arrival_us = 0
        try:
            expected_us = parse_duration_us(
                _first(
                    item,
                    (
                        "expected_batch_latency_us",
                        "expected_batch_latency",
                        "expected_batch_latency_ms",
                        "expected_latency_us",
                        "latency_cap",
                    ),
                ),
                f"{field}.expected_batch_latency",
            )
        except QuantityError as exc:
            errors.append({"field": f"{field}.expected_batch_latency", "message": str(exc)})
            expected_us = 0
        if expected_us <= 0:
            errors.append(
                {"field": f"{field}.expected_batch_latency", "message": "must be greater than zero"}
            )
        model = models.get(model_id)
        if model is not None and 0 < expected_us < model.cost_us:
            errors.append(
                {
                    "field": f"{field}.expected_batch_latency",
                    "message": "latency cap cannot be smaller than one sample cost",
                    "model_id": model_id,
                    "unit_cost_us": model.cost_us,
                }
            )
        if previous_arrival is not None and arrival_us < previous_arrival:
            monotonic_errors.append(
                {
                    "field": f"{field}.arrival_ms",
                    "message": "arrival timestamp is not monotonically non-decreasing",
                    "previous_arrival_us": previous_arrival,
                    "arrival_us": arrival_us,
                }
            )
        previous_arrival = arrival_us
        if len(errors) == record_errors_before:
            requests.append(
                RequestSpec(
                    request_id=request_id,
                    seq=index,
                    arrival_us=arrival_us,
                    model_id=model_id,
                    samples=samples,
                    expected_latency_us=expected_us,
                    priority=priority,
                )
            )

    if errors:
        raise SpecInvalidError(
            "model or trace specification is invalid; the entire submission was rejected",
            details={"errors": errors, "written": False},
        )
    if monotonic_errors:
        raise TraceInvalidError(
            "trace arrival timestamps must be monotonically non-decreasing",
            details={"errors": monotonic_errors, "written": False},
        )

    canonical_payload = {
        "models": [models[key].as_dict() for key in sorted(models)],
        "trace_id": trace_id,
        "node": node.as_dict(),
        "requests": [
            {
                "request_id": request.request_id,
                "seq": request.seq,
                "arrival_us": request.arrival_us,
                "model_id": request.model_id,
                "samples": request.samples,
                "expected_latency_us": request.expected_latency_us,
                "priority": request.priority,
            }
            for request in requests
        ],
        "algorithm_version": ALGORITHM_VERSION,
    }
    digest = hashlib.sha256(_canonical(canonical_payload).encode("utf-8")).hexdigest()
    return NormalizedTrace(
        trace_id=trace_id,
        name=name,
        models=models,
        requests=tuple(requests),
        node=node,
        source_schema=source_schema,
        payload_digest=digest,
    )


def model_contribution(model: ModelSpec, samples: int) -> tuple[int, int]:
    try:
        memory = checked_mul(model.memory_bytes, samples, "sample_memory * samples")
        latency = checked_mul(model.cost_us, samples, "sample_cost * samples")
    except QuantityError as exc:
        raise SpecInvalidError(str(exc), details={"model_id": model.model_id}) from exc
    return memory, latency
