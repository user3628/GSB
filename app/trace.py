from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any

from .domain import EngineConfig, Layout, Trace, TraceRequest
from .errors import fail
from .layout import MAX_SIZE_BYTES


PRIORITY_NAMES = {
    "low": 10,
    "normal": 50,
    "high": 80,
    "critical": 95,
}

CONFIG_ALIASES = {
    "concurrency": "rebuild_concurrency",
    "queue_capacity": "rebuild_queue_capacity",
    "byte_budget": "round_byte_budget",
    "time_budget_us": "round_time_budget_us",
}


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        fail("E_TRACE_INVALID", f"{field} 必须是整数", details={"field": field})
    if value < minimum:
        fail("E_TRACE_INVALID", f"{field} 不能小于 {minimum}", details={"field": field, "value": value})
    if value > MAX_SIZE_BYTES:
        fail("E_TRACE_INVALID", f"{field} 超出范围", details={"field": field})
    return value


def _parse_bitmap(value: Any, total_shards: int, field: str) -> int:
    if isinstance(value, bool):
        fail("E_TRACE_INVALID", f"{field} 不是合法位图")
    if isinstance(value, int):
        bitmap = value
    elif isinstance(value, str):
        text = value.strip().lower()
        try:
            if text.startswith("0x"):
                bitmap = int(text[2:] or "0", 16)
            elif text.startswith("0b"):
                bitmap = int(text[2:] or "0", 2)
            else:
                bitmap = int(text, 10)
        except ValueError:
            fail("E_TRACE_INVALID", f"{field} 不是合法位图字符串", details={"value": value})
    else:
        fail("E_TRACE_INVALID", f"{field} 必须是整数、十六进制或二进制字符串")
    if bitmap <= 0:
        fail("E_TRACE_INVALID", f"{field} 至少需要一个分片位", details={"bitmap": value})
    limit = (1 << total_shards) - 1
    if bitmap & ~limit:
        fail(
            "E_TRACE_INVALID",
            f"{field} 包含超出对象分片数的位",
            details={"bitmap": value, "total_shards": total_shards},
        )
    return bitmap


def _bits(bitmap: int) -> tuple[int, ...]:
    result = []
    index = 0
    while bitmap:
        if bitmap & 1:
            result.append(index)
        bitmap >>= 1
        index += 1
    return tuple(result)


def _resolve_options(layout: Layout, document: dict[str, Any]) -> EngineConfig:
    raw = document.get("options", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        fail("E_TRACE_INVALID", "options 必须是对象")
    merged = layout.engine.to_dict()
    for key, value in raw.items():
        normalized_key = CONFIG_ALIASES.get(key, key)
        if normalized_key not in merged:
            fail("E_TRACE_INVALID", f"options 包含未知字段: {key}")
        merged[normalized_key] = value
    for key, value in merged.items():
        allow_zero = key in {"rebuild_queue_capacity", "round_byte_budget"}
        if isinstance(value, bool) or not isinstance(value, int) or value < (0 if allow_zero else 1):
            fail("E_TRACE_INVALID", f"options.{key} 非法", details={"value": value})
        if value > MAX_SIZE_BYTES:
            fail("E_TRACE_INVALID", f"options.{key} 超出范围")
    if merged["rebuild_concurrency"] > 64:
        fail("E_TRACE_INVALID", "options.rebuild_concurrency 不能超过 64")
    if merged["rebuild_queue_capacity"] > 100_000:
        fail("E_TRACE_INVALID", "options.rebuild_queue_capacity 不能超过 100000")
    return EngineConfig(**merged)


def parse_trace(document: Any, layout: Layout) -> Trace:
    if not isinstance(document, dict):
        fail("E_TRACE_INVALID", "轨迹文档必须是 JSON 对象")
    layout_version = document.get("layout_version", layout.layout_version)
    if layout_version != layout.layout_version:
        fail(
            "E_TRACE_INVALID",
            "轨迹所引用布局版本与加载版本不一致",
            details={"expected": layout.layout_version, "actual": layout_version},
        )
    raw_requests = document.get("requests")
    if not isinstance(raw_requests, list) or not raw_requests:
        fail("E_TRACE_INVALID", "requests 必须是非空数组")
    options = _resolve_options(layout, document)
    previous_timestamp = None
    seen_ids: set[str] = set()
    normalized_requests: list[TraceRequest] = []
    for seq, entry in enumerate(raw_requests):
        if not isinstance(entry, dict):
            fail("E_TRACE_INVALID", f"requests[{seq}] 必须是对象")
        request_id = entry.get("request_id", entry.get("id"))
        if not isinstance(request_id, str) or not request_id.strip():
            fail("E_TRACE_INVALID", f"requests[{seq}].request_id 缺失")
        request_id = request_id.strip()
        if request_id in seen_ids:
            fail("E_TRACE_INVALID", f"同一轨迹内 request_id 重复: {request_id}")
        seen_ids.add(request_id)
        object_id = entry.get("object_id")
        if not isinstance(object_id, str) or not object_id:
            fail("E_TRACE_INVALID", f"requests[{seq}].object_id 缺失", details={"request_id": request_id})
        if object_id not in layout.objects:
            fail(
                "E_TRACE_INVALID",
                f"请求引用了未知对象: {object_id}",
                details={"request_id": request_id, "object_id": object_id},
            )
        timestamp = _integer(
            entry.get("arrival_ms", entry.get("timestamp_ms", entry.get("timestamp"))),
            f"requests[{seq}].arrival_ms",
        )
        if previous_timestamp is not None and timestamp < previous_timestamp:
            fail(
                "E_TRACE_INVALID",
                "请求时间戳必须单调不减，整批拒绝",
                details={"request_id": request_id, "previous": previous_timestamp, "current": timestamp},
            )
        previous_timestamp = timestamp
        obj = layout.objects[object_id]
        bitmap_value = entry.get(
            "shard_bitmap",
            entry.get("requested_bitmap", entry.get("shard_mask")),
        )
        if bitmap_value is None and "requested_shards" in entry:
            requested_raw = entry["requested_shards"]
            if not isinstance(requested_raw, list):
                fail("E_TRACE_INVALID", f"requests[{seq}].requested_shards 必须是数组")
            bitmap_value = 0
            for shard_index in requested_raw:
                if isinstance(shard_index, bool) or not isinstance(shard_index, int):
                    fail("E_TRACE_INVALID", f"requests[{seq}].requested_shards 必须是整数数组")
                if shard_index < 0 or shard_index >= obj.total_shards:
                    fail(
                        "E_TRACE_INVALID",
                        f"requests[{seq}].requested_shards 包含越界索引",
                        details={"shard_index": shard_index, "total_shards": obj.total_shards},
                    )
                bitmap_value |= 1 << shard_index
        shard_bitmap = _parse_bitmap(bitmap_value, obj.total_shards, f"requests[{seq}].shard_bitmap")
        priority = entry.get("priority", 50)
        if isinstance(priority, str):
            priority = PRIORITY_NAMES.get(priority.lower(), None)
            if priority is None:
                fail("E_TRACE_INVALID", f"requests[{seq}].priority 名称未知", details={"value": entry.get("priority")})
        priority = _integer(priority, f"requests[{seq}].priority")
        if priority > 100:
            fail("E_TRACE_INVALID", f"requests[{seq}].priority 必须在 0..100")
        tolerable = _integer(
            entry.get(
                "tolerable_latency_us",
                entry.get("deadline_us", entry.get("latency_budget_us")),
            ),
            f"requests[{seq}].tolerable_latency_us",
            minimum=1,
        )
        normalized_requests.append(
            TraceRequest(
                seq=seq,
                request_id=request_id,
                arrival_ms=timestamp,
                object_id=object_id,
                shard_bitmap=shard_bitmap,
                requested_shards=_bits(shard_bitmap),
                priority=priority,
                tolerable_latency_us=tolerable,
            )
        )
    canonical_requests = [request.canonical() | {"layout_version": layout.layout_version} for request in normalized_requests]
    trace_hash = "sha256:" + hashlib.sha256(
        json.dumps(canonical_requests, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    trace_id = document.get("trace_id")
    if not isinstance(trace_id, str) or not trace_id.strip():
        trace_id = f"trace-{trace_hash[7:19]}"
    else:
        trace_id = trace_id.strip()
    with_hashes = tuple(
        replace(
            request,
            content_hash="sha256:"
            + hashlib.sha256(
                json.dumps(
                    request.canonical() | {"layout_version": layout.layout_version, "options": options.to_dict()},
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8")
            ).hexdigest(),
        )
        for request in normalized_requests
    )
    return Trace(
        trace_id=trace_id,
        layout_version=layout.layout_version,
        requests=with_hashes,
        options=options,
        trace_hash=trace_hash,
    )
