from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from typing import Any

from .domain import EngineConfig, Layout, ObjectLayout, Replica, ShardLayout
from .errors import fail
from .gf import MAX_TOTAL_SHARDS


DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
MAX_SIZE_BYTES = (1 << 63) - 1

DEFAULT_ENGINE: dict[str, int] = EngineConfig().to_dict()


def _as_int(value: Any, field: str, *, allow_zero: bool = True) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        fail("E_LAYOUT_INVALID", f"{field} 必须是整数", details={"field": field})
    if allow_zero and value < 0:
        fail("E_LAYOUT_INVALID", f"{field} 不能为负数", details={"field": field})
    if not allow_zero and value <= 0:
        fail("E_LAYOUT_INVALID", f"{field} 必须为正整数", details={"field": field})
    if value > MAX_SIZE_BYTES:
        fail("E_LAYOUT_INVALID", f"{field} 超出 64 位有符号整数范围", details={"field": field})
    return value


def _normalize_digest(value: Any, field: str) -> str:
    if not isinstance(value, str):
        fail("E_LAYOUT_INVALID", f"{field} 摘要必须是字符串", details={"field": field})
    candidate = value.strip().lower()
    if candidate.startswith("sha-256:"):
        candidate = "sha256:" + candidate[8:]
    if not DIGEST_RE.fullmatch(candidate):
        fail(
            "E_LAYOUT_INVALID",
            f"{field} 必须为 sha256:<64位小写十六进制>",
            details={"field": field, "value": value},
        )
    return candidate


def _decode_content(entry: dict[str, Any], field: str) -> bytes | None:
    b64 = entry.get("content_b64", entry.get("content"))
    hex_value = entry.get("content_hex")
    if b64 is not None and hex_value is not None:
        fail("E_LAYOUT_INVALID", f"{field} 不能同时提供 content_b64 与 content_hex", details={"field": field})
    if b64 is not None:
        if not isinstance(b64, str):
            fail("E_LAYOUT_INVALID", f"{field}.content_b64 必须是字符串", details={"field": field})
        try:
            return base64.b64decode(b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            fail("E_LAYOUT_INVALID", f"{field}.content_b64 不是合法 Base64", details={"field": field, "reason": str(exc)})
    if hex_value is not None:
        if not isinstance(hex_value, str):
            fail("E_LAYOUT_INVALID", f"{field}.content_hex 必须是字符串", details={"field": field})
        try:
            return bytes.fromhex(hex_value)
        except ValueError as exc:
            fail("E_LAYOUT_INVALID", f"{field}.content_hex 不是合法十六进制", details={"field": field, "reason": str(exc)})
    return None


def _parse_engine_config(document: dict[str, Any]) -> EngineConfig:
    raw = document.get("engine", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        fail("E_LAYOUT_INVALID", "engine 配置必须是对象", details={"field": "engine"})
    merged = dict(DEFAULT_ENGINE)
    aliases = {
        "concurrency": "rebuild_concurrency",
        "queue_capacity": "rebuild_queue_capacity",
        "byte_budget": "round_byte_budget",
        "time_budget_us": "round_time_budget_us",
    }
    for key, value in raw.items():
        merged[aliases.get(key, key)] = value
    integer_fields = set(DEFAULT_ENGINE)
    unknown = sorted(set(merged) - integer_fields)
    if unknown:
        fail("E_LAYOUT_INVALID", "engine 配置包含未知字段", details={"fields": unknown})
    for key in integer_fields:
        merged[key] = _as_int(merged[key], f"engine.{key}", allow_zero=key in {"rebuild_queue_capacity", "round_byte_budget"})
    if not 1 <= merged["rebuild_concurrency"] <= 64:
        fail("E_LAYOUT_INVALID", "rebuild_concurrency 必须在 1..64", details={"value": merged["rebuild_concurrency"]})
    if merged["rebuild_queue_capacity"] > 100_000:
        fail("E_LAYOUT_INVALID", "rebuild_queue_capacity 不能超过 100000", details={"value": merged["rebuild_queue_capacity"]})
    if merged["round_time_budget_us"] <= 0:
        fail("E_LAYOUT_INVALID", "round_time_budget_us 必须大于 0", details={"value": merged["round_time_budget_us"]})
    if merged["rebuild_bandwidth_bytes_per_us"] <= 0:
        fail("E_LAYOUT_INVALID", "rebuild_bandwidth_bytes_per_us 必须大于 0")
    return EngineConfig(**merged)


def _parse_nodes(document: dict[str, Any]) -> dict[str, bool]:
    raw = document.get("nodes", {})
    health: dict[str, bool] = {}
    if raw is None:
        return health
    if isinstance(raw, dict):
        items = raw.items()
    elif isinstance(raw, list):
        items_list: list[tuple[str, Any]] = []
        for entry in raw:
            if not isinstance(entry, dict) or not entry.get("node_id"):
                fail("E_LAYOUT_INVALID", "nodes 条目必须包含 node_id", details={"field": "nodes"})
            items_list.append((str(entry["node_id"]), entry))
        items = items_list
    else:
        fail("E_LAYOUT_INVALID", "nodes 必须是对象或数组", details={"field": "nodes"})
        return health
    for node_id, value in items:
        if not isinstance(node_id, str) or not node_id:
            fail("E_LAYOUT_INVALID", "node_id 不能为空")
        if isinstance(value, bool):
            alive = value
        elif isinstance(value, dict):
            if "healthy" in value:
                if not isinstance(value["healthy"], bool):
                    fail("E_LAYOUT_INVALID", f"节点 {node_id} 的 healthy 必须是布尔值")
                alive = value["healthy"]
            else:
                status = str(value.get("status", "healthy")).lower()
                alive = status in {"healthy", "up", "online", "ok"}
        else:
            fail("E_LAYOUT_INVALID", f"节点 {node_id} 状态格式非法")
        health[node_id] = alive
    return health


def _parse_disks(document: dict[str, Any]) -> dict[tuple[str, str], str]:
    raw = document.get("disks", [])
    disks: dict[tuple[str, str], str] = {}
    if raw is None:
        return disks
    if isinstance(raw, dict):
        entries = list(raw.values())
    elif isinstance(raw, list):
        entries = raw
    else:
        fail("E_LAYOUT_INVALID", "disks 必须是数组或对象", details={"field": "disks"})
        return disks
    for entry in entries:
        if not isinstance(entry, dict):
            fail("E_LAYOUT_INVALID", "disks 条目必须是对象")
        node_id = entry.get("node_id")
        disk_id = entry.get("disk_id", entry.get("id"))
        if not isinstance(node_id, str) or not node_id or not isinstance(disk_id, str) or not disk_id:
            fail("E_LAYOUT_INVALID", "disk 条目必须包含 node_id 与 disk_id")
        status = str(entry.get("status", "online")).lower()
        disks[(node_id, disk_id)] = status
    return disks


def _parse_replica(
    entry: dict[str, Any],
    *,
    object_id: str,
    shard_index: int,
    replica_index: int,
    digest: str,
    size_bytes: int,
) -> Replica:
    field = f"objects[{object_id}].shards[{shard_index}].replicas[{replica_index}]"
    node_id = entry.get("node_id")
    disk_id = entry.get("disk_id")
    if not isinstance(node_id, str) or not node_id:
        fail("E_LAYOUT_INVALID", f"{field}.node_id 缺失", details={"object_id": object_id, "shard_index": shard_index})
    if not isinstance(disk_id, str) or not disk_id:
        fail("E_LAYOUT_INVALID", f"{field}.disk_id 缺失", details={"object_id": object_id, "shard_index": shard_index})
    replica_id = entry.get("replica_id")
    if replica_id is None:
        replica_id = f"{object_id}:s{shard_index}:{node_id}:{disk_id}"
    if not isinstance(replica_id, str) or not replica_id:
        fail("E_LAYOUT_INVALID", f"{field}.replica_id 非法")
    present = entry.get("present", True)
    if not isinstance(present, bool):
        fail("E_LAYOUT_INVALID", f"{field}.present 必须是布尔值")
    content = _decode_content(entry, field) if present else None
    if present and content is None:
        fail(
            "E_LAYOUT_INVALID",
            f"{field} 标记 present=true 但缺少分片内容",
            details={"object_id": object_id, "shard_index": shard_index, "replica_id": replica_id},
        )
    if content is not None and len(content) != size_bytes:
        fail(
            "E_LAYOUT_INVALID",
            f"{field} 内容长度与 size_bytes 不一致",
            details={"expected": size_bytes, "actual": len(content), "digest": digest},
        )
    return Replica(
        replica_id=replica_id,
        node_id=node_id,
        disk_id=disk_id,
        present=present,
        content=content,
    )


def _parse_shard(
    entry: dict[str, Any],
    *,
    object_id: str,
    expected_index: int,
    k: int,
    m: int,
    shard_size_bytes: int,
) -> ShardLayout:
    if not isinstance(entry, dict):
        fail("E_LAYOUT_INVALID", f"对象 {object_id} 的分片条目必须是对象")
    index = entry.get("shard_index", entry.get("index", expected_index))
    if isinstance(index, bool) or not isinstance(index, int) or index != expected_index:
        fail(
            "E_LAYOUT_INVALID",
            f"对象 {object_id} 分片索引缺失、重复或未按顺序归一化",
            details={"object_id": object_id, "expected_index": expected_index, "actual_index": index},
        )
    role = "data" if index < k else "parity"
    declared_role = entry.get("role")
    role_aliases = {"data": {"data", "数据"}, "parity": {"parity", "校验"}}
    if declared_role is not None and str(declared_role).lower() not in role_aliases[role]:
        fail("E_LAYOUT_INVALID", f"对象 {object_id} 分片 {index} 的 role 与 k 不一致", details={"expected": role, "actual": declared_role})
    digest = _normalize_digest(entry.get("digest", entry.get("sha256")), f"objects[{object_id}].shards[{index}].digest")
    size_bytes = _as_int(entry.get("size_bytes", entry.get("size", shard_size_bytes)), f"objects[{object_id}].shards[{index}].size_bytes")
    if size_bytes != shard_size_bytes:
        fail(
            "E_LAYOUT_INVALID",
            f"对象 {object_id} 各分片 size_bytes 必须一致",
            details={"shard_index": index, "expected": shard_size_bytes, "actual": size_bytes},
        )

    replicas_raw = entry.get("replicas")
    replica_entries: list[dict[str, Any]] = []
    if replicas_raw is not None:
        if not isinstance(replicas_raw, list) or not replicas_raw:
            fail("E_LAYOUT_INVALID", f"对象 {object_id} 分片 {index} 的 replicas 必须是非空数组")
        for replica in replicas_raw:
            if not isinstance(replica, dict):
                fail("E_LAYOUT_INVALID", f"对象 {object_id} 分片 {index} 的副本必须是对象")
            replica_entries.append(replica)
    elif "node_id" in entry or "disk_id" in entry:
        replica_entries.append(entry)
    else:
        fail(
            "E_LAYOUT_INVALID",
            f"对象 {object_id} 分片 {index} 缺少节点与磁盘位置",
            details={"object_id": object_id, "shard_index": index},
        )
    replicas = tuple(
        _parse_replica(
            replica_entry,
            object_id=object_id,
            shard_index=index,
            replica_index=replica_index,
            digest=digest,
            size_bytes=size_bytes,
        )
        for replica_index, replica_entry in enumerate(replica_entries)
    )
    replica_ids = [replica.replica_id for replica in replicas]
    if len(set(replica_ids)) != len(replica_ids):
        fail("E_LAYOUT_INVALID", f"对象 {object_id} 分片 {index} 存在重复 replica_id")
    return ShardLayout(index=index, role=role, size_bytes=size_bytes, digest=digest, replicas=replicas)


def _parse_object(object_id: str, entry: dict[str, Any], max_k_plus_m: int) -> ObjectLayout:
    if not isinstance(entry, dict):
        fail("E_LAYOUT_INVALID", f"对象 {object_id} 必须是对象")
    if "object_id" not in entry:
        fail("E_LAYOUT_INVALID", "对象标识缺失", details={"object_id": object_id})
    object_id_inner = entry.get("object_id")
    if not isinstance(object_id_inner, str) or not object_id_inner:
        fail("E_LAYOUT_INVALID", "object_id 缺失", details={"object_id": object_id})
    if object_id_inner != object_id:
        fail("E_LAYOUT_INVALID", "objects 映射键与 object_id 不一致", details={"key": object_id, "object_id": object_id_inner})
    k = _as_int(entry.get("k"), f"objects[{object_id}].k", allow_zero=False)
    m = _as_int(entry.get("m"), f"objects[{object_id}].m", allow_zero=False)
    if k + m > max_k_plus_m:
        fail(
            "E_LAYOUT_INVALID",
            f"对象 {object_id} 的 k+m 超过声明上限",
            details={"object_id": object_id, "k": k, "m": m, "max_k_plus_m": max_k_plus_m},
        )
    if k + m > MAX_TOTAL_SHARDS:
        fail("E_LAYOUT_INVALID", f"对象 {object_id} 的 k+m 超过 GF(256) 上限 255")
    shards_raw = entry.get("shards")
    if not isinstance(shards_raw, list) or len(shards_raw) != k + m:
        fail(
            "E_LAYOUT_INVALID",
            f"对象 {object_id} 必须提供恰好 k+m 个分片（含缺失分片占位）",
            details={"object_id": object_id, "expected": k + m, "actual": len(shards_raw) if isinstance(shards_raw, list) else None},
        )
    first_size = None
    for shard_entry in shards_raw:
        if isinstance(shard_entry, dict) and ("size_bytes" in shard_entry or "size" in shard_entry):
            first_size = shard_entry.get("size_bytes", shard_entry.get("size"))
            break
    if first_size is not None:
        shard_size = _as_int(first_size, f"objects[{object_id}].shard_size_bytes")
    else:
        shard_size = _as_int(entry.get("shard_size_bytes", 0), f"objects[{object_id}].shard_size_bytes")
    indexed_shards: list[tuple[int, dict[str, Any]]] = []
    seen_indices: set[int] = set()
    for position, shard_entry in enumerate(shards_raw):
        if not isinstance(shard_entry, dict):
            fail("E_LAYOUT_INVALID", f"对象 {object_id} 的分片条目必须是对象")
        shard_index = shard_entry.get("shard_index", shard_entry.get("index", position))
        if isinstance(shard_index, bool) or not isinstance(shard_index, int):
            fail("E_LAYOUT_INVALID", f"对象 {object_id} 分片索引必须是整数")
        if shard_index < 0 or shard_index >= k + m:
            fail(
                "E_LAYOUT_INVALID",
                f"对象 {object_id} 分片索引越界",
                details={"shard_index": shard_index, "total_shards": k + m},
            )
        if shard_index in seen_indices:
            fail("E_LAYOUT_INVALID", f"对象 {object_id} 分片索引重复", details={"shard_index": shard_index})
        seen_indices.add(shard_index)
        indexed_shards.append((shard_index, shard_entry))
    indexed_shards.sort(key=lambda item: item[0])
    shards = tuple(
        _parse_shard(
            shard_entry,
            object_id=object_id,
            expected_index=index,
            k=k,
            m=m,
            shard_size_bytes=shard_size,
        )
        for index, shard_entry in indexed_shards
    )
    return ObjectLayout(object_id=object_id, k=k, m=m, shard_size_bytes=shard_size, shards=shards)


def parse_layout(document: Any) -> Layout:
    if not isinstance(document, dict):
        fail("E_LAYOUT_INVALID", "布局文档必须是 JSON 对象")
    layout_version = document.get("layout_version", document.get("version"))
    if not isinstance(layout_version, str) or not layout_version.strip():
        fail("E_LAYOUT_INVALID", "layout_version 缺失")
    layout_version = layout_version.strip()
    max_k_plus_m = document.get(
        "max_k_plus_m",
        document.get("declared_max_k_plus_m", document.get("limits", {}).get("max_k_plus_m") if isinstance(document.get("limits"), dict) else None),
    )
    max_k_plus_m = _as_int(max_k_plus_m, "max_k_plus_m", allow_zero=False)
    if max_k_plus_m > MAX_TOTAL_SHARDS:
        fail("E_LAYOUT_INVALID", f"max_k_plus_m 不能超过 {MAX_TOTAL_SHARDS}", details={"value": max_k_plus_m})
    raw_objects = document.get("objects")
    if isinstance(raw_objects, list):
        objects_map: dict[str, dict[str, Any]] = {}
        for entry in raw_objects:
            if not isinstance(entry, dict):
                fail("E_LAYOUT_INVALID", "objects 条目必须是对象")
            object_id = entry.get("object_id")
            if not isinstance(object_id, str) or not object_id:
                fail("E_LAYOUT_INVALID", "对象标识缺失", details={"field": "object_id"})
            if object_id in objects_map:
                fail("E_LAYOUT_INVALID", f"object_id 重复: {object_id}")
            objects_map[object_id] = entry
    elif isinstance(raw_objects, dict):
        objects_map = raw_objects
    else:
        fail("E_LAYOUT_INVALID", "objects 缺失或格式非法", details={"field": "objects"})
        return None  # pragma: no cover
    if not objects_map:
        fail("E_LAYOUT_INVALID", "布局至少需要一个对象")
    objects = {
        str(object_id): _parse_object(str(object_id), entry, max_k_plus_m)
        for object_id, entry in objects_map.items()
    }
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    document_hash = "sha256:" + hashlib.sha256(canonical).hexdigest()
    return Layout(
        layout_version=layout_version,
        max_k_plus_m=max_k_plus_m,
        matrix_id="cauchy-gf256-0x11d-v1",
        objects=objects,
        node_health=_parse_nodes(document),
        disk_status=_parse_disks(document),
        engine=_parse_engine_config(document),
        raw_document=document,
        document_hash=document_hash,
    )


def layout_summary(layout: Layout) -> dict[str, Any]:
    objects = []
    for obj in layout.objects.values():
        objects.append(
            {
                "object_id": obj.object_id,
                "k": obj.k,
                "m": obj.m,
                "shard_size_bytes": obj.shard_size_bytes,
                "total_shards": obj.total_shards,
                "replica_count": sum(len(shard.replicas) for shard in obj.shards),
            }
        )
    return {
        "layout_version": layout.layout_version,
        "document_hash": layout.document_hash,
        "max_k_plus_m": layout.max_k_plus_m,
        "matrix_id": layout.matrix_id,
        "object_count": len(layout.objects),
        "objects": objects,
        "engine": layout.engine.to_dict(),
        "node_health": layout.node_health,
        "disk_status": {f"{node}/{disk}": status for (node, disk), status in sorted(layout.disk_status.items())},
    }
