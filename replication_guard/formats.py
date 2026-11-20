from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .errors import format_incompatible, format_malformed, policy_invalid
from .models import Policy, normalize_policy


BINARY_MAGIC = b"RGPB"
BINARY_FORMAT = "policy-binary"
JSONL_FORMAT = "policy-jsonl"
LATEST_BINARY_VERSION = 2
LATEST_JSONL_VERSION = 1
MAX_RECORD_BYTES = 16 * 1024 * 1024

BINARY_PRESENT_UPDATED_AT = 1 << 0
BINARY_PRESENT_ACTION = 1 << 1
BINARY_PRESENT_FORWARD = 1 << 2
BINARY_PRESENT_POLICY_VERSION = 1 << 3
_BINARY_V1_MASK = BINARY_PRESENT_UPDATED_AT | BINARY_PRESENT_ACTION
_BINARY_V2_MASK = (
    BINARY_PRESENT_UPDATED_AT
    | BINARY_PRESENT_ACTION
    | BINARY_PRESENT_FORWARD
    | BINARY_PRESENT_POLICY_VERSION
)

JSONL_KNOWN_FIELDS = {
    "policyId",
    "slaveId",
    "replicationGroup",
    "delayThresholdMs",
    "tolerableDelayMs",
    "expectedVersion",
    "degradationAction",
    "priority",
    "forwardCompatible",
    "policyVersion",
    "updatedAtMs",
}
JSONL_KNOWN_FIELDS.update(
    {
        "slave_id",
        "replication_group",
        "delay_threshold_ms",
        "tolerable_delay_ms",
        "expected_version",
        "degradation_action",
        "forward_compatible",
        "policy_version",
        "updated_at_ms",
    }
)

COMPATIBILITY_MATRIX: dict[str, Any] = {
    "matrixVersion": "1",
    "policy-binary": {
        "supportedVersions": [1, 2],
        "latestVersion": 2,
        "versions": {
            "1": {
                "decision": "DEGRADED_ACCEPT",
                "reason": "旧二进制版缺少前向兼容位与策略修订号；补齐声明默认值，秒级时间乘以 1000。",
            },
            "2": {"decision": "ACCEPT", "reason": "当前二进制版，毫秒时间与可选字段位图。"},
        },
        "higherVersions": {"decision": "REJECT", "reason": "高版本可能含未知必填字段。"},
    },
    "policy-jsonl": {
        "supportedVersions": [1],
        "latestVersion": 1,
        "versions": {
            "1": {"decision": "ACCEPT", "reason": "当前 JSON Lines 分片格式，毫秒时间。"},
        },
        "higherVersions": {
            "decision": "DEGRADED_ACCEPT_IF_NO_UNKNOWN_REQUIRED_FIELDS",
            "reason": "仅在未声明未知必填字段时保守读取；否则整批拒绝。",
        },
    },
}


@dataclass
class DecodedPolicies:
    policies: list[Policy]
    negotiation: dict[str, Any]
    degradations: list[dict[str, Any]] = field(default_factory=list)
    time_normalizations: list[dict[str, Any]] = field(default_factory=list)
    ignored_optional_fields: list[dict[str, Any]] = field(default_factory=list)
    shard_count: int = 1


def _encode_string(value: str, field_name: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFFFF:
        raise format_incompatible(f"{field_name} 超过 65535 字节", field=field_name)
    return struct.pack(">H", len(encoded)) + encoded


def _decode_string(payload: bytes, offset: int, field_name: str) -> tuple[str, int]:
    if offset + 2 > len(payload):
        raise format_malformed(f"读取 {field_name} 长度时截断")
    length = struct.unpack_from(">H", payload, offset)[0]
    offset += 2
    end = offset + length
    if end > len(payload):
        raise format_malformed(f"读取 {field_name} 内容时截断")
    try:
        text = payload[offset:end].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise format_malformed(f"{field_name} 不是合法 UTF-8") from exc
    return text, end


def _encode_binary_payload(policy: Policy, version: int) -> bytes:
    bitmap = BINARY_PRESENT_UPDATED_AT | BINARY_PRESENT_ACTION
    if version >= 2:
        bitmap |= BINARY_PRESENT_FORWARD | BINARY_PRESENT_POLICY_VERSION

    payload = bytearray(struct.pack(">H", bitmap))
    payload += _encode_string(policy.slave_id, "slave_id")
    payload += _encode_string(policy.replication_group, "replication_group")
    if policy.delay_threshold_ms > 0xFFFFFFFF or policy.tolerable_delay_ms > 0xFFFFFFFF:
        raise format_incompatible(
            "毫秒阈值超出二进制格式 u32 范围",
            policy_id=policy.slave_id,
            delay_threshold_ms=policy.delay_threshold_ms,
            tolerable_delay_ms=policy.tolerable_delay_ms,
        )
    if policy.priority > 0xFFFF:
        raise format_incompatible(
            "优先级超出二进制格式 u16 范围",
            policy_id=policy.slave_id,
            priority=policy.priority,
        )
    payload += struct.pack(">II", policy.delay_threshold_ms, policy.tolerable_delay_ms)
    payload += _encode_string(policy.expected_version, "expected_version")
    payload += struct.pack(">H", policy.priority)

    if version == 1:
        seconds = policy.updated_at_ms // 1000
        if seconds > 0xFFFFFFFF:
            raise format_incompatible("二进制 v1 秒级时间超出 u32 范围", updated_at_ms=policy.updated_at_ms)
        payload += struct.pack(">I", seconds)
    else:
        if policy.updated_at_ms > 0xFFFFFFFFFFFFFFFF:
            raise format_incompatible(
                "更新时间超出二进制格式 u64 范围",
                policy_id=policy.slave_id,
                updated_at_ms=policy.updated_at_ms,
            )
        payload += struct.pack(">Q", policy.updated_at_ms)

    payload += _encode_string(policy.degradation_action, "degradation_action")
    if version >= 2:
        if policy.policy_version > 0xFFFFFFFF:
            raise format_incompatible(
                "策略修订号超出二进制格式 u32 范围",
                policy_id=policy.slave_id,
                policy_version=policy.policy_version,
            )
        payload += struct.pack(">B", 1 if policy.forward_compatible else 0)
        payload += struct.pack(">I", policy.policy_version)
    return bytes(payload)


def encode_binary(policies: Iterable[Policy], *, version: int = LATEST_BINARY_VERSION) -> tuple[bytes, list[dict[str, Any]]]:
    if version not in (1, 2):
        raise format_incompatible(
            "无法导出该二进制格式版本",
            requested_version=version,
            supported_versions=[1, 2],
        )
    ordered = sorted(policies, key=lambda item: item.slave_id)
    truncations: list[dict[str, Any]] = []
    if version == 1:
        for policy in ordered:
            if policy.updated_at_ms % 1000:
                truncations.append(
                    {
                        "policyId": policy.slave_id,
                        "field": "updated_at_ms",
                        "from": policy.updated_at_ms,
                        "to": (policy.updated_at_ms // 1000) * 1000,
                        "rule": "FLOOR_TO_SECOND_MULTIPLE_OF_1000",
                    }
                )

    output = bytearray(BINARY_MAGIC)
    output += struct.pack(">BBI", version, 0, len(ordered))
    for policy in ordered:
        payload = _encode_binary_payload(policy, version)
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        record = payload + struct.pack(">I", crc)
        output += struct.pack(">I", len(record)) + record
    return bytes(output), truncations


def _binary_decision(version: int) -> str:
    if version == 1:
        return "DEGRADED_ACCEPT"
    if version == 2:
        return "ACCEPT"
    return "REJECT"


def decode_binary(data: bytes) -> DecodedPolicies:
    if len(data) < 10:
        raise format_malformed("二进制文件头不完整")
    if data[:4] != BINARY_MAGIC:
        raise format_malformed("二进制魔数不匹配", expected=BINARY_MAGIC.decode("ascii"))
    version = data[4]
    flags = data[5]
    count = struct.unpack_from(">I", data, 6)[0]
    if flags != 0:
        raise format_incompatible("二进制头包含未知标志", flags=flags)
    if len(data) < 10 + 0:
        raise format_malformed("二进制头不完整")

    negotiation = {
        "format": BINARY_FORMAT,
        "inputVersion": version,
        "latestVersion": LATEST_BINARY_VERSION,
        "decision": _binary_decision(version),
        "matrixVersion": COMPATIBILITY_MATRIX["matrixVersion"],
        "unknownRequiredFields": [],
        "timePrecision": "seconds" if version == 1 else "milliseconds",
        "targetTimePrecision": "milliseconds",
    }
    if version not in (1, 2):
        raise format_incompatible(
            "二进制格式版本高于支持范围，可能含未知必填字段",
            format=BINARY_FORMAT,
            input_version=version,
            supported_versions=[1, 2],
            negotiation=negotiation,
        )

    policies: list[Policy] = []
    degradations: list[dict[str, Any]] = []
    normalizations: list[dict[str, Any]] = []
    offset = 10
    for index in range(count):
        if offset + 4 > len(data):
            raise format_malformed("记录长度前缀截断", record_index=index)
        record_length = struct.unpack_from(">I", data, offset)[0]
        offset += 4
        if record_length < 4:
            raise format_malformed("记录长度小于 CRC32 本身", record_index=index)
        if record_length > MAX_RECORD_BYTES:
            raise format_malformed("记录长度超过安全上限", record_index=index, record_length=record_length)
        record_end = offset + record_length
        if record_end > len(data):
            raise format_malformed("记录内容截断", record_index=index)
        payload = data[offset : record_end - 4]
        stored_crc = struct.unpack_from(">I", data, record_end - 4)[0]
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if stored_crc != actual_crc:
            raise format_malformed(
                "CRC32 校验失败",
                record_index=index,
                expected_crc=stored_crc,
                actual_crc=actual_crc,
            )
        policy, record_degradations, record_normalizations = _decode_binary_payload(
            payload, version=version, record_index=index
        )
        policies.append(policy)
        degradations.extend(record_degradations)
        normalizations.extend(record_normalizations)
        offset = record_end

    if offset != len(data):
        raise format_malformed("二进制文件尾部存在未知必填数据", trailing_bytes=len(data) - offset)

    return DecodedPolicies(
        policies=policies,
        negotiation=negotiation,
        degradations=degradations,
        time_normalizations=normalizations,
    )


def _decode_binary_payload(
    payload: bytes, *, version: int, record_index: int
) -> tuple[Policy, list[dict[str, Any]], list[dict[str, Any]]]:
    if len(payload) < 2:
        raise format_malformed("记录缺少可选字段位图", record_index=record_index)
    bitmap = struct.unpack_from(">H", payload, 0)[0]
    allowed_mask = _BINARY_V1_MASK if version == 1 else _BINARY_V2_MASK
    unknown_bits = bitmap & ~allowed_mask
    if unknown_bits:
        raise format_incompatible(
            "记录位图包含当前格式版本不认识的必填字段位",
            record_index=record_index,
            unknown_bitmap_bits=unknown_bits,
        )

    offset = 2
    slave_id, offset = _decode_string(payload, offset, "slave_id")
    replication_group, offset = _decode_string(payload, offset, "replication_group")
    if offset + 8 > len(payload):
        raise format_malformed("阈值字段截断", record_index=record_index)
    threshold, tolerable = struct.unpack_from(">II", payload, offset)
    offset += 8
    expected_version, offset = _decode_string(payload, offset, "expected_version")
    if offset + 2 > len(payload):
        raise format_malformed("优先级截断", record_index=record_index)
    priority = struct.unpack_from(">H", payload, offset)[0]
    offset += 2

    degradations: list[dict[str, Any]] = []
    normalizations: list[dict[str, Any]] = []
    if bitmap & BINARY_PRESENT_UPDATED_AT:
        if version == 1:
            if offset + 4 > len(payload):
                raise format_malformed("更新时间截断", record_index=record_index)
            seconds = struct.unpack_from(">I", payload, offset)[0]
            offset += 4
            updated_at_ms = seconds * 1000
            normalizations.append(
                {
                    "policyId": slave_id,
                    "field": "updated_at_ms",
                    "sourcePrecision": "seconds",
                    "targetPrecision": "milliseconds",
                    "rule": "SECONDS_MULTIPLY_1000",
                }
            )
        else:
            if offset + 8 > len(payload):
                raise format_malformed("更新时间截断", record_index=record_index)
            updated_at_ms = struct.unpack_from(">Q", payload, offset)[0]
            offset += 8
    else:
        updated_at_ms = 0
        degradations.append(
            {
                "policyId": slave_id,
                "field": "updated_at_ms",
                "action": "DEFAULTED",
                "default": 0,
                "reason": "旧格式记录未携带更新时间",
            }
        )

    if bitmap & BINARY_PRESENT_ACTION:
        action, offset = _decode_string(payload, offset, "degradation_action")
    else:
        action = "NONE"
        degradations.append(
            {
                "policyId": slave_id,
                "field": "degradation_action",
                "action": "DEFAULTED",
                "default": "NONE",
                "reason": "旧格式记录未携带降级动作",
            }
        )

    if bitmap & BINARY_PRESENT_FORWARD:
        if offset + 1 > len(payload):
            raise format_malformed("前向兼容标志截断", record_index=record_index)
        forward_compatible = bool(payload[offset])
        offset += 1
    else:
        forward_compatible = False
        degradations.append(
            {
                "policyId": slave_id,
                "field": "forward_compatible",
                "action": "DEFAULTED",
                "default": False,
                "reason": "二进制 v1 不含前向兼容字段",
            }
        )

    if bitmap & BINARY_PRESENT_POLICY_VERSION:
        if offset + 4 > len(payload):
            raise format_malformed("策略修订号截断", record_index=record_index)
        policy_version = struct.unpack_from(">I", payload, offset)[0]
        offset += 4
    else:
        policy_version = 1
        degradations.append(
            {
                "policyId": slave_id,
                "field": "policy_version",
                "action": "DEFAULTED",
                "default": 1,
                "reason": "二进制 v1 不含策略修订号",
            }
        )

    if offset != len(payload):
        raise format_incompatible(
            "记录包含当前格式版本未知的必填尾部字段",
            record_index=record_index,
            trailing_bytes=len(payload) - offset,
        )

    raw = {
        "slave_id": slave_id,
        "replication_group": replication_group,
        "delay_threshold_ms": threshold,
        "tolerable_delay_ms": tolerable,
        "expected_version": expected_version,
        "degradation_action": action,
        "priority": priority,
        "forward_compatible": forward_compatible,
        "policy_version": policy_version,
        "updated_at_ms": updated_at_ms,
    }
    try:
        policy = normalize_policy(raw)
    except Exception as exc:
        if hasattr(exc, "details"):
            exc.details.setdefault("record_index", record_index)
        raise
    return policy, degradations, normalizations


def _jsonl_meta(version: int, *, shard_index: int, shard_count: int) -> dict[str, Any]:
    return {
        "_meta": {
            "format": JSONL_FORMAT,
            "version": version,
            "timePrecision": "milliseconds",
            "optionalDefaults": {
                "forwardCompatible": False,
                "policyVersion": 1,
                "updatedAtMs": 0,
                "degradationAction": "NONE",
            },
            "shardIndex": shard_index,
            "shardCount": shard_count,
        }
    }


def encode_jsonl_shards(
    policies: Iterable[Policy], *, shard_count: int = 1, version: int = LATEST_JSONL_VERSION
) -> dict[str, str]:
    if version != 1:
        raise format_incompatible(
            "无法导出该 JSONL 版本", requested_version=version, supported_versions=[1]
        )
    if shard_count < 1:
        raise format_incompatible("分片数必须至少为 1", shard_count=shard_count)
    ordered = sorted(policies, key=lambda item: item.slave_id)
    buckets: list[list[Policy]] = [[] for _ in range(shard_count)]
    for index, policy in enumerate(ordered):
        buckets[index % shard_count].append(policy)
    result: dict[str, str] = {}
    width = max(4, len(str(shard_count)))
    for index, bucket in enumerate(buckets, start=1):
        lines = [json.dumps(_jsonl_meta(version, shard_index=index, shard_count=shard_count), separators=(",", ":"))]
        for policy in bucket:
            lines.append(json.dumps(policy.to_jsonl_dict(), ensure_ascii=False, separators=(",", ":")))
        result[f"policies-{index:0{width}d}.jsonl"] = "\n".join(lines) + "\n"
    return result


def encode_jsonl(policies: Iterable[Policy], *, version: int = LATEST_JSONL_VERSION) -> str:
    shards = encode_jsonl_shards(policies, shard_count=1, version=version)
    return next(iter(shards.values()))


def decode_jsonl(text: str | bytes, *, shard_count_hint: int | None = None) -> DecodedPolicies:
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise format_malformed("JSON Lines 不是合法 UTF-8") from exc
    lines = [(number, line.strip()) for number, line in enumerate(text.splitlines(), start=1) if line.strip()]
    if not lines:
        raise format_malformed("JSON Lines 为空")

    meta: dict[str, Any] | None = None
    records: list[tuple[int, Mapping[str, Any]]] = []
    for line_no, line in lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise format_malformed("JSONL 行不是合法 JSON", line=line_no, column=exc.colno) from exc
        if not isinstance(value, Mapping):
            raise format_malformed("JSONL 行必须是 JSON 对象", line=line_no)
        if "_meta" in value:
            candidate = value["_meta"]
            if not isinstance(candidate, Mapping):
                raise format_malformed("_meta 必须是对象", line=line_no)
            meta = dict(candidate) if meta is None else {**meta, **dict(candidate)}
            continue
        records.append((line_no, value))

    if not records:
        raise format_malformed("JSON Lines 不含策略记录")

    input_version = 1
    if meta is not None:
        format_name = meta.get("format", JSONL_FORMAT)
        if format_name not in {JSONL_FORMAT, "jsonl", "ndjson"}:
            raise format_incompatible("JSONL 元数据声明了其他格式", declared_format=format_name)
        try:
            input_version = int(meta.get("version", 1))
        except (TypeError, ValueError) as exc:
            raise format_incompatible("JSONL 版本标记不是整数", version=meta.get("version")) from exc

    required_fields_raw = []
    if meta is not None:
        for key in ("requiredFields", "unknownRequiredFields", "mandatoryFields"):
            value = meta.get(key, [])
            if isinstance(value, list):
                required_fields_raw.extend(str(item) for item in value)
    for _line_no, record in records:
        declared = record.get("_requiredFields", [])
        if isinstance(declared, list):
            required_fields_raw.extend(str(item) for item in declared)
    unknown_required = sorted(set(required_fields_raw) - JSONL_KNOWN_FIELDS)

    negotiation = {
        "format": JSONL_FORMAT,
        "inputVersion": input_version,
        "latestVersion": LATEST_JSONL_VERSION,
        "decision": "ACCEPT",
        "matrixVersion": COMPATIBILITY_MATRIX["matrixVersion"],
        "unknownRequiredFields": unknown_required,
        "timePrecision": "milliseconds",
        "targetTimePrecision": "milliseconds",
    }
    if meta is not None and meta.get("timePrecision") not in (None, "milliseconds", "ms"):
        raise format_incompatible(
            "JSONL 声明了不支持的更新时间精度",
            time_precision=meta.get("timePrecision"),
        )

    if input_version < 1:
        raise format_incompatible("JSONL 版本号必须为正整数", input_version=input_version)
    if unknown_required:
        raise format_incompatible(
            "高版本 JSONL 声明了未知必填字段",
            format=JSONL_FORMAT,
            input_version=input_version,
            unknown_required_fields=unknown_required,
            negotiation={**negotiation, "decision": "REJECT"},
        )
    if input_version > LATEST_JSONL_VERSION:
        negotiation["decision"] = "DEGRADED_ACCEPT"
        negotiation["reason"] = "高版本未声明未知必填字段，按 v1 已知字段保守读取。"

    policies: list[Policy] = []
    degradations: list[dict[str, Any]] = []
    ignored_optional: list[dict[str, Any]] = []
    required_alias_groups = {
        "forwardCompatible": ("forwardCompatible", "forward_compatible"),
        "policyVersion": ("policyVersion", "policy_version"),
        "updatedAtMs": ("updatedAtMs", "updated_at_ms"),
        "degradationAction": ("degradationAction", "degradation_action"),
        "policyId": ("policyId", "slaveId", "slave_id"),
        "replicationGroup": ("replicationGroup", "replication_group"),
        "delayThresholdMs": ("delayThresholdMs", "delay_threshold_ms"),
        "tolerableDelayMs": ("tolerableDelayMs", "tolerable_delay_ms"),
        "expectedVersion": ("expectedVersion", "expected_version"),
        "priority": ("priority",),
    }
    for line_no, record in records:
        policy_id = record.get("policyId", record.get("slaveId", f"line-{line_no}"))
        missing_declared = [
            field_name
            for field_name in sorted(set(required_fields_raw))
            if not any(alias in record for alias in required_alias_groups.get(field_name, (field_name,)))
        ]
        if missing_declared:
            raise format_incompatible(
                "记录缺少版本元数据声明的必填字段",
                format=JSONL_FORMAT,
                line=line_no,
                policy_id=policy_id,
                missing_required_fields=missing_declared,
                input_version=input_version,
            )
        unknown = sorted(set(record) - JSONL_KNOWN_FIELDS - {"_requiredFields"})
        if unknown:
            ignored_optional.append({"line": line_no, "policyId": policy_id, "fields": unknown})
            if input_version > LATEST_JSONL_VERSION:
                raise format_incompatible(
                    "高版本 JSONL 记录包含未知字段，无法确认其可选性",
                    line=line_no,
                    policy_id=policy_id,
                    unknown_fields=unknown,
                )
        defaults: dict[str, tuple[Any, str]] = {
            "forwardCompatible": (False, "旧 JSONL 记录缺少前向兼容标记"),
            "policyVersion": (1, "旧 JSONL 记录缺少策略修订号"),
            "updatedAtMs": (0, "旧 JSONL 记录缺少毫秒更新时间"),
            "degradationAction": ("NONE", "旧 JSONL 记录缺少降级动作"),
        }
        for field_name, (default, reason) in defaults.items():
            aliases = {
                "forwardCompatible": ("forwardCompatible", "forward_compatible"),
                "policyVersion": ("policyVersion", "policy_version"),
                "updatedAtMs": ("updatedAtMs", "updated_at_ms"),
                "degradationAction": ("degradationAction", "degradation_action"),
            }[field_name]
            if not any(key in record for key in aliases):
                record = dict(record)
                record[field_name] = default
                degradations.append(
                    {
                        "policyId": str(policy_id),
                        "field": field_name,
                        "action": "DEFAULTED",
                        "default": default,
                        "reason": reason,
                        "line": line_no,
                    }
                )
        try:
            policy = normalize_policy(record)
        except Exception as exc:
            if hasattr(exc, "details"):
                exc.details.setdefault("line", line_no)
            raise
        policies.append(policy)

    shard_count = shard_count_hint or 1
    if meta is not None and meta.get("shardCount") is not None:
        try:
            declared_count = int(meta["shardCount"])
        except (TypeError, ValueError) as exc:
            raise format_incompatible("JSONL shardCount 不是整数", shard_count=meta.get("shardCount")) from exc
        if declared_count < 1:
            raise format_incompatible("JSONL shardCount 必须为正整数", shard_count=declared_count)
        shard_count = declared_count

    return DecodedPolicies(
        policies=policies,
        negotiation=negotiation,
        degradations=degradations,
        ignored_optional_fields=ignored_optional,
        shard_count=shard_count,
    )


def detect_format(data: bytes | str, format_hint: str | None = None) -> str:
    hint = (format_hint or "auto").strip().lower()
    aliases = {
        "auto": "auto",
        "binary": BINARY_FORMAT,
        "bin": BINARY_FORMAT,
        "compact": BINARY_FORMAT,
        "compact-binary": BINARY_FORMAT,
        "policy-binary": BINARY_FORMAT,
        "jsonl": JSONL_FORMAT,
        "ndjson": JSONL_FORMAT,
        "json-lines": JSONL_FORMAT,
        "policy-jsonl": JSONL_FORMAT,
    }
    if hint not in aliases:
        raise format_incompatible("未知导入格式提示", format_hint=format_hint)
    normalized_hint = aliases[hint]
    if normalized_hint != "auto":
        return normalized_hint
    if isinstance(data, bytes) and data.startswith(BINARY_MAGIC):
        return BINARY_FORMAT
    return JSONL_FORMAT


def decode_policies(data: bytes | str, format_hint: str | None = None) -> DecodedPolicies:
    selected = detect_format(data, format_hint)
    if selected == BINARY_FORMAT:
        if isinstance(data, str):
            raise format_incompatible("binary 格式需要二进制请求体")
        return decode_binary(data)
    return decode_jsonl(data)

