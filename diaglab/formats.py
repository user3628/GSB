"""Compact binary and fragmented JSON-lines codecs plus format negotiation."""

from __future__ import annotations

import json
import struct
import zlib
from collections import Counter
from typing import Any, Iterable, Sequence

from .errors import bad_request, format_unsupported, frame_corrupt, unit_invalid
from .models import DecodedBundle, DiagnosticRecord, NegotiationReport, RawRecord


MAGIC = b"DXPK"
BINARY_FORMAT = "compact-binary"
JSONL_FORMAT = "jsonl"
MAX_SUPPORTED_VERSION = 2
HEADER_SIZE = 16
MAX_RECORD_BYTES = 1024 * 1024
ENDIAN_MARKERS = {"little": 0x4C, "big": 0x42}
ENDIAN_BY_MARKER = {value: key for key, value in ENDIAN_MARKERS.items()}
RTT_UNIT_CODES = {"us": 0, "ms": 1}
RTT_UNITS_BY_CODE = {value: key for key, value in RTT_UNIT_CODES.items()}
PAYLOAD_UNIT_CODES = {"B": 0, "KiB": 1}
PAYLOAD_UNITS_BY_CODE = {value: key for key, value in PAYLOAD_UNIT_CODES.items()}
PROTOCOL_CODES = {"tcp": 0, "udp": 1, "icmp": 2}
PROTOCOLS_BY_CODE = {value: key for key, value in PROTOCOL_CODES.items()}
FAILURE_CODES = {"none": 0, "refused": 1, "timeout": 2, "reset": 3, "error": 4}
FAILURES_BY_CODE = {value: key for key, value in FAILURE_CODES.items()}

COMPATIBILITY_MATRIX: dict[str, Any] = {
    "matrix_version": "2026-11-23",
    "formats": {
        BINARY_FORMAT: {
            "1": {
                "decision": "degrade_accept",
                "reason": "旧版可选字段可能缺失，按 rtt_us=0、payload_bytes=0 补齐",
            },
            "2": {"decision": "accept", "reason": "当前完整格式"},
            ">=3": {
                "decision": "reject",
                "code": "E_FORMAT_UNSUPPORTED",
                "reason": "版本包含未知必填字段",
            },
        },
        JSONL_FORMAT: {
            "1": {
                "decision": "degrade_accept",
                "reason": "旧版可选字段可能缺失，按 rtt_us=0、payload_bytes=0 补齐",
            },
            "2": {"decision": "accept", "reason": "当前完整格式"},
            ">=3": {
                "decision": "reject",
                "code": "E_FORMAT_UNSUPPORTED",
                "reason": "版本包含未知必填字段",
            },
        },
    },
}


def _write_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFFFF:
        raise bad_request("字符串字段超过 u16 长度上限", field_length=len(encoded))
    return struct.pack(">H", len(encoded)) + encoded


def _read_string(payload: bytes, offset: int, field: str) -> tuple[str, int]:
    if offset + 2 > len(payload):
        raise frame_corrupt("记录字符串长度前缀被截断", field=field, offset=offset)
    length = struct.unpack_from(">H", payload, offset)[0]
    offset += 2
    if offset + length > len(payload):
        raise frame_corrupt("记录字符串越过帧边界", field=field, offset=offset, length=length)
    try:
        value = payload[offset : offset + length].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise frame_corrupt("记录字符串不是合法 UTF-8", field=field) from exc
    return value, offset + length


def _encode_binary_record(record: DiagnosticRecord, version: int, endian: str) -> bytes:
    e = "<" if endian == "little" else ">"
    presence = 0
    if version >= 2 or not record.rtt_missing:
        presence |= 0x01
    if version >= 2 or not record.payload_missing:
        presence |= 0x02
    if version >= 2 and (record.rtt_missing or record.payload_missing):
        raise bad_request("格式版本 2 不允许省略 RTT 或载荷字段")

    protocol_code = PROTOCOL_CODES.get(record.protocol)
    failure_code = FAILURE_CODES.get(record.failure_type)
    if protocol_code is None or failure_code is None:
        raise bad_request("无法编码未知协议或失败类型")

    payload = bytearray()
    payload += struct.pack("B", presence)
    payload += struct.pack(f"{e}Q", record.timestamp_ms)
    payload += struct.pack(
        "BBB",
        protocol_code,
        1 if record.success else 0,
        failure_code,
    )
    payload += _write_string(record.probe_id)
    payload += _write_string(record.path_node)
    if presence & 0x01:
        payload += struct.pack(f"{e}I", record.rtt_us)
    if presence & 0x02:
        payload += struct.pack(f"{e}I", record.payload_bytes)
    return bytes(payload)


def _decode_binary_record(
    payload: bytes,
    version: int,
    endian: str,
    rtt_unit: str,
    payload_unit: str,
    ordinal: int,
) -> RawRecord:
    e = "<" if endian == "little" else ">"
    if len(payload) < 1 + 8 + 3:
        raise frame_corrupt("记录载荷过短", ordinal=ordinal)
    presence = payload[0]
    if presence & 0xFC:
        raise frame_corrupt(
            "存在位图包含当前版本无法解释的必填/未知字段",
            ordinal=ordinal,
            presence_bitmap=presence,
        )
    if version >= 2 and presence & 0x03 != 0x03:
        raise frame_corrupt("版本 2 记录必须包含 RTT 与载荷字段", ordinal=ordinal)

    timestamp_value = struct.unpack_from(f"{e}Q", payload, 1)[0]
    protocol_code, success_byte, failure_code = struct.unpack_from("BBB", payload, 9)
    if protocol_code not in PROTOCOLS_BY_CODE:
        raise frame_corrupt("未知探测协议代码", ordinal=ordinal, code=protocol_code)
    if success_byte not in (0, 1):
        raise frame_corrupt("成功标志必须为 0 或 1", ordinal=ordinal)
    if failure_code not in FAILURES_BY_CODE:
        raise frame_corrupt("未知失败类型代码", ordinal=ordinal, code=failure_code)

    offset = 12
    probe_id, offset = _read_string(payload, offset, "probe_id")
    path_node, offset = _read_string(payload, offset, "path_node")
    rtt_value: int | None = None
    payload_value: int | None = None
    if presence & 0x01:
        if offset + 4 > len(payload):
            raise frame_corrupt("RTT 字段被截断", ordinal=ordinal)
        rtt_value = struct.unpack_from(f"{e}I", payload, offset)[0]
        offset += 4
    if presence & 0x02:
        if offset + 4 > len(payload):
            raise frame_corrupt("载荷字段被截断", ordinal=ordinal)
        payload_value = struct.unpack_from(f"{e}I", payload, offset)[0]
        offset += 4
    if offset != len(payload):
        raise frame_corrupt(
            "记录长度前缀与字段实际占用不一致",
            ordinal=ordinal,
            consumed=offset,
            declared=len(payload),
        )
    return RawRecord(
        timestamp_value=timestamp_value,
        timestamp_unit="ms",
        probe_id=probe_id,
        path_node=path_node,
        protocol=PROTOCOLS_BY_CODE[protocol_code],
        success=bool(success_byte),
        rtt_value=rtt_value,
        rtt_unit=rtt_unit,
        payload_value=payload_value,
        payload_unit=payload_unit,
        failure_type=FAILURES_BY_CODE[failure_code],
    )


def encode_binary(
    records: Sequence[DiagnosticRecord],
    *,
    version: int = 2,
    endian: str = "little",
    rtt_unit: str = "us",
    payload_unit: str = "B",
) -> bytes:
    """Encode normalized records into the compact binary diagnostic package."""

    if version not in (1, 2):
        raise format_unsupported("仅支持编码格式版本 1 或 2", version=version)
    if endian not in ENDIAN_MARKERS:
        raise bad_request("endian 必须为 little 或 big", endian=endian)
    if rtt_unit not in RTT_UNIT_CODES or payload_unit not in PAYLOAD_UNIT_CODES:
        raise unit_invalid("二进制格式声明了不支持的单位", rtt_unit=rtt_unit, payload_unit=payload_unit)
    if len(records) > 0xFFFFFFFF:
        raise bad_request("记录数超过 u32 上限")

    e = "<" if endian == "little" else ">"
    header = struct.pack(
        f"{e}4sBBBBHIH",
        MAGIC,
        version,
        ENDIAN_MARKERS[endian],
        RTT_UNIT_CODES[rtt_unit],
        PAYLOAD_UNIT_CODES[payload_unit],
        0,
        len(records),
        HEADER_SIZE,
    )
    out = bytearray(header)
    for record in records:
        payload = _encode_binary_record(record, version, endian)
        out += struct.pack(f"{e}I", len(payload))
        out += payload
        out += struct.pack(f"{e}I", zlib.crc32(payload) & 0xFFFFFFFF)
    return bytes(out)


def _binary_header(data: bytes) -> dict[str, Any]:
    if len(data) < HEADER_SIZE:
        raise frame_corrupt("二进制头不足 16 字节", actual_bytes=len(data))
    if data[:4] != MAGIC:
        raise format_unsupported("二进制魔数不匹配", magic_hex=data[:4].hex())
    version = data[4]
    endian_marker = data[5]
    if endian_marker not in ENDIAN_BY_MARKER:
        raise frame_corrupt("未知大小端标记", endian_marker=endian_marker)
    endian = ENDIAN_BY_MARKER[endian_marker]
    e = "<" if endian == "little" else ">"
    rtt_unit_code, payload_unit_code, flags, count, header_size = struct.unpack_from(
        f"{e}BBHIH", data, 6
    )
    if version > MAX_SUPPORTED_VERSION:
        raise format_unsupported(
            "格式版本高于本工作台支持的版本",
            version=version,
            max_supported_version=MAX_SUPPORTED_VERSION,
            decision="reject",
            compatibility_matrix=COMPATIBILITY_MATRIX,
        )
    if version < 1:
        raise format_unsupported("格式版本 0 不受支持", version=version)
    if flags & 0x0001:
        raise format_unsupported(
            "头部声明了未知必填特性",
            version=version,
            required_flags=flags,
            decision="reject",
        )
    if rtt_unit_code not in RTT_UNITS_BY_CODE:
        raise unit_invalid("未知 RTT 单位代码", rtt_unit_code=rtt_unit_code)
    if payload_unit_code not in PAYLOAD_UNITS_BY_CODE:
        raise unit_invalid("未知载荷单位代码", payload_unit_code=payload_unit_code)
    if header_size < HEADER_SIZE:
        raise frame_corrupt("头部长度小于最小头部长度", declared=header_size)
    if header_size > len(data):
        raise frame_corrupt("头部长度越过文件边界", declared=header_size, actual=len(data))
    return {
        "version": version,
        "endian": endian,
        "rtt_unit": RTT_UNITS_BY_CODE[rtt_unit_code],
        "payload_unit": PAYLOAD_UNITS_BY_CODE[payload_unit_code],
        "flags": flags,
        "count": count,
        "header_size": header_size,
    }


def _decode_binary(data: bytes) -> DecodedBundle:
    header = _binary_header(data)
    version = header["version"]
    endian = header["endian"]
    e = "<" if endian == "little" else ">"
    offset = header["header_size"]
    count = header["count"]
    records: list[RawRecord] = []
    for ordinal in range(count):
        if offset + 4 > len(data):
            raise frame_corrupt(
                "记录长度前缀被截断",
                ordinal=ordinal,
                offset=offset,
                declared_count=count,
            )
        length = struct.unpack_from(f"{e}I", data, offset)[0]
        offset += 4
        if length <= 0 or length > MAX_RECORD_BYTES:
            raise frame_corrupt(
                "记录长度前缀非法",
                ordinal=ordinal,
                length=length,
                max_record_bytes=MAX_RECORD_BYTES,
            )
        frame_end = offset + length
        if frame_end + 4 > len(data):
            raise frame_corrupt(
                "记录载荷或 CRC 越过文件边界",
                ordinal=ordinal,
                length=length,
                remaining=max(0, len(data) - offset),
            )
        payload = data[offset:frame_end]
        expected_crc = struct.unpack_from(f"{e}I", data, frame_end)[0]
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise frame_corrupt(
                "CRC32 校验失败",
                ordinal=ordinal,
                expected_crc=f"{expected_crc:08x}",
                actual_crc=f"{actual_crc:08x}",
            )
        records.append(
            _decode_binary_record(
                payload,
                version,
                endian,
                header["rtt_unit"],
                header["payload_unit"],
                ordinal,
            )
        )
        offset = frame_end + 4
    if offset != len(data):
        raise frame_corrupt(
            "记录数与文件尾部字节不一致",
            declared_count=count,
            trailing_bytes=len(data) - offset,
        )

    degradations: list[dict[str, Any]] = []
    if version == 1:
        missing_rtt = [i for i, r in enumerate(records) if r.rtt_value is None]
        missing_payload = [i for i, r in enumerate(records) if r.payload_value is None]
        degradations.append(
            {
                "kind": "version_policy",
                "version": 1,
                "missing_rtt_records": missing_rtt,
                "missing_payload_records": missing_payload,
                "defaults": {"rtt_us": 0, "payload_bytes": 0},
                "note": "旧版缺失的可选字段按声明缺省规则补齐；0 仅表示缺省，不冒充有效 RTT 统计样本。",
            }
        )
        decision = "degrade_accept"
        label = "降级接受"
    else:
        decision = "accept"
        label = "接受"
    report = NegotiationReport(
        format=BINARY_FORMAT,
        version=version,
        decision=decision,
        decision_label=label,
        records_detected=len(records),
        units={"timestamp": "ms", "rtt": header["rtt_unit"], "payload": header["payload_unit"]},
        degradations=tuple(degradations),
        warnings=(),
    )
    return DecodedBundle(
        report=report,
        records=records,
        metadata={
            "endian": endian,
            "flags": header["flags"],
            "header_size": header["header_size"],
            "crc": "crc32-per-record-payload",
        },
    )


def encode_jsonl(records: Sequence[DiagnosticRecord], *, version: int = 2) -> bytes:
    if version not in (1, 2):
        raise format_unsupported("仅支持编码 JSONL 版本 1 或 2", version=version)
    lines: list[str] = []
    for record in records:
        item: dict[str, Any] = {
            "format": JSONL_FORMAT,
            "version": version,
            "timestamp_us": record.timestamp_ms * 1000,
            "probe_id": record.probe_id,
            "path_node": record.path_node,
            "protocol": record.protocol,
            "success": record.success,
            "failure_type": record.failure_type,
        }
        if version >= 2 or not record.rtt_missing:
            item["rtt_value"] = record.rtt_us
            item["rtt_unit"] = "us"
        if version >= 2 or not record.payload_missing:
            item["payload_value"] = record.payload_bytes
            item["payload_unit"] = "B"
        lines.append(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def _json_line_objects(data: bytes) -> list[dict[str, Any]]:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise frame_corrupt("JSONL 不是合法 UTF-8", reason=str(exc)) from exc
    objects: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise frame_corrupt(
                "JSONL 行解析失败",
                line=line_number,
                column=exc.colno,
                reason=exc.msg,
            ) from exc
        if not isinstance(value, dict):
            raise frame_corrupt("JSONL 每行必须是对象", line=line_number)
        objects.append(value)
    if not objects:
        raise frame_corrupt("JSONL 不包含任何记录")
    return objects


def _json_version(objects: list[dict[str, Any]]) -> int:
    versions: set[int] = set()
    for line_number, item in enumerate(objects, start=1):
        fmt = item.get("format")
        if fmt is not None and fmt != JSONL_FORMAT:
            raise format_unsupported(
                "JSONL 格式标识不匹配",
                line=line_number,
                format=fmt,
                expected=JSONL_FORMAT,
            )
        version = item.get("version")
        if not isinstance(version, int) or isinstance(version, bool):
            raise frame_corrupt("JSONL 记录缺少整数 version", line=line_number)
        versions.add(version)
    if len(versions) != 1:
        raise format_unsupported(
            "同一 JSONL 文件混用了多个格式版本",
            versions=sorted(versions),
            decision="reject",
        )
    version = next(iter(versions))
    if version > MAX_SUPPORTED_VERSION:
        raise format_unsupported(
            "JSONL 格式版本高于本工作台支持的版本",
            version=version,
            max_supported_version=MAX_SUPPORTED_VERSION,
            decision="reject",
            compatibility_matrix=COMPATIBILITY_MATRIX,
        )
    if version < 1:
        raise format_unsupported("JSONL 格式版本 0 不受支持", version=version)
    return version


def _json_int(item: dict[str, Any], key: str, line: int, *, required: bool = True) -> int | None:
    if key not in item or item[key] is None:
        if required:
            raise frame_corrupt("JSONL 记录缺少必填字段", line=line, field=key)
        return None
    value = item[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise frame_corrupt("JSONL 数值字段必须为整数", line=line, field=key, value=value)
    return value


def _json_text(item: dict[str, Any], key: str, line: int, *, required: bool = True) -> str | None:
    if key not in item or item[key] is None:
        if required:
            raise frame_corrupt("JSONL 记录缺少必填字段", line=line, field=key)
        return None
    value = item[key]
    if not isinstance(value, str):
        raise frame_corrupt("JSONL 文本字段必须为字符串", line=line, field=key)
    return value


def _decode_jsonl(data: bytes) -> DecodedBundle:
    objects = _json_line_objects(data)
    version = _json_version(objects)

    supported_required_features = {"rtt", "payload", "failure_type"}
    for line_number, item in enumerate(objects, start=1):
        required_features = item.get("required_features", [])
        if required_features is None:
            required_features = []
        if not isinstance(required_features, list) or not all(
            isinstance(feature, str) for feature in required_features
        ):
            raise frame_corrupt("required_features 必须为字符串数组", line=line_number)
        unknown = sorted(set(required_features) - supported_required_features)
        if unknown:
            raise format_unsupported(
                "JSONL 声明了未知必填字段",
                line=line_number,
                unknown_required_features=unknown,
                decision="reject",
            )

    records: list[RawRecord] = []
    units_rtt: Counter[str] = Counter()
    units_payload: Counter[str] = Counter()
    for line_number, item in enumerate(objects, start=1):
        timestamp_us = _json_int(item, "timestamp_us", line_number)
        probe_id = _json_text(item, "probe_id", line_number)
        path_node = _json_text(item, "path_node", line_number)
        protocol = _json_text(item, "protocol", line_number)
        success = item.get("success")
        if not isinstance(success, bool):
            raise frame_corrupt("JSONL success 必须为布尔值", line=line_number)
        failure_type = _json_text(item, "failure_type", line_number, required=version >= 2)
        if failure_type is None:
            failure_type = "none"
        rtt_value = _json_int(item, "rtt_value", line_number, required=version >= 2)
        payload_value = _json_int(
            item, "payload_value", line_number, required=version >= 2
        )
        rtt_unit = _json_text(item, "rtt_unit", line_number, required=version >= 2)
        payload_unit = _json_text(
            item, "payload_unit", line_number, required=version >= 2
        )
        if rtt_value is not None and rtt_unit is None:
            raise frame_corrupt("JSONL 存在 RTT 值时缺少 rtt_unit", line=line_number)
        if payload_value is not None and payload_unit is None:
            raise frame_corrupt("JSONL 存在载荷值时缺少 payload_unit", line=line_number)
        if rtt_value is not None:
            units_rtt[str(rtt_unit)] += 1
        if payload_value is not None:
            units_payload[str(payload_unit)] += 1
        records.append(
            RawRecord(
                timestamp_value=int(timestamp_us),
                timestamp_unit="us",
                probe_id=str(probe_id),
                path_node=str(path_node),
                protocol=str(protocol),
                success=success,
                rtt_value=rtt_value,
                rtt_unit=str(rtt_unit or "us"),
                payload_value=payload_value,
                payload_unit=str(payload_unit or "B"),
                failure_type=str(failure_type),
            )
        )

    def unit_label(counter: Counter[str], fallback: str) -> str:
        if not counter:
            return fallback
        if len(counter) == 1:
            return next(iter(counter))
        return "mixed:" + "|".join(sorted(counter))

    degradations: list[dict[str, Any]] = []
    if version == 1:
        missing_rtt = [i for i, record in enumerate(records) if record.rtt_value is None]
        missing_payload = [i for i, record in enumerate(records) if record.payload_value is None]
        degradations.append(
            {
                "kind": "version_policy",
                "version": 1,
                "missing_rtt_records": missing_rtt,
                "missing_payload_records": missing_payload,
                "defaults": {"rtt_us": 0, "payload_bytes": 0},
                "note": "旧版缺失的可选字段按声明缺省规则补齐；0 仅表示缺省，不冒充有效 RTT 统计样本。",
            }
        )
        decision = "degrade_accept"
        label = "降级接受"
    else:
        decision = "accept"
        label = "接受"
    report = NegotiationReport(
        format=JSONL_FORMAT,
        version=version,
        decision=decision,
        decision_label=label,
        records_detected=len(records),
        units={
            "timestamp": "us",
            "rtt": unit_label(units_rtt, "us"),
            "payload": unit_label(units_payload, "B"),
        },
        degradations=tuple(degradations),
        warnings=(),
    )
    return DecodedBundle(report=report, records=records, metadata={"lines": len(records)})


def detect_format(data: bytes) -> str:
    if not isinstance(data, (bytes, bytearray)) or len(data) == 0:
        raise bad_request("诊断包内容为空")
    raw = bytes(data)
    if raw.startswith(MAGIC):
        return BINARY_FORMAT
    stripped = raw.lstrip(b"\xef\xbb\xbf \t\r\n")
    if stripped.startswith(b"{"):
        return JSONL_FORMAT
    if stripped.startswith(b"["):
        raise format_unsupported(
            "不接受 JSON 数组；JSONL 格式要求每行一条诊断记录",
            decision="reject",
        )
    raise format_unsupported(
        "无法识别诊断包格式",
        expected_magic=MAGIC.decode("ascii"),
        first_bytes=raw[:8].hex(),
        decision="reject",
    )


def decode_bundle(data: bytes) -> DecodedBundle:
    """Detect, negotiate and fully validate/decode a diagnostic package."""

    fmt = detect_format(data)
    if fmt == BINARY_FORMAT:
        return _decode_binary(bytes(data))
    return _decode_jsonl(bytes(data))


def compatibility_matrix() -> dict[str, Any]:
    return COMPATIBILITY_MATRIX


def canonical_fingerprint_payload(data: bytes, options: dict[str, Any]) -> bytes:
    """Stable bytes used by callers to fingerprint an idempotent batch."""

    header = json.dumps(options, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return struct.pack(">I", len(header)) + header + bytes(data)


def decoded_records_for_export(records: Iterable[DiagnosticRecord]) -> list[DiagnosticRecord]:
    """Small helper preserving deterministic ordinal ordering for exporters."""

    return sorted(records, key=lambda record: (record.timestamp_ms, record.ordinal))
