from __future__ import annotations

import json
import struct
import zlib
from typing import Any

from ..errors import E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED, BusinessError, corrupt
from ..models import (
    ProbeResultRecord,
    RegistrationRecord,
    Snapshot,
)


MAGIC = b"SRMW"
SUPPORTED_VERSIONS = (1, 2)
ENDIAN_LITTLE = 1
ENDIAN_BIG = 2
ENDIAN_NAMES = {ENDIAN_LITTLE: "little", ENDIAN_BIG: "big"}
ENDIAN_MARKERS = {value: key for key, value in ENDIAN_NAMES.items()}
KIND_TO_BYTE = {"registration": 1, "probe_result": 2}
BYTE_TO_KIND = {value: key for key, value in KIND_TO_BYTE.items()}
METHOD_TO_BYTE = {"": 0, "http": 1, "tcp": 2, "lease": 3}
BYTE_TO_METHOD = {value: key for key, value in METHOD_TO_BYTE.items()}

HEADER_FIXED = struct.Struct(">4sBBBB")
MAX_RECORDS = 1_000_000
MAX_RECORD_BYTES = 16 * 1024 * 1024

FLAG_UNKNOWN_REQUIRED = 0x01


def _endian_prefix(endianness: str) -> str:
    if endianness == "little":
        return "<"
    if endianness == "big":
        return ">"
    raise BusinessError(
        E_FORMAT_UNSUPPORTED,
        "binary endianness marker must be little or big",
        details={"endianness": endianness},
    )


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _encode_string(value: str, prefix: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFFFF:
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "string field is too long for binary format",
            details={"length": len(encoded)},
        )
    return struct.pack(prefix + "H", len(encoded)) + encoded


class _PayloadWriter:
    def __init__(self, endianness: str) -> None:
        self.prefix = _endian_prefix(endianness)
        self.parts: list[bytes] = []

    def u8(self, value: int) -> None:
        self.parts.append(struct.pack(self.prefix + "B", value))

    def u16(self, value: int) -> None:
        self.parts.append(struct.pack(self.prefix + "H", value))

    def u32(self, value: int) -> None:
        self.parts.append(struct.pack(self.prefix + "I", value))

    def u64(self, value: int) -> None:
        self.parts.append(struct.pack(self.prefix + "Q", value))

    def f32(self, value: float) -> None:
        self.parts.append(struct.pack(self.prefix + "f", value))

    def string(self, value: str) -> None:
        self.parts.append(_encode_string(value, self.prefix))

    def blob(self, value: bytes) -> None:
        self.parts.append(struct.pack(self.prefix + "I", len(value)) + value)

    def build(self) -> bytes:
        return b"".join(self.parts)


class _PayloadReader:
    def __init__(self, payload: bytes, endianness: str, record_index: int) -> None:
        self.payload = payload
        self.offset = 0
        self.prefix = _endian_prefix(endianness)
        self.record_index = record_index

    def _take(self, length: int) -> bytes:
        end = self.offset + length
        if end > len(self.payload):
            raise corrupt(self.record_index, "record payload ended unexpectedly")
        value = self.payload[self.offset:end]
        self.offset = end
        return value

    def u8(self) -> int:
        return struct.unpack(self.prefix + "B", self._take(1))[0]

    def u16(self) -> int:
        return struct.unpack(self.prefix + "H", self._take(2))[0]

    def u32(self) -> int:
        return struct.unpack(self.prefix + "I", self._take(4))[0]

    def u64(self) -> int:
        return struct.unpack(self.prefix + "Q", self._take(8))[0]

    def f32(self) -> float:
        return struct.unpack(self.prefix + "f", self._take(4))[0]

    def string(self) -> str:
        length = struct.unpack(self.prefix + "H", self._take(2))[0]
        try:
            return self._take(length).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise corrupt(self.record_index, "string field is not valid UTF-8") from exc

    def blob(self) -> bytes:
        return self._take(self.u32())

    def json_object(self) -> dict[str, Any]:
        raw = self.blob()
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise corrupt(self.record_index, "JSON field is invalid") from exc
        if not isinstance(value, dict):
            raise corrupt(self.record_index, "JSON field must be an object")
        return value

    def ensure_finished(self) -> None:
        if self.offset != len(self.payload):
            raise corrupt(
                self.record_index,
                "record payload contains trailing bytes",
                trailing_bytes=len(self.payload) - self.offset,
            )


def _encode_registration(record: RegistrationRecord, version: int, endianness: str) -> bytes:
    if record.time_unit != "ms":
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "binary registration encoding requires timestamps normalized to milliseconds",
            details={"service_name": record.service_name, "instance_id": record.instance_id, "time_unit": record.time_unit},
        )
    bitmap = 0
    if record.probe_params is not None:
        bitmap |= 1 << 0
    if record.lease_version is not None:
        bitmap |= 1 << 1
    if record.probe_window_ms is not None:
        bitmap |= 1 << 2
    if record.last_heartbeat_at is not None:
        bitmap |= 1 << 3
    writer = _PayloadWriter(endianness)
    writer.u16(bitmap)
    writer.string(record.service_name)
    writer.string(record.instance_id)
    writer.string(record.endpoint)
    if version == 1:
        if record.weight_encoding != "ratio":
            raise BusinessError(
                E_FORMAT_CORRUPT,
                "binary v1 registration requires a ratio weight",
                details={"service_name": record.service_name, "instance_id": record.instance_id},
            )
        writer.f32(float(record.weight))
    else:
        if record.weight_encoding != "permille":
            raise BusinessError(
                E_FORMAT_CORRUPT,
                "binary v2 registration requires an integer permille weight",
                details={"service_name": record.service_name, "instance_id": record.instance_id},
            )
        writer.u16(int(record.weight))
    writer.u32(record.lease_ttl_ms)
    writer.u8(METHOD_TO_BYTE[record.probe_method])
    writer.u64(record.registered_at)
    if bitmap & (1 << 0):
        writer.blob(_json_bytes(record.probe_params or {}))
    if bitmap & (1 << 1):
        writer.u32(int(record.lease_version or 1))
    if bitmap & (1 << 2):
        writer.u32(int(record.probe_window_ms or 0))
    if bitmap & (1 << 3):
        writer.u64(int(record.last_heartbeat_at or 0))
    return writer.build()


def _encode_probe_result(record: ProbeResultRecord, version: int, endianness: str) -> bytes:
    if record.time_unit != "ms":
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "binary probe-result encoding requires timestamps normalized to milliseconds",
            details={"service_name": record.service_name, "instance_id": record.instance_id, "time_unit": record.time_unit},
        )
    bitmap = 0
    if record.probe_params is not None:
        bitmap |= 1 << 0
    if record.lease_version is not None:
        bitmap |= 1 << 1
    if record.probe_window_ms is not None:
        bitmap |= 1 << 2
    if record.error_code is not None:
        bitmap |= 1 << 3
    if record.detail is not None:
        bitmap |= 1 << 4
    writer = _PayloadWriter(endianness)
    writer.u16(bitmap)
    writer.string(record.service_name)
    writer.string(record.instance_id)
    writer.string(record.endpoint)
    if version == 1:
        writer.f32(float(record.weight))
    else:
        writer.u16(int(record.weight))
    writer.u8(METHOD_TO_BYTE[record.probe_method])
    writer.u64(record.checked_at)
    writer.u8(1 if record.success else 0)
    writer.string(record.status)
    writer.string(record.outcome)
    writer.u32(record.latency_ms)
    if bitmap & (1 << 0):
        writer.blob(_json_bytes(record.probe_params or {}))
    if bitmap & (1 << 1):
        writer.u32(int(record.lease_version or 1))
    if bitmap & (1 << 2):
        writer.u32(int(record.probe_window_ms or 0))
    if bitmap & (1 << 3):
        writer.string(record.error_code or "")
    if bitmap & (1 << 4):
        writer.string(record.detail or "")
    return writer.build()


def encode_binary(
    snapshot: Snapshot,
    *,
    version: int = 2,
    endianness: str = "little",
) -> bytes:
    if version not in SUPPORTED_VERSIONS:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary encoder only supports versions 1 and 2",
            details={"version": version, "supported": list(SUPPORTED_VERSIONS)},
        )
    if snapshot.kind not in KIND_TO_BYTE:
        raise BusinessError(E_FORMAT_CORRUPT, "unknown snapshot kind", details={"kind": snapshot.kind})
    marker = ENDIAN_MARKERS.get(endianness)
    if marker is None:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "unsupported endianness",
            details={"endianness": endianness},
        )
    if len(snapshot.records) > MAX_RECORDS:
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "record count exceeds binary format limit",
            details={"record_count": len(snapshot.records)},
        )
    created_at_ms = 0
    header = HEADER_FIXED.pack(
        MAGIC,
        version,
        marker,
        KIND_TO_BYTE[snapshot.kind],
        0,
    )
    header += struct.pack(_endian_prefix(endianness) + "I", len(snapshot.records))
    header += struct.pack(_endian_prefix(endianness) + "Q", created_at_ms)
    frames: list[bytes] = [header]
    for index, record in enumerate(snapshot.records):
        if snapshot.kind == "registration":
            if not isinstance(record, RegistrationRecord):
                raise BusinessError(E_FORMAT_CORRUPT, "record type does not match snapshot kind")
            payload = _encode_registration(record, version, endianness)
        else:
            if not isinstance(record, ProbeResultRecord):
                raise BusinessError(E_FORMAT_CORRUPT, "record type does not match snapshot kind")
            payload = _encode_probe_result(record, version, endianness)
        if len(payload) > MAX_RECORD_BYTES:
            raise BusinessError(
                E_FORMAT_CORRUPT,
                "record exceeds binary format size limit",
                details={"record_index": index, "record_bytes": len(payload)},
            )
        prefix = struct.pack(_endian_prefix(endianness) + "I", len(payload))
        crc = struct.pack(_endian_prefix(endianness) + "I", zlib.crc32(payload) & 0xFFFFFFFF)
        frames.append(prefix)
        frames.append(payload)
        frames.append(crc)
    return b"".join(frames)


def _decode_header(data: bytes) -> tuple[int, str, str, int, int]:
    if len(data) < HEADER_FIXED.size:
        raise corrupt(0, "binary header is truncated")
    magic, version, marker, kind_byte, flags = HEADER_FIXED.unpack_from(data, 0)
    if magic != MAGIC:
        raise corrupt(0, "binary magic does not match")
    if marker not in ENDIAN_NAMES:
        raise corrupt(0, "binary endianness marker is invalid", endianness_marker=marker)
    endianness = ENDIAN_NAMES[marker]
    if kind_byte not in BYTE_TO_KIND:
        raise corrupt(0, "binary snapshot kind is invalid", kind_byte=kind_byte)
    if flags & FLAG_UNKNOWN_REQUIRED:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary snapshot declares an unknown required field",
            details={"version": version, "flags": flags, "unknown_required": True},
            http_status=409,
        )
    if version > max(SUPPORTED_VERSIONS):
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary snapshot version is newer than supported",
            details={"version": version, "supported": list(SUPPORTED_VERSIONS)},
            http_status=409,
        )
    if version < 1:
        raise corrupt(0, "binary snapshot version is invalid", version=version)
    offset = HEADER_FIXED.size
    if len(data) < offset + 4:
        raise corrupt(0, "binary header is missing record count")
    record_count = struct.unpack(_endian_prefix(endianness) + "I", data[offset : offset + 4])[0]
    offset += 4
    if record_count > MAX_RECORDS:
        raise corrupt(0, "binary record count exceeds format limit", record_count=record_count)
    if len(data) < offset + 8:
        raise corrupt(0, "binary header is missing creation timestamp")
    created_at_ms = struct.unpack(_endian_prefix(endianness) + "Q", data[offset : offset + 8])[0]
    offset += 8
    return version, endianness, BYTE_TO_KIND[kind_byte], record_count, offset


def _decode_registration(
    payload: bytes,
    *,
    version: int,
    endianness: str,
    record_index: int,
) -> RegistrationRecord:
    reader = _PayloadReader(payload, endianness, record_index)
    bitmap = reader.u16()
    service_name = reader.string()
    instance_id = reader.string()
    endpoint = reader.string()
    if version == 1:
        weight: int | float = reader.f32()
        weight_encoding = "ratio"
    else:
        weight = reader.u16()
        weight_encoding = "permille"
    lease_ttl_ms = reader.u32()
    method_byte = reader.u8()
    if method_byte not in BYTE_TO_METHOD:
        raise corrupt(record_index, "probe method byte is invalid", probe_method_byte=method_byte)
    probe_method = BYTE_TO_METHOD[method_byte]
    registered_at = reader.u64()
    probe_params = None
    if bitmap & (1 << 0):
        probe_params = reader.json_object()
    lease_version = reader.u32() if bitmap & (1 << 1) else None
    probe_window_ms = reader.u32() if bitmap & (1 << 2) else None
    heartbeat = reader.u64() if bitmap & (1 << 3) else None
    reader.ensure_finished()
    return RegistrationRecord(
        service_name=service_name,
        instance_id=instance_id,
        endpoint=endpoint,
        weight=weight,
        weight_encoding=weight_encoding,
        lease_ttl_ms=lease_ttl_ms,
        probe_method=probe_method,
        probe_params=probe_params,
        registered_at=registered_at,
        time_unit="ms",
        lease_version=lease_version,
        probe_window_ms=probe_window_ms,
        last_heartbeat_at=heartbeat,
        source_version=version,
    )


def _decode_probe_result(
    payload: bytes,
    *,
    version: int,
    endianness: str,
    record_index: int,
) -> ProbeResultRecord:
    reader = _PayloadReader(payload, endianness, record_index)
    bitmap = reader.u16()
    service_name = reader.string()
    instance_id = reader.string()
    endpoint = reader.string()
    if version == 1:
        weight: int | float = reader.f32()
        weight_encoding = "ratio"
    else:
        weight = reader.u16()
        weight_encoding = "permille"
    method_byte = reader.u8()
    if method_byte not in BYTE_TO_METHOD:
        raise corrupt(record_index, "probe method byte is invalid", probe_method_byte=method_byte)
    probe_method = BYTE_TO_METHOD[method_byte]
    checked_at = reader.u64()
    success = bool(reader.u8())
    status = reader.string()
    outcome = reader.string()
    latency_ms = reader.u32()
    probe_params = reader.json_object() if bitmap & (1 << 0) else None
    lease_version = reader.u32() if bitmap & (1 << 1) else None
    probe_window_ms = reader.u32() if bitmap & (1 << 2) else None
    error_code = reader.string() if bitmap & (1 << 3) else None
    detail = reader.string() if bitmap & (1 << 4) else None
    reader.ensure_finished()
    return ProbeResultRecord(
        service_name=service_name,
        instance_id=instance_id,
        endpoint=endpoint,
        weight=weight,
        weight_encoding=weight_encoding,
        probe_method=probe_method,
        probe_params=probe_params,
        checked_at=checked_at,
        time_unit="ms",
        success=success,
        status=status,
        latency_ms=latency_ms,
        outcome=outcome,
        lease_version=lease_version,
        probe_window_ms=probe_window_ms,
        error_code=error_code,
        detail=detail,
        source_version=version,
    )


def decode_binary(data: bytes) -> Snapshot:
    version, endianness, kind, record_count, offset = _decode_header(data)
    records: list[RegistrationRecord | ProbeResultRecord] = []
    prefix = _endian_prefix(endianness)
    for record_index in range(record_count):
        if offset + 4 > len(data):
            raise corrupt(record_index, "record length prefix is truncated")
        length = struct.unpack(prefix + "I", data[offset : offset + 4])[0]
        offset += 4
        if length > MAX_RECORD_BYTES:
            raise corrupt(
                record_index,
                "record length exceeds format limit",
                declared_length=length,
            )
        if offset + length > len(data):
            raise corrupt(
                record_index,
                "record payload is shorter than its length prefix",
                declared_length=length,
                available=len(data) - offset,
            )
        payload = data[offset : offset + length]
        offset += length
        if offset + 4 > len(data):
            raise corrupt(record_index, "record CRC is truncated")
        expected_crc = struct.unpack(prefix + "I", data[offset : offset + 4])[0]
        offset += 4
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise corrupt(
                record_index,
                "record CRC32 mismatch",
                expected_crc=f"{expected_crc:08x}",
                actual_crc=f"{actual_crc:08x}",
            )
        try:
            if kind == "registration":
                record = _decode_registration(
                    payload,
                    version=version,
                    endianness=endianness,
                    record_index=record_index,
                )
            else:
                record = _decode_probe_result(
                    payload,
                    version=version,
                    endianness=endianness,
                    record_index=record_index,
                )
        except BusinessError:
            raise
        except Exception as exc:
            raise corrupt(record_index, f"record payload could not be decoded: {exc}") from exc
        records.append(record)
    if offset != len(data):
        raise corrupt(record_count, "trailing bytes after declared records", trailing_bytes=len(data) - offset)
    return Snapshot(
        kind=kind,
        version=version,
        records=tuple(records),
        endianness=endianness,
        source_format="binary",
    )
