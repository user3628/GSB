from __future__ import annotations

import hashlib
import json
import struct
import zlib
from dataclasses import dataclass
from typing import Any, Iterable

from .compat import (
    COMPACT_MAX_BATCH_RECORDS,
    COMPACT_MAX_BODY_BYTES,
    FORMAT_COMPACT,
    FORMAT_JSONL,
    JSONL_MAX_BATCH_RECORDS,
    JSONL_MAX_BODY_BYTES,
    decide_compatibility,
)
from .errors import (
    E_BATCH_SPLIT_REQUIRED,
    E_FORMAT_INVALID,
    E_FORMAT_MISMATCH,
    E_FORMAT_UNKNOWN,
    E_RECORD_TOO_LARGE,
    MQError,
)
from .models import MessageRecord, MessageState, ParsedBatch, QueueMeta
from .timeutil import SubMsPolicy, check_policy, ms_to_us, normalize_us, us_to_ms_floor

BINARY_MAGIC = b"MSMQ"
BINARY_HEADER = struct.Struct("<4sBBHII")
BINARY_RECORD_ENVELOPE = struct.Struct("<HHI")
BINARY_RECORD_PREFIX = struct.Struct("<I")
BINARY_CURRENT_VERSION = 2
MAX_BINARY_RECORD_BYTES = 1_048_576
MAX_JSONL_LINE_BYTES = 65_536
DEFAULT_TTL_US = 60_000_000

# Record field presence bitmap.  Fields 0..3 are mandatory.  Everything else
# has a declared default in the compatibility matrix.
F_ID = 1 << 0
F_QUEUE = 1 << 1
F_BODY = 1 << 2
F_PRODUCED = 1 << 3
F_EXPIRES = 1 << 4
F_CONTENT_TYPE = 1 << 5
F_STATE = 1 << 6
F_RECORD_VERSION = 1 << 7
F_LAST_WRITE = 1 << 8
F_DELIVERY_COUNT = 1 << 9
F_EXTENSIONS = 1 << 10
F_LEASE = 1 << 11
REQUIRED_BITS = F_ID | F_QUEUE | F_BODY | F_PRODUCED
V1_ALLOWED_BITS = REQUIRED_BITS
V2_ALLOWED_BITS = REQUIRED_BITS | F_EXPIRES | F_CONTENT_TYPE | F_STATE | F_RECORD_VERSION | F_LAST_WRITE | F_DELIVERY_COUNT | F_EXTENSIONS | F_LEASE

QUEUE_SEMANTICS = {"at_least_once", "exactly_once"}
MESSAGE_STATES = {"pending", "leased", "acked", "expired"}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _Reader:
    def __init__(self, data: bytes, *, index: int) -> None:
        self.data = data
        self.offset = 0
        self.index = index

    def take(self, size: int, field: str) -> bytes:
        if size < 0 or self.offset + size > len(self.data):
            raise MQError(
                E_FORMAT_INVALID,
                "record payload ended before a declared field",
                http_status=422,
                details={"index": self.index, "field": field},
            )
        value = self.data[self.offset : self.offset + size]
        self.offset += size
        return value

    def u8(self, field: str) -> int:
        return self.take(1, field)[0]

    def u16(self, field: str) -> int:
        return struct.unpack("<H", self.take(2, field))[0]

    def u32(self, field: str) -> int:
        return struct.unpack("<I", self.take(4, field))[0]

    def u64(self, field: str) -> int:
        return struct.unpack("<Q", self.take(8, field))[0]

    def text(self, field: str) -> str:
        length = self.u32(f"{field}.length")
        raw = self.take(length, field)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MQError(
                E_FORMAT_INVALID,
                "text field is not valid UTF-8",
                http_status=422,
                details={"index": self.index, "field": field, "offset": exc.start},
            ) from exc

    def finish(self) -> None:
        if self.offset != len(self.data):
            raise MQError(
                E_FORMAT_INVALID,
                "record has trailing undeclared bytes",
                http_status=422,
                details={"index": self.index, "consumed": self.offset, "payload_bytes": len(self.data)},
            )


def _add_fill(report: dict[str, Any], index: int, field: str, default: Any, *, reason: str) -> None:
    report["fields_filled"].append(
        {"index": index, "field": field, "default": default, "reason": reason}
    )


def _add_normalization(
    report: dict[str, Any], index: int, field: str, original_us: int, normalized_us: int, policy: str
) -> None:
    report["sub_ms_normalizations"].append(
        {
            "index": index,
            "field": field,
            "original_us": original_us,
            "normalized_us": normalized_us,
            "truncated_us": original_us - normalized_us,
            "policy": policy,
        }
    )


def _validate_queue_metadata(
    raw: dict[str, Any], *, index: int, report: dict[str, Any]
) -> QueueMeta:
    known = {"name", "semantics", "visibility_timeout_ms", "version", "extensions"}
    name = raw.get("name")
    if not isinstance(name, str) or not name or len(name) > 128:
        raise MQError(
            E_FORMAT_INVALID,
            "queue metadata requires a valid name",
            http_status=422,
            details={"queue_index": index},
        )
    semantics = raw.get("semantics")
    if semantics is None:
        semantics = "at_least_once"
        _add_fill(report, -1, f"queues[{index}].semantics", semantics, reason="missing_optional")
    if semantics not in QUEUE_SEMANTICS:
        raise MQError(
            E_FORMAT_INVALID,
            "unknown queue delivery semantics",
            http_status=422,
            details={"queue_index": index, "name": name, "semantics": semantics},
        )
    visibility = raw.get("visibility_timeout_ms", 30_000)
    version = raw.get("version", 1)
    if not isinstance(visibility, int) or not (1 <= visibility <= 3_600_000):
        raise MQError(
            E_FORMAT_INVALID,
            "queue visibility timeout must be within 1..3600000ms",
            http_status=422,
            details={"queue_index": index, "name": name, "visibility_timeout_ms": visibility},
        )
    if not isinstance(version, int) or version < 1:
        raise MQError(
            E_FORMAT_INVALID,
            "queue version must be a positive integer",
            http_status=422,
            details={"queue_index": index, "name": name, "version": version},
        )
    extensions = raw.get("extensions", {})
    if not isinstance(extensions, dict):
        raise MQError(
            E_FORMAT_INVALID,
            "queue extensions must be a JSON object",
            http_status=422,
            details={"queue_index": index, "name": name},
        )
    unknown = {key: value for key, value in raw.items() if key not in known}
    if unknown:
        extensions = {**extensions, **unknown}
        report["unknown_optional_keys"].extend(f"queue:{name}:{key}" for key in sorted(unknown))
    return QueueMeta(
        name=name,
        semantics=semantics,  # type: ignore[arg-type]
        visibility_timeout_ms=visibility,
        version=version,
        extensions=extensions,
    )


def _load_metadata(payload: bytes, *, report: dict[str, Any]) -> tuple[list[QueueMeta], dict[str, Any]]:
    try:
        metadata = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MQError(
            E_FORMAT_INVALID,
            "binary metadata is not valid UTF-8 JSON",
            http_status=422,
            details={"error": str(exc)},
        ) from exc
    if not isinstance(metadata, dict):
        raise MQError(E_FORMAT_INVALID, "binary metadata root must be an object", http_status=422)
    raw_queues = metadata.get("queues", [])
    if not isinstance(raw_queues, list):
        raise MQError(E_FORMAT_INVALID, "binary metadata queues must be an array", http_status=422)
    queues = [_validate_queue_metadata(item, index=i, report=report) for i, item in enumerate(raw_queues)]
    if len({q.name for q in queues}) != len(queues):
        raise MQError(E_FORMAT_INVALID, "duplicate queue metadata name", http_status=422)
    known = {"queues", "extensions"}
    batch_extensions = metadata.get("extensions", {})
    if not isinstance(batch_extensions, dict):
        raise MQError(E_FORMAT_INVALID, "batch extensions must be a JSON object", http_status=422)
    unknown = {key: value for key, value in metadata.items() if key not in known}
    batch_extensions = {**batch_extensions, **unknown}
    report["batch_extensions"] = batch_extensions
    return queues, batch_extensions


def _encode_text(value: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFFFFFF:
        raise MQError(E_RECORD_TOO_LARGE, "text field exceeds uint32 length prefix", http_status=413)
    return struct.pack("<I", len(raw)) + raw


def _encode_json_field(value: dict[str, Any]) -> bytes:
    return _encode_text(canonical_json(value))


def _encode_compact_record(message: MessageRecord, *, version: int, include_optional: bool) -> bytes:
    bitmap = REQUIRED_BITS
    payload_parts: list[bytes] = [
        _encode_text(message.message_id),
        _encode_text(message.queue),
        _encode_text(message.body),
        struct.pack("<Q", message.produced_at_us),
    ]
    if include_optional and version >= 2:
        if message.expires_at_us is not None:
            bitmap |= F_EXPIRES
            payload_parts.append(struct.pack("<Q", message.expires_at_us))
        bitmap |= F_CONTENT_TYPE
        payload_parts.append(_encode_text(message.content_type))
        bitmap |= F_STATE
        payload_parts.append(struct.pack("<B", _state_code(message.state)))
        bitmap |= F_RECORD_VERSION
        payload_parts.append(struct.pack("<I", message.record_version))
        bitmap |= F_LAST_WRITE
        payload_parts.append(struct.pack("<Q", int(message.last_write_at_us or message.produced_at_us)))
        bitmap |= F_DELIVERY_COUNT
        payload_parts.append(struct.pack("<I", message.delivery_count))
        if message.extensions:
            bitmap |= F_EXTENSIONS
            payload_parts.append(_encode_json_field(message.extensions))
        if message.state == "leased" or message.lease_token is not None:
            bitmap |= F_LEASE
            payload_parts.append(
                _encode_json_field(
                    {
                        "owner": message.lease_owner,
                        "token": message.lease_token,
                        "expires_at_us": message.lease_expires_at_us,
                    }
                )
            )
    payload = b"".join(payload_parts)
    envelope = BINARY_RECORD_ENVELOPE.pack(bitmap, version, zlib.crc32(payload) & 0xFFFFFFFF)
    return BINARY_RECORD_PREFIX.pack(len(envelope) + len(payload)) + envelope + payload


def _state_code(state: str) -> int:
    mapping = {"pending": 0, "leased": 1, "acked": 2, "expired": 3}
    try:
        return mapping[state]
    except KeyError as exc:
        raise MQError(E_FORMAT_INVALID, "unknown message state", http_status=422, details={"state": state}) from exc


def _state_from_code(code: int, *, index: int) -> MessageState:
    mapping = {0: "pending", 1: "leased", 2: "acked", 3: "expired"}
    if code not in mapping:
        raise MQError(
            E_FORMAT_INVALID,
            "unknown message state code",
            http_status=422,
            details={"index": index, "state_code": code},
        )
    return mapping[code]  # type: ignore[return-value]


def encode_compact(
    messages: Iterable[MessageRecord],
    queues: Iterable[QueueMeta],
    *,
    version: int = BINARY_CURRENT_VERSION,
    batch_extensions: dict[str, Any] | None = None,
) -> bytes:
    records = list(messages)
    queue_list = list(queues)
    if version not in {1, 2}:
        raise MQError(
            E_FORMAT_MISMATCH,
            "this encoder only emits supported compact versions 1 and 2",
            http_status=422,
            details={"version": version},
        )
    if len(records) > COMPACT_MAX_BATCH_RECORDS:
        raise MQError(
            E_BATCH_SPLIT_REQUIRED,
            "compact batch exceeds the format record limit",
            http_status=413,
            details={"count": len(records), "max_records": COMPACT_MAX_BATCH_RECORDS, "format": FORMAT_COMPACT},
        )
    for index, message in enumerate(records):
        body_size = len(message.body_bytes)
        if body_size > COMPACT_MAX_BODY_BYTES:
            raise MQError(
                E_RECORD_TOO_LARGE,
                "message body exceeds compact format capacity",
                http_status=413,
                details={
                    "index": index,
                    "message_id": message.message_id,
                    "body_bytes": body_size,
                    "max_body_bytes": COMPACT_MAX_BODY_BYTES,
                    "format": FORMAT_COMPACT,
                },
            )
    metadata = {
        "queues": [queue.to_dict() for queue in queue_list],
        "extensions": batch_extensions or {},
    }
    metadata_bytes = canonical_json(metadata).encode("utf-8")
    header = BINARY_HEADER.pack(
        BINARY_MAGIC, version, 0, 0, len(metadata_bytes), len(records)
    )
    encoded = bytearray(header + metadata_bytes)
    include_optional = version >= 2
    for message in records:
        record = _encode_compact_record(message, version=version, include_optional=include_optional)
        if len(record) - BINARY_RECORD_PREFIX.size > MAX_BINARY_RECORD_BYTES:
            raise MQError(
                E_RECORD_TOO_LARGE,
                "encoded compact record exceeds the safety limit",
                http_status=413,
                details={"message_id": message.message_id, "record_bytes": len(record)},
            )
        encoded.extend(record)
    return bytes(encoded)


def _decode_compact(data: bytes, *, sub_ms_policy: SubMsPolicy) -> ParsedBatch:
    if len(data) < BINARY_HEADER.size:
        raise MQError(E_FORMAT_INVALID, "compact input is shorter than its header", http_status=422)
    magic, version, flags, reserved, metadata_len, record_count = BINARY_HEADER.unpack_from(data, 0)
    if magic != BINARY_MAGIC:
        raise MQError(E_FORMAT_INVALID, "compact magic does not match MSMQ", http_status=422)
    decision, compat = decide_compatibility(
        FORMAT_COMPACT,
        version,
        has_unknown_required_extension=bool(flags & 0x01),
    )
    if record_count > COMPACT_MAX_BATCH_RECORDS:
        raise MQError(
            E_BATCH_SPLIT_REQUIRED,
            "compact batch must be split before import",
            http_status=413,
            details={
                "count": record_count,
                "max_records": COMPACT_MAX_BATCH_RECORDS,
                "format": FORMAT_COMPACT,
                "decision": "reject",
                "state_changed": False,
            },
        )
    offset = BINARY_HEADER.size
    if offset + metadata_len > len(data):
        raise MQError(E_FORMAT_INVALID, "truncated compact metadata", http_status=422)
    report: dict[str, Any] = {
        "format": FORMAT_COMPACT,
        "version": version,
        "decision": decision,
        "compatibility": compat,
        "fields_filled": [],
        "sub_ms_normalizations": [],
        "unknown_optional_keys": [],
        "record_extensions_preserved": 0,
        "header_flags": flags,
        "header_reserved": reserved,
        "record_count_declared": record_count,
    }
    metadata_bytes = data[offset : offset + metadata_len]
    offset += metadata_len
    queues, batch_extensions = _load_metadata(metadata_bytes, report=report)
    queue_names = {queue.name for queue in queues}
    messages: list[MessageRecord] = []
    for index in range(record_count):
        if offset + BINARY_RECORD_PREFIX.size > len(data):
            raise MQError(
                E_FORMAT_INVALID,
                "truncated compact record length prefix",
                http_status=422,
                details={"index": index},
            )
        (record_len,) = BINARY_RECORD_PREFIX.unpack_from(data, offset)
        offset += BINARY_RECORD_PREFIX.size
        if record_len < BINARY_RECORD_ENVELOPE.size or record_len > MAX_BINARY_RECORD_BYTES:
            raise MQError(
                E_FORMAT_INVALID,
                "compact record length is outside allowed bounds",
                http_status=422,
                details={"index": index, "record_len": record_len, "max_record_bytes": MAX_BINARY_RECORD_BYTES},
            )
        if offset + record_len > len(data):
            raise MQError(
                E_FORMAT_INVALID,
                "truncated compact record payload",
                http_status=422,
                details={"index": index, "record_len": record_len},
            )
        envelope = data[offset : offset + BINARY_RECORD_ENVELOPE.size]
        bitmap, schema_version, expected_crc = BINARY_RECORD_ENVELOPE.unpack(envelope)
        payload = data[
            offset + BINARY_RECORD_ENVELOPE.size : offset + record_len
        ]
        offset += record_len
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise MQError(
                E_FORMAT_INVALID,
                "compact record CRC32 mismatch",
                http_status=422,
                details={
                    "index": index,
                    "expected_crc32": f"{expected_crc:08x}",
                    "actual_crc32": f"{actual_crc:08x}",
                },
            )
        allowed = V1_ALLOWED_BITS if version == 1 else V2_ALLOWED_BITS
        unknown_bits = bitmap & ~allowed
        if unknown_bits:
            raise MQError(
                E_FORMAT_INVALID,
                "record contains unsupported optional bitmap bits",
                http_status=422,
                details={"index": index, "unknown_bitmap_bits": unknown_bits},
            )
        missing = REQUIRED_BITS & ~bitmap
        if missing:
            raise MQError(
                E_FORMAT_INVALID,
                "record is missing a required bitmap field",
                http_status=422,
                details={"index": index, "missing_bitmap_bits": missing},
            )
        reader = _Reader(payload, index=index)
        message_id = reader.text("message_id")
        queue = reader.text("queue")
        body = reader.text("body")
        body_bytes = len(body.encode("utf-8"))
        if body_bytes > COMPACT_MAX_BODY_BYTES:
            raise MQError(
                E_RECORD_TOO_LARGE,
                "message body exceeds compact format capacity",
                http_status=413,
                details={
                    "index": index,
                    "message_id": message_id,
                    "body_bytes": body_bytes,
                    "max_body_bytes": COMPACT_MAX_BODY_BYTES,
                },
            )
        produced_us = reader.u64("produced_at_us")
        original_produced_us = produced_us
        produced_us, changed = normalize_us(
            produced_us, sub_ms_policy, field="produced_at_us", index=index
        )
        if changed:
            _add_normalization(
                report, index, "produced_at_us", original_produced_us, produced_us, sub_ms_policy
            )
        expires_us: int
        if bitmap & F_EXPIRES:
            expires_us = reader.u64("expires_at_us")
            original_expires = expires_us
            expires_us, changed = normalize_us(
                expires_us, sub_ms_policy, field="expires_at_us", index=index
            )
            if changed:
                _add_normalization(report, index, "expires_at_us", original_expires, expires_us, sub_ms_policy)
        else:
            expires_us = produced_us + DEFAULT_TTL_US
            _add_fill(
                report,
                index,
                "expires_at_us",
                expires_us,
                reason="missing_in_v1" if version == 1 else "missing_optional",
            )
        if bitmap & F_CONTENT_TYPE:
            content_type = reader.text("content_type")
        else:
            content_type = "text/plain"
            _add_fill(report, index, "content_type", content_type, reason="missing_optional")
        if bitmap & F_STATE:
            state = _state_from_code(reader.u8("state"), index=index)
        else:
            state = "pending"
            _add_fill(report, index, "state", state, reason="missing_optional")
        if bitmap & F_RECORD_VERSION:
            record_version = reader.u32("record_version")
        else:
            record_version = 1
            _add_fill(report, index, "record_version", record_version, reason="missing_optional")
        if bitmap & F_LAST_WRITE:
            last_write_us = reader.u64("last_write_at_us")
            original_last_write = last_write_us
            last_write_us, changed = normalize_us(
                last_write_us, sub_ms_policy, field="last_write_at_us", index=index
            )
            if changed:
                _add_normalization(
                    report, index, "last_write_at_us", original_last_write, last_write_us, sub_ms_policy
                )
        else:
            last_write_us = produced_us
            _add_fill(report, index, "last_write_at_us", last_write_us, reason="missing_optional")
        if bitmap & F_DELIVERY_COUNT:
            delivery_count = reader.u32("delivery_count")
        else:
            delivery_count = 0
            _add_fill(report, index, "delivery_count", delivery_count, reason="missing_optional")
        extensions: dict[str, Any] = {}
        if bitmap & F_EXTENSIONS:
            raw = reader.text("extensions")
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise MQError(
                    E_FORMAT_INVALID,
                    "record extensions are not valid JSON",
                    http_status=422,
                    details={"index": index},
                ) from exc
            if not isinstance(parsed, dict):
                raise MQError(
                    E_FORMAT_INVALID,
                    "record extensions must be a JSON object",
                    http_status=422,
                    details={"index": index},
                )
            extensions = parsed
        lease_owner = lease_token = None
        lease_expires_us = None
        if bitmap & F_LEASE:
            raw = reader.text("lease")
            try:
                parsed_lease = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise MQError(
                    E_FORMAT_INVALID,
                    "lease field is not valid JSON",
                    http_status=422,
                    details={"index": index},
                ) from exc
            if not isinstance(parsed_lease, dict):
                raise MQError(
                    E_FORMAT_INVALID,
                    "lease field must be a JSON object",
                    http_status=422,
                    details={"index": index},
                )
            lease_owner = parsed_lease.get("owner")
            lease_token = parsed_lease.get("token")
            lease_expires_us = parsed_lease.get("expires_at_us")
            if lease_expires_us is not None:
                lease_expires_us = int(lease_expires_us)
                normalized, changed = normalize_us(
                    lease_expires_us, sub_ms_policy, field="lease_expires_at_us", index=index
                )
                if changed:
                    _add_normalization(
                        report,
                        index,
                        "lease_expires_at_us",
                        lease_expires_us,
                        normalized,
                        sub_ms_policy,
                    )
                lease_expires_us = normalized
        reader.finish()
        if queue not in queue_names:
            raise MQError(
                E_FORMAT_INVALID,
                "record references a queue absent from compact metadata",
                http_status=422,
                details={"index": index, "queue": queue},
            )
        if extensions:
            report["record_extensions_preserved"] += 1
            report["unknown_optional_keys"].extend(
                f"record:{index}:{key}" for key in sorted(extensions)
            )
        messages.append(
            MessageRecord(
                message_id=message_id,
                queue=queue,
                body=body,
                produced_at_us=produced_us,
                expires_at_us=expires_us,
                content_type=content_type,
                state=state,
                record_version=record_version,
                last_write_at_us=last_write_us,
                delivery_count=delivery_count,
                extensions=extensions,
                lease_owner=lease_owner,
                lease_token=lease_token,
                lease_expires_at_us=lease_expires_us,
                source_format=FORMAT_COMPACT,
                source_version=version,
            )
        )
    if offset != len(data):
        raise MQError(
            E_FORMAT_INVALID,
            "compact input has trailing bytes after the declared record count",
            http_status=422,
            details={"index": record_count, "trailing_bytes": len(data) - offset},
        )
    report["records_total"] = len(messages)
    report["queues_total"] = len(queues)
    report["batch_extensions"] = batch_extensions
    report["degraded_fields"] = [
        item for item in report["fields_filled"] if item["reason"] == "missing_in_v1"
    ] if decision == "degrade_accept" else []
    report["degradations"] = report["degraded_fields"]
    return ParsedBatch(
        format=FORMAT_COMPACT,
        version=version,
        decision=decision,
        queues=queues,
        messages=messages,
        decode_report=report,
        raw_size=len(data),
        sha256=sha256_bytes(data),
    )


def _jsonl_manifest(queues: list[QueueMeta], *, version: int, batch_extensions: dict[str, Any]) -> dict[str, Any]:
    return {
        "_type": "mq_manifest",
        "format": FORMAT_JSONL,
        "version": version,
        "queues": [queue.to_dict() for queue in queues],
        "extensions": batch_extensions,
    }


def encode_jsonl(
    messages: Iterable[MessageRecord],
    queues: Iterable[QueueMeta],
    *,
    version: int = 1,
    batch_extensions: dict[str, Any] | None = None,
) -> bytes:
    records = list(messages)
    queue_list = list(queues)
    if version != 1:
        raise MQError(
            E_FORMAT_MISMATCH,
            "this encoder only emits supported JSONL version 1",
            http_status=422,
            details={"version": version},
        )
    if len(records) > JSONL_MAX_BATCH_RECORDS:
        raise MQError(
            E_BATCH_SPLIT_REQUIRED,
            "JSONL batch exceeds the format record limit",
            http_status=413,
            details={"count": len(records), "max_records": JSONL_MAX_BATCH_RECORDS, "format": FORMAT_JSONL},
        )
    lines = [canonical_json(_jsonl_manifest(queue_list, version=version, batch_extensions=batch_extensions or {}))]
    reserved = {
        "id",
        "queue",
        "body",
        "produced_at_ms",
        "ttl_ms",
        "content_type",
        "state",
        "record_version",
        "last_write_at_ms",
        "delivery_count",
        "extensions",
        "lease",
    }
    for index, message in enumerate(records):
        body_size = len(message.body_bytes)
        if body_size > JSONL_MAX_BODY_BYTES:
            raise MQError(
                E_RECORD_TOO_LARGE,
                "message body exceeds JSONL format capacity",
                http_status=413,
                details={
                    "index": index,
                    "message_id": message.message_id,
                    "body_bytes": body_size,
                    "max_body_bytes": JSONL_MAX_BODY_BYTES,
                    "format": FORMAT_JSONL,
                },
            )
        ttl_ms = (message.expires_at_us - message.produced_at_us) // 1_000
        record: dict[str, Any] = {
            "id": message.message_id,
            "queue": message.queue,
            "body": message.body,
            "produced_at_ms": us_to_ms_floor(message.produced_at_us),
            "ttl_ms": ttl_ms,
            "content_type": message.content_type,
            "state": message.state,
            "record_version": message.record_version,
            "last_write_at_ms": us_to_ms_floor(int(message.last_write_at_us or message.produced_at_us)),
            "delivery_count": message.delivery_count,
        }
        if message.extensions:
            record["extensions"] = message.extensions
        for key, value in message.extensions.items():
            if key not in reserved:
                record[key] = value
        if message.state == "leased" or message.lease_token is not None:
            record["lease"] = {
                "owner": message.lease_owner,
                "token": message.lease_token,
                "expires_at_ms": (
                    us_to_ms_floor(message.lease_expires_at_us)
                    if message.lease_expires_at_us is not None
                    else None
                ),
            }
        line = canonical_json(record).encode("utf-8")
        if len(line) > MAX_JSONL_LINE_BYTES:
            raise MQError(
                E_RECORD_TOO_LARGE,
                "JSONL record line exceeds the format limit",
                http_status=413,
                details={
                    "index": index,
                    "message_id": message.message_id,
                    "line_bytes": len(line),
                    "max_line_bytes": MAX_JSONL_LINE_BYTES,
                },
            )
        lines.append(line.decode("utf-8"))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _decode_jsonl(data: bytes, *, sub_ms_policy: SubMsPolicy) -> ParsedBatch:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise MQError(
            E_FORMAT_INVALID,
            "JSONL input is not valid UTF-8",
            http_status=422,
            details={"offset": exc.start},
        ) from exc
    raw_lines = [line for line in text.splitlines() if line.strip()]
    if not raw_lines:
        raise MQError(E_FORMAT_INVALID, "JSONL input is empty", http_status=422)
    parsed_lines: list[dict[str, Any]] = []
    for line_no, line in enumerate(raw_lines, start=1):
        if len(line.encode("utf-8")) > MAX_JSONL_LINE_BYTES:
            raise MQError(
                E_RECORD_TOO_LARGE,
                "JSONL line exceeds the format limit",
                http_status=413,
                details={"line": line_no, "max_line_bytes": MAX_JSONL_LINE_BYTES},
            )
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise MQError(
                E_FORMAT_INVALID,
                "JSONL line is not valid JSON",
                http_status=422,
                details={"line": line_no, "column": exc.colno},
            ) from exc
        if not isinstance(value, dict):
            raise MQError(
                E_FORMAT_INVALID,
                "JSONL line root must be an object",
                http_status=422,
                details={"line": line_no},
            )
        parsed_lines.append(value)
    first = parsed_lines[0]
    manifest_present = first.get("_type") == "mq_manifest" or first.get("format") == FORMAT_JSONL
    version = first.get("version", first.get("format_version", 1)) if manifest_present else first.get("format_version", 1)
    if not isinstance(version, int):
        raise MQError(
            E_FORMAT_INVALID,
            "JSONL version must be an integer",
            http_status=422,
            details={"version": version},
        )
    has_unknown_required = bool(first.get("requires_unknown_extension", False)) if manifest_present else False
    decision, compat = decide_compatibility(
        FORMAT_JSONL, version, has_unknown_required_extension=has_unknown_required
    )
    report: dict[str, Any] = {
        "format": FORMAT_JSONL,
        "version": version,
        "decision": decision,
        "compatibility": compat,
        "manifest_present": manifest_present,
        "fields_filled": [],
        "sub_ms_normalizations": [],
        "unknown_optional_keys": [],
        "record_extensions_preserved": 0,
    }
    manifest_queues: list[QueueMeta] = []
    batch_extensions: dict[str, Any] = {}
    record_lines = parsed_lines
    if manifest_present:
        known_manifest = {
            "_type",
            "format",
            "version",
            "format_version",
            "requires_unknown_extension",
            "queues",
            "extensions",
        }
        raw_queues = first.get("queues", [])
        if not isinstance(raw_queues, list):
            raise MQError(E_FORMAT_INVALID, "manifest queues must be an array", http_status=422)
        manifest_queues = [
            _validate_queue_metadata(item, index=i, report=report) for i, item in enumerate(raw_queues)
        ]
        if len({q.name for q in manifest_queues}) != len(manifest_queues):
            raise MQError(E_FORMAT_INVALID, "manifest has duplicate queue names", http_status=422)
        raw_extensions = first.get("extensions", {})
        if not isinstance(raw_extensions, dict):
            raise MQError(E_FORMAT_INVALID, "manifest extensions must be a JSON object", http_status=422)
        unknown_manifest = {key: value for key, value in first.items() if key not in known_manifest}
        batch_extensions = {**raw_extensions, **unknown_manifest}
        report["unknown_optional_keys"].extend(
            f"manifest:{key}" for key in sorted(unknown_manifest)
        )
        record_lines = parsed_lines[1:]
    if len(record_lines) > JSONL_MAX_BATCH_RECORDS:
        raise MQError(
            E_BATCH_SPLIT_REQUIRED,
            "JSONL batch must be split before import",
            http_status=413,
            details={
                "count": len(record_lines),
                "max_records": JSONL_MAX_BATCH_RECORDS,
                "format": FORMAT_JSONL,
                "decision": "reject",
                "state_changed": False,
            },
        )
    known_record = {
        "id",
        "message_id",
        "queue",
        "body",
        "produced_at_ms",
        "ttl_ms",
        "content_type",
        "state",
        "record_version",
        "last_write_at_ms",
        "delivery_count",
        "extensions",
        "lease",
        "format_version",
        "requires_unknown_extension",
    }
    derived_queues: dict[str, QueueMeta] = {queue.name: queue for queue in manifest_queues}
    messages: list[MessageRecord] = []
    for index, raw in enumerate(record_lines):
        if raw.get("requires_unknown_extension") is True:
            raise MQError(
                "E_FORMAT_INCOMPATIBLE",
                "record declares an unknown required extension",
                http_status=422,
                details={"index": index, "decision": "reject", "state_changed": False},
            )
        for required in ("queue", "body", "produced_at_ms", "ttl_ms"):
            if required not in raw:
                raise MQError(
                    E_FORMAT_INVALID,
                    f"JSONL record is missing required field {required}",
                    http_status=422,
                    details={"index": index, "field": required},
                )
        if "id" in raw and "message_id" in raw and raw["id"] != raw["message_id"]:
            raise MQError(
                E_FORMAT_INVALID,
                "JSONL record has conflicting id and message_id values",
                http_status=422,
                details={"index": index},
            )
        message_id = raw.get("id", raw.get("message_id"))
        queue = raw["queue"]
        body = raw["body"]
        if not isinstance(message_id, str) or not message_id or len(message_id) > 256:
            raise MQError(
                E_FORMAT_INVALID,
                "message id must be a non-empty string of at most 256 characters",
                http_status=422,
                details={"index": index},
            )
        if not isinstance(queue, str) or not queue or len(queue) > 128:
            raise MQError(
                E_FORMAT_INVALID,
                "queue name must be a non-empty string of at most 128 characters",
                http_status=422,
                details={"index": index},
            )
        if not isinstance(body, str):
            raise MQError(
                E_FORMAT_INVALID,
                "message body must be a JSON string",
                http_status=422,
                details={"index": index},
            )
        body_bytes = len(body.encode("utf-8"))
        if body_bytes > JSONL_MAX_BODY_BYTES:
            raise MQError(
                E_RECORD_TOO_LARGE,
                "message body exceeds JSONL format capacity",
                http_status=413,
                details={
                    "index": index,
                    "message_id": message_id,
                    "body_bytes": body_bytes,
                    "max_body_bytes": JSONL_MAX_BODY_BYTES,
                    "format": FORMAT_JSONL,
                },
            )
        produced_ms = raw["produced_at_ms"]
        ttl_ms = raw["ttl_ms"]
        if not isinstance(produced_ms, int) or produced_ms < 0:
            raise MQError(
                E_FORMAT_INVALID,
                "produced_at_ms must be a non-negative integer",
                http_status=422,
                details={"index": index},
            )
        if not isinstance(ttl_ms, int):
            raise MQError(
                E_FORMAT_INVALID,
                "ttl_ms must be an integer",
                http_status=422,
                details={"index": index},
            )
        produced_us = ms_to_us(produced_ms)
        expires_us = ms_to_us(produced_ms + ttl_ms)
        content_type = raw.get("content_type", "text/plain")
        if content_type is None:
            content_type = "text/plain"
        if not isinstance(content_type, str):
            raise MQError(
                E_FORMAT_INVALID,
                "content_type must be a string",
                http_status=422,
                details={"index": index},
            )
        if "content_type" not in raw:
            _add_fill(report, index, "content_type", content_type, reason="missing_optional")
        state = raw.get("state", "pending")
        if state not in MESSAGE_STATES:
            raise MQError(
                E_FORMAT_INVALID,
                "unknown message state",
                http_status=422,
                details={"index": index, "state": state},
            )
        if "state" not in raw:
            _add_fill(report, index, "state", state, reason="missing_optional")
        record_version = raw.get("record_version", 1)
        if not isinstance(record_version, int) or record_version < 1:
            raise MQError(
                E_FORMAT_INVALID,
                "record_version must be a positive integer",
                http_status=422,
                details={"index": index},
            )
        if "record_version" not in raw:
            _add_fill(report, index, "record_version", record_version, reason="missing_optional")
        last_write_ms = raw.get("last_write_at_ms", produced_ms)
        if not isinstance(last_write_ms, int) or last_write_ms < 0:
            raise MQError(
                E_FORMAT_INVALID,
                "last_write_at_ms must be a non-negative integer",
                http_status=422,
                details={"index": index},
            )
        if "last_write_at_ms" not in raw:
            _add_fill(report, index, "last_write_at_ms", last_write_ms * 1_000, reason="missing_optional")
        delivery_count = raw.get("delivery_count", 0)
        if not isinstance(delivery_count, int) or delivery_count < 0:
            raise MQError(
                E_FORMAT_INVALID,
                "delivery_count must be a non-negative integer",
                http_status=422,
                details={"index": index},
            )
        if "delivery_count" not in raw:
            _add_fill(report, index, "delivery_count", delivery_count, reason="missing_optional")
        extensions = raw.get("extensions", {})
        if not isinstance(extensions, dict):
            raise MQError(
                E_FORMAT_INVALID,
                "extensions must be a JSON object",
                http_status=422,
                details={"index": index},
            )
        unknown = {key: value for key, value in raw.items() if key not in known_record}
        extensions = {**extensions, **unknown}
        if extensions:
            report["record_extensions_preserved"] += 1
            report["unknown_optional_keys"].extend(
                f"record:{index}:{key}" for key in sorted(extensions)
            )
        lease = raw.get("lease")
        lease_owner = lease_token = None
        lease_expires_us = None
        if lease is not None:
            if not isinstance(lease, dict):
                raise MQError(
                    E_FORMAT_INVALID,
                    "lease must be a JSON object",
                    http_status=422,
                    details={"index": index},
                )
            lease_owner = lease.get("owner")
            lease_token = lease.get("token")
            lease_expires_ms = lease.get("expires_at_ms")
            if lease_expires_ms is not None:
                if not isinstance(lease_expires_ms, int):
                    raise MQError(
                        E_FORMAT_INVALID,
                        "lease expires_at_ms must be an integer",
                        http_status=422,
                        details={"index": index},
                    )
                lease_expires_us = ms_to_us(lease_expires_ms)
        if queue not in derived_queues:
            if manifest_present:
                raise MQError(
                    E_FORMAT_INVALID,
                    "record references a queue absent from manifest metadata",
                    http_status=422,
                    details={"index": index, "queue": queue},
                )
            derived = QueueMeta(
                name=queue,
                semantics="at_least_once",
                visibility_timeout_ms=30_000,
                version=1,
            )
            derived_queues[queue] = derived
            _add_fill(
                report,
                -1,
                f"queues[{queue}].metadata",
                derived.to_dict(),
                reason="manifest_absent",
            )
        messages.append(
            MessageRecord(
                message_id=message_id,
                queue=queue,
                body=body,
                produced_at_us=produced_us,
                expires_at_us=expires_us,
                content_type=content_type,
                state=state,  # type: ignore[arg-type]
                record_version=record_version,
                last_write_at_us=ms_to_us(last_write_ms),
                delivery_count=delivery_count,
                extensions=extensions,
                lease_owner=lease_owner,
                lease_token=lease_token,
                lease_expires_at_us=lease_expires_us,
                source_format=FORMAT_JSONL,
                source_version=version,
            )
        )
    report["records_total"] = len(messages)
    report["queues_total"] = len(derived_queues)
    report["batch_extensions"] = batch_extensions
    report["degraded_fields"] = []
    report["degradations"] = []
    return ParsedBatch(
        format=FORMAT_JSONL,
        version=version,
        decision=decision,
        queues=list(derived_queues.values()),
        messages=messages,
        decode_report=report,
        raw_size=len(data),
        sha256=sha256_bytes(data),
    )


def detect_format(data: bytes) -> tuple[str, int | None]:
    if data.startswith(BINARY_MAGIC):
        if len(data) < BINARY_HEADER.size:
            raise MQError(E_FORMAT_INVALID, "compact input is missing its full header", http_status=422)
        return FORMAT_COMPACT, data[4]
    stripped = data.lstrip()
    if not stripped:
        raise MQError(E_FORMAT_UNKNOWN, "empty input has no detectable format", http_status=415)
    if stripped[:1] == b"{":
        try:
            text = stripped.decode("utf-8-sig")
            first_line = next((line for line in text.splitlines() if line.strip()), "")
            value = json.loads(first_line)
            if isinstance(value, dict):
                version = value.get("version", value.get("format_version", 1))
                return FORMAT_JSONL, version if isinstance(version, int) else None
        except (UnicodeDecodeError, json.JSONDecodeError, StopIteration):
            pass
    raise MQError(
        E_FORMAT_UNKNOWN,
        "input is neither compact magic nor JSON lines with an object header",
        http_status=415,
    )


def decode_auto(
    data: bytes,
    *,
    expected_format: str | None = None,
    sub_ms_policy: str = "truncate",
) -> ParsedBatch:
    policy = check_policy(sub_ms_policy)
    format_name, version = detect_format(data)
    if expected_format is not None and format_name != expected_format:
        raise MQError(
            E_FORMAT_MISMATCH,
            "detected format does not match the import endpoint",
            http_status=415,
            details={
                "endpoint_format": expected_format,
                "detected_format": format_name,
                "detected_version": version,
            },
        )
    if format_name == FORMAT_COMPACT:
        return _decode_compact(data, sub_ms_policy=policy)
    return _decode_jsonl(data, sub_ms_policy=policy)


def build_future_compact_sample(
    queues: Iterable[QueueMeta], messages: Iterable[MessageRecord], *, version: int = 3
) -> bytes:
    """Build a deliberately unsupported file for compatibility tests."""

    queue_list = list(queues)
    records = list(messages)
    metadata = canonical_json({"queues": [queue.to_dict() for queue in queue_list], "extensions": {}}).encode("utf-8")
    header = BINARY_HEADER.pack(BINARY_MAGIC, version, 0x01, 0, len(metadata), len(records))
    result = bytearray(header + metadata)
    for message in records:
        result.extend(_encode_compact_record(message, version=2, include_optional=True))
    return bytes(result)
