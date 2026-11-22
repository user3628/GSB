from __future__ import annotations

import base64
import binascii
import json
import struct
from typing import Any, Iterable

from .errors import StoreError, record_corrupt
from .model import DecodedRecord, ParsedRecord, Record


BINARY_MAGIC = b"KVSG"
BINARY_FORMAT_VERSION = 1
BINARY_HEADER_SIZE = 12
ENDIAN_MARKERS = {"little": b"L", "big": b"B"}
ENDIAN_BY_MARKER = {value: key for key, value in ENDIAN_MARKERS.items()}
OP_PUT = 0
OP_DELETE = 1
SUPPORTED_JSON_SHARD_FORMAT_VERSION = 1
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def _struct_prefix(endianness: str) -> str:
    if endianness == "little":
        return "<"
    if endianness == "big":
        return ">"
    raise StoreError(
        "E_FORMAT_UNSUPPORTED",
        "unknown endianness",
        {"endianness": endianness, "supported": sorted(ENDIAN_MARKERS)},
    )


def _check_ms(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field} must be an integer millisecond timestamp")
    if value < INT64_MIN or value > INT64_MAX:
        raise ValueError(f"{field} is outside signed 64-bit range")
    return value


def encode_binary(records: Iterable[Record], *, endian: str = "little") -> bytes:
    """Encode records in the compact KVSG binary interchange format.

    Layout:
      0..3   magic "KVSG"
      4      format version (1)
      5      endian marker "L" or "B"
      6..7   reserved flags (0)
      8..11  record count (u32, declared-endian)

    Each record is a u32 length prefix, a payload and a u32 CRC32. The length
    count includes the trailing CRC but not the prefix itself.
    """

    prefix = _struct_prefix(endian)
    record_list = list(records)
    if len(record_list) > 0xFFFFFFFF:
        raise ValueError("too many records for binary format")
    parts = [
        BINARY_MAGIC,
        bytes([BINARY_FORMAT_VERSION]),
        ENDIAN_MARKERS[endian],
        struct.pack(prefix + "H", 0),
        struct.pack(prefix + "I", len(record_list)),
    ]
    for record in record_list:
        key = record.key.encode("utf-8")
        value = bytes(record.value)
        if len(key) > 0xFFFFFFFF or len(value) > 0xFFFFFFFF:
            raise ValueError("key or value is too large")
        version = int(record.version)
        if version < 0 or version > 0xFFFFFFFFFFFFFFFF:
            raise ValueError("version must fit unsigned 64-bit")
        write_ts = _check_ms(int(record.write_ts_ms), "write_ts_ms")
        expires = (
            None
            if record.expires_at_ms is None
            else _check_ms(int(record.expires_at_ms), "expires_at_ms")
        )
        flag = 0 if expires is None else 1
        payload = bytearray()
        payload.extend(bytes([OP_DELETE if record.deleted else OP_PUT, flag]))
        payload.extend(struct.pack(prefix + "IIQq", len(key), len(value), version, write_ts))
        if expires is not None:
            payload.extend(struct.pack(prefix + "q", expires))
        payload.extend(key)
        payload.extend(value)
        crc = binascii.crc32(payload) & 0xFFFFFFFF
        frame_len = len(payload) + 4
        parts.append(struct.pack(prefix + "I", frame_len))
        parts.append(bytes(payload))
        parts.append(struct.pack(prefix + "I", crc))
    return b"".join(parts)


def _decode_binary_payload(
    payload: bytes,
    *,
    prefix: str,
    record_index: int,
    byte_offset: int,
) -> Record:
    # op(1), expiry_flag(1), key_len(4), value_len(4), version(8),
    # write_ts(8), optional expires(8), key, value
    fixed = 1 + 1 + 4 + 4 + 8 + 8
    if len(payload) < fixed:
        raise record_corrupt(
            "binary record payload is truncated",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
            record_position=f"record #{record_index} at byte {byte_offset}",
        )
    op = payload[0]
    expiry_flag = payload[1]
    if op not in (OP_PUT, OP_DELETE):
        raise record_corrupt(
            "binary record has an unknown operation",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
            op=op,
        )
    if expiry_flag not in (0, 1):
        raise record_corrupt(
            "binary record has an invalid expiry flag",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
            expiry_flag=expiry_flag,
        )
    try:
        key_len, value_len, version, write_ts = struct.unpack_from(
            prefix + "IIQq", payload, 2
        )
        cursor = 26
        if expiry_flag:
            if len(payload) < cursor + 8:
                raise struct.error("truncated expiry")
            (expires_at,) = struct.unpack_from(prefix + "q", payload, cursor)
            cursor += 8
        else:
            expires_at = None
    except struct.error as exc:
        raise record_corrupt(
            f"binary record field decode failed: {exc}",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
        ) from exc
    if version < 1:
        raise record_corrupt(
            "binary record version must be >= 1",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
        )
    expected = cursor + key_len + value_len
    if expected != len(payload):
        raise record_corrupt(
            "binary record length does not match key/value lengths",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
            declared_payload_length=len(payload),
            expected_payload_length=expected,
        )
    key_bytes = payload[cursor : cursor + key_len]
    value = payload[cursor + key_len : cursor + key_len + value_len]
    try:
        key = key_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise record_corrupt(
            "binary record key is not valid UTF-8",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
            key_offset=cursor,
        ) from exc
    if not key:
        raise record_corrupt(
            "binary record key must not be empty",
            record_index=record_index,
            byte_offset=byte_offset,
            format_name="binary",
        )
    return Record(
        key=key,
        value=bytes(value),
        version=int(version),
        write_ts_ms=int(write_ts),
        expires_at_ms=None if expires_at is None else int(expires_at),
        deleted=op == OP_DELETE,
    )


def decode_binary(data: bytes | bytearray | memoryview) -> list[DecodedRecord]:
    raw = bytes(data)
    if len(raw) < BINARY_HEADER_SIZE:
        raise record_corrupt(
            "binary header is truncated",
            record_index=0,
            byte_offset=0,
            format_name="binary",
        )
    if raw[:4] != BINARY_MAGIC:
        raise record_corrupt(
            "binary magic mismatch",
            record_index=0,
            byte_offset=0,
            format_name="binary",
            magic=raw[:4].hex(),
        )
    version = raw[4]
    if version != BINARY_FORMAT_VERSION:
        raise record_corrupt(
            "unsupported binary format version",
            record_index=0,
            byte_offset=4,
            format_name="binary",
            format_version=version,
            supported_versions=[BINARY_FORMAT_VERSION],
        )
    marker = raw[5:6]
    endianness = ENDIAN_BY_MARKER.get(marker)
    if endianness is None:
        raise record_corrupt(
            "binary endian marker is invalid",
            record_index=0,
            byte_offset=5,
            format_name="binary",
            endian_marker=marker.hex(),
        )
    prefix = _struct_prefix(endianness)
    (record_count,) = struct.unpack_from(prefix + "I", raw, 8)
    if record_count > 10_000_000:
        raise record_corrupt(
            "binary record count exceeds the safety limit",
            record_index=0,
            byte_offset=8,
            format_name="binary",
            record_count=record_count,
        )
    records: list[DecodedRecord] = []
    cursor = BINARY_HEADER_SIZE
    for record_index in range(record_count):
        start = cursor
        if cursor + 4 > len(raw):
            raise record_corrupt(
                "binary record length prefix is truncated",
                record_index=record_index,
                byte_offset=cursor,
                format_name="binary",
                record_position=f"record #{record_index} at byte {cursor}",
            )
        (frame_len,) = struct.unpack_from(prefix + "I", raw, cursor)
        cursor += 4
        if frame_len < 4:
            raise record_corrupt(
                "binary record length is smaller than its CRC",
                record_index=record_index,
                byte_offset=start,
                format_name="binary",
                frame_length=frame_len,
            )
        if cursor + frame_len > len(raw):
            raise record_corrupt(
                "binary record is truncated",
                record_index=record_index,
                byte_offset=start,
                format_name="binary",
                frame_length=frame_len,
                bytes_remaining=len(raw) - cursor,
            )
        payload = raw[cursor : cursor + frame_len - 4]
        (declared_crc,) = struct.unpack_from(prefix + "I", raw, cursor + frame_len - 4)
        actual_crc = binascii.crc32(payload) & 0xFFFFFFFF
        if actual_crc != declared_crc:
            raise record_corrupt(
                "binary record CRC32 mismatch",
                record_index=record_index,
                byte_offset=start,
                format_name="binary",
                record_position=f"record #{record_index} at byte {start}",
                declared_crc32=f"{declared_crc:08x}",
                actual_crc32=f"{actual_crc:08x}",
            )
        record = _decode_binary_payload(
            payload,
            prefix=prefix,
            record_index=record_index,
            byte_offset=start,
        )
        total_length = 4 + frame_len
        records.append(
            DecodedRecord(
                record=record,
                offset=start,
                length=total_length,
                record_index=record_index,
            )
        )
        cursor += frame_len
    if cursor != len(raw):
        raise record_corrupt(
            "binary stream has trailing bytes after the declared records",
            record_index=record_count,
            byte_offset=cursor,
            format_name="binary",
            trailing_bytes=len(raw) - cursor,
        )
    return records


def decode_binary_record_at(
    data: bytes,
    offset: int,
    *,
    expected_length: int | None = None,
    record_index: int = 0,
) -> DecodedRecord:
    """Decode one frame at an absolute segment offset without scanning."""

    if offset < BINARY_HEADER_SIZE or offset + 4 > len(data):
        raise record_corrupt(
            "segment offset is outside the binary file",
            record_index=record_index,
            byte_offset=offset,
            format_name="binary",
        )
    (frame_len,) = struct.unpack_from("<I", data, offset)
    total = 4 + frame_len
    if expected_length is not None and total != expected_length:
        raise record_corrupt(
            "segment record length no longer matches the index",
            record_index=record_index,
            byte_offset=offset,
            format_name="binary",
            expected_length=expected_length,
            actual_length=total,
        )
    if frame_len < 4 or offset + total > len(data):
        raise record_corrupt(
            "segment record frame is truncated",
            record_index=record_index,
            byte_offset=offset,
            format_name="binary",
        )
    payload = data[offset + 4 : offset + total - 4]
    (declared_crc,) = struct.unpack_from("<I", data, offset + total - 4)
    if (binascii.crc32(payload) & 0xFFFFFFFF) != declared_crc:
        raise record_corrupt(
            "segment record CRC32 mismatch",
            record_index=record_index,
            byte_offset=offset,
            format_name="binary",
        )
    record = _decode_binary_payload(
        payload,
        prefix="<",
        record_index=record_index,
        byte_offset=offset,
    )
    return DecodedRecord(record=record, offset=offset, length=total, record_index=record_index)


def _value_to_json(value: bytes) -> tuple[str, str]:
    try:
        return value.decode("utf-8"), "utf8"
    except UnicodeDecodeError:
        return base64.b64encode(value).decode("ascii"), "base64"


def _value_from_json(obj: dict[str, Any], *, record_index: int, line_no: int) -> bytes:
    encoding = obj.get("value_encoding", obj.get("encoding", "utf8"))
    value = obj.get("value", "")
    if encoding in (None, "utf8", "text"):
        if not isinstance(value, str):
            raise record_corrupt(
                "JSON record value must be a string for utf8 encoding",
                record_index=record_index,
                line_no=line_no,
                format_name="jsonl",
            )
        return value.encode("utf-8")
    if encoding == "base64":
        if not isinstance(value, str):
            raise record_corrupt(
                "JSON record value must be a base64 string",
                record_index=record_index,
                line_no=line_no,
                format_name="jsonl",
            )
        try:
            return base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise record_corrupt(
                "JSON record base64 value is invalid",
                record_index=record_index,
                line_no=line_no,
                format_name="jsonl",
            ) from exc
    raise record_corrupt(
        "JSON record has an unsupported value encoding",
        record_index=record_index,
        line_no=line_no,
        format_name="jsonl",
        value_encoding=encoding,
    )


def _canonical_json_bytes(obj: dict[str, Any]) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _line_crc(obj: dict[str, Any]) -> str:
    body = {key: value for key, value in obj.items() if key != "crc32"}
    return f"{binascii.crc32(_canonical_json_bytes(body)) & 0xFFFFFFFF:08x}"


def jsonl_record_object(
    record: Record, *, shard_count: int = 1, include_deleted: bool = True
) -> dict[str, Any]:
    value, encoding = _value_to_json(record.value)
    write_us = record.write_ts_ms * 1000
    expires_us = None if record.expires_at_ms is None else record.expires_at_ms * 1000
    shard_id = binascii.crc32(record.key.encode("utf-8")) % max(1, shard_count)
    obj: dict[str, Any] = {
        "key": record.key,
        "value": value,
        "value_encoding": encoding,
        "version": record.version,
        "write_timestamp": write_us,
        "write_timestamp_us": write_us,
        "write_timestamp_unit": "us",
        "expires_at": expires_us,
        "expires_at_us": expires_us,
        "shard_count": shard_count,
        "shard_id": shard_id,
    }
    if include_deleted and record.deleted:
        obj["op"] = "delete"
        obj["deleted"] = True
    obj["crc32"] = _line_crc(obj)
    return obj


def encode_jsonl(
    records: Iterable[Record], *, shard_count: int = 1, include_deleted: bool = True
) -> bytes:
    lines = [
        json.dumps(
            jsonl_record_object(
                record, shard_count=shard_count, include_deleted=include_deleted
            ),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        for record in records
    ]
    return (("\n".join(lines) + "\n") if lines else "").encode("utf-8")


def encode_jsonl_shards(
    records: Iterable[Record], *, shard_count: int
) -> dict[str, Any]:
    if shard_count < 1:
        raise ValueError("shard_count must be at least 1")
    shards: list[list[dict[str, Any]]] = [[] for _ in range(shard_count)]
    for record in records:
        obj = jsonl_record_object(record, shard_count=shard_count, include_deleted=True)
        shards[int(obj["shard_id"])].append(obj)
    return {
        "format": "kv-jsonl-shards",
        "format_version": SUPPORTED_JSON_SHARD_FORMAT_VERSION,
        "shard_count": shard_count,
        "shards": [
            {"shard_id": index, "records": shard}
            for index, shard in enumerate(shards)
        ],
    }


def _int_field(
    obj: dict[str, Any],
    names: tuple[str, ...],
    *,
    record_index: int,
    line_no: int,
) -> tuple[int | None, str | None, bool]:
    for name in names:
        if name in obj:
            value = obj[name]
            if value is None:
                return None, name, True
            if not isinstance(value, int) or isinstance(value, bool):
                raise record_corrupt(
                    "JSON record timestamp must be an integer",
                    record_index=record_index,
                    line_no=line_no,
                    format_name="jsonl",
                    field=name,
                )
            return value, name, True
    return None, None, False


def _timestamp_to_us(value: int, unit: str | None) -> int:
    unit = (unit or "us").lower()
    if unit in ("us", "microsecond", "microseconds", "µs"):
        return value
    if unit in ("ms", "millisecond", "milliseconds"):
        return value * 1000
    if unit in ("s", "second", "seconds"):
        return value * 1_000_000
    raise ValueError(f"unsupported timestamp unit: {unit}")


def _normalize_timestamp(source_us: int) -> tuple[int, int]:
    # Declared rule: floor toward negative infinity. Remainder is therefore in
    # [0, 999] and the signed error (normalized - source) is -remainder.
    normalized_ms = source_us // 1000
    remainder = source_us - normalized_ms * 1000
    return normalized_ms, remainder


def _validate_normalized_ms(
    value: int, *, record_index: int, line_no: int, field: str
) -> int:
    if value < INT64_MIN or value > INT64_MAX:
        raise record_corrupt(
            "JSON timestamp is outside the signed 64-bit millisecond range",
            record_index=record_index,
            line_no=line_no,
            format_name="jsonl",
            field=field,
            normalized_ms=value,
        )
    return value


def _norm_part(source_us: int, normalized_ms: int) -> dict[str, Any]:
    return {
        "source_us": source_us,
        "normalized_ms": normalized_ms,
        "rounding_remainder_us": source_us - normalized_ms * 1000,
        "rounding_error_us": normalized_ms * 1000 - source_us,
    }


def _record_from_json(
    obj: Any,
    *,
    record_index: int,
    line_no: int,
    import_time_ms: int,
    shard_id: int | None,
    cumulative_error_us: int,
) -> tuple[ParsedRecord, int]:
    if not isinstance(obj, dict):
        raise record_corrupt(
            "JSON record line must be an object",
            record_index=record_index,
            line_no=line_no,
            format_name="jsonl",
        )
    if "crc32" in obj:
        declared = obj["crc32"]
        actual = _line_crc(obj)
        if declared != actual:
            raise record_corrupt(
                "JSON record CRC32 mismatch",
                record_index=record_index,
                line_no=line_no,
                format_name="jsonl",
                declared_crc32=declared,
                actual_crc32=actual,
            )
    key = obj.get("key")
    if not isinstance(key, str) or not key:
        raise record_corrupt(
            "JSON record key is required and must be a non-empty string",
            record_index=record_index,
            line_no=line_no,
            format_name="jsonl",
        )
    value = _value_from_json(obj, record_index=record_index, line_no=line_no)
    defaults: list[str] = []
    version = obj.get("version")
    if version is None:
        version = 1
        defaults.append("version=1")
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version < 1
        or version > 0xFFFFFFFFFFFFFFFF
    ):
        raise record_corrupt(
            "JSON record version must be a positive integer",
            record_index=record_index,
            line_no=line_no,
            format_name="jsonl",
        )

    now_us = import_time_ms * 1000
    write_us, write_field, write_present = _int_field(
        obj,
        (
            "write_timestamp_us",
            "write_ts_us",
            "timestamp_us",
            "write_timestamp_ms",
            "write_ts_ms",
            "write_timestamp",
            "write_ts",
            "timestamp",
        ),
        record_index=record_index,
        line_no=line_no,
    )
    if not write_present:
        write_us = now_us
        write_field = "default_import_time"
        defaults.append("write_timestamp=import_time")
    elif write_us is None:
        raise record_corrupt(
            "JSON record write timestamp must not be null",
            record_index=record_index,
            line_no=line_no,
            format_name="jsonl",
        )
    assert write_us is not None
    unit = obj.get("write_timestamp_unit", obj.get("timestamp_unit"))
    if write_field and write_field.endswith("_ms"):
        unit = "ms"
    try:
        write_us = _timestamp_to_us(write_us, unit)
    except ValueError as exc:
        raise record_corrupt(
            str(exc),
            record_index=record_index,
            line_no=line_no,
            format_name="jsonl",
            field=write_field,
        ) from exc
    write_ms, write_remainder = _normalize_timestamp(write_us)
    _validate_normalized_ms(
        write_ms, record_index=record_index, line_no=line_no, field="write_timestamp"
    )
    write_part = _norm_part(write_us, write_ms)
    cumulative_error_us += write_part["rounding_error_us"]
    write_part["cumulative_rounding_error_us"] = cumulative_error_us

    base_us = write_us
    base_value, base_field, base_present = _int_field(
        obj,
        (
            "base_time_us",
            "base_timestamp_us",
            "base_write_timestamp_us",
            "base_time",
            "base_time_ms",
        ),
        record_index=record_index,
        line_no=line_no,
    )
    if base_present and base_value is not None:
        base_unit = obj.get("base_time_unit")
        if base_field and base_field.endswith("_ms"):
            base_unit = "ms"
        try:
            base_us = _timestamp_to_us(base_value, base_unit)
        except ValueError as exc:
            raise record_corrupt(
                str(exc),
                record_index=record_index,
                line_no=line_no,
                format_name="jsonl",
                field=base_field,
            ) from exc

    ttl_us: int | None = None
    ttl_field: str | None = None
    for field in (
        "ttl_us",
        "ttl",
        "expires_in_us",
        "ttl_ms",
        "expires_in_ms",
        "ttl_seconds",
        "ttl_s",
    ):
        if field in obj and obj[field] is not None:
            raw_ttl = obj[field]
            if not isinstance(raw_ttl, int) or isinstance(raw_ttl, bool):
                raise record_corrupt(
                    "JSON record TTL must be an integer",
                    record_index=record_index,
                    line_no=line_no,
                    format_name="jsonl",
                    field=field,
                )
            ttl_field = field
            if field in ("ttl_us", "ttl", "expires_in_us"):
                ttl_us = raw_ttl
            elif field in ("ttl_ms", "expires_in_ms"):
                ttl_us = raw_ttl * 1000
            else:
                ttl_us = raw_ttl * 1_000_000
            break

    expires_ms: int | None
    expires_part: dict[str, Any] | None
    if ttl_us is not None:
        expires_us_source = base_us + ttl_us
        expires_ms, _ = _normalize_timestamp(expires_us_source)
        _validate_normalized_ms(
            expires_ms,
            record_index=record_index,
            line_no=line_no,
            field="expires_at",
        )
        expires_part = _norm_part(expires_us_source, expires_ms)
        cumulative_error_us += expires_part["rounding_error_us"]
        expires_part["cumulative_rounding_error_us"] = cumulative_error_us
        expires_part["source"] = "relative_ttl"
        expires_part["ttl_field"] = ttl_field
        expires_part["base_time_us"] = base_us
    else:
        exp_value, exp_field, exp_present = _int_field(
            obj,
            ("expires_at_us", "expires_at_ms", "expires_at", "expiry_timestamp_us"),
            record_index=record_index,
            line_no=line_no,
        )
        if not exp_present:
            expires_ms = None
            expires_part = None
            defaults.append("expires_at=null")
        elif exp_value is None:
            expires_ms = None
            expires_part = None
        else:
            exp_unit = obj.get("expires_at_unit", obj.get("expiry_timestamp_unit"))
            if exp_field and exp_field.endswith("_ms"):
                exp_unit = "ms"
            try:
                exp_us_source = _timestamp_to_us(exp_value, exp_unit)
            except ValueError as exc:
                raise record_corrupt(
                    str(exc),
                    record_index=record_index,
                    line_no=line_no,
                    format_name="jsonl",
                    field=exp_field,
                ) from exc
            expires_ms, _ = _normalize_timestamp(exp_us_source)
            _validate_normalized_ms(
                expires_ms,
                record_index=record_index,
                line_no=line_no,
                field="expires_at",
            )
            expires_part = _norm_part(exp_us_source, expires_ms)
            cumulative_error_us += expires_part["rounding_error_us"]
            expires_part["cumulative_rounding_error_us"] = cumulative_error_us
            expires_part["source"] = "absolute"

    deleted = bool(obj.get("deleted", False)) or obj.get("op") in ("delete", "d")
    record = Record(
        key=key,
        value=value,
        version=version,
        write_ts_ms=write_ms,
        expires_at_ms=expires_ms,
        deleted=deleted,
    )
    normalization = {
        "rule": "floor_toward_negative_infinity_to_milliseconds",
        "write": write_part,
        "expires": expires_part,
        "cumulative_rounding_error_us": cumulative_error_us,
    }
    return (
        ParsedRecord(
            record=record,
            record_index=record_index,
            line_no=line_no,
            shard_id=shard_id,
            defaults_used=defaults,
            normalization=normalization,
        ),
        cumulative_error_us,
    )


def _iter_json_objects(data: bytes) -> list[tuple[Any, int, int | None]]:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise record_corrupt(
            "JSONL input is not valid UTF-8",
            record_index=0,
            line_no=1,
            format_name="jsonl",
        ) from exc
    stripped = text.lstrip()
    if not stripped:
        return []
    # A shard bundle is a single JSON document. Plain NDJSON always wins when
    # the first non-space character is not an opening JSON object/array.
    if stripped[0] in "[{":
        try:
            whole = json.loads(text)
        except json.JSONDecodeError:
            whole = None
        if isinstance(whole, list):
            return [(obj, index + 1, None) for index, obj in enumerate(whole)]
        if isinstance(whole, dict) and "shards" in whole:
            if whole.get("format_version", SUPPORTED_JSON_SHARD_FORMAT_VERSION) != SUPPORTED_JSON_SHARD_FORMAT_VERSION:
                raise StoreError(
                    "E_FORMAT_MISMATCH",
                    "unsupported JSON shard format version",
                    {"format_version": whole.get("format_version")},
                )
            result: list[tuple[Any, int, int | None]] = []
            flat_index = 1
            for shard in whole.get("shards", []):
                if isinstance(shard, dict):
                    sid = shard.get("shard_id")
                    lines = shard.get("records", shard.get("lines", []))
                else:
                    sid = None
                    lines = shard
                if isinstance(lines, str):
                    lines = [json.loads(line) for line in lines.splitlines() if line.strip()]
                for obj in lines:
                    result.append((obj, flat_index, sid))
                    flat_index += 1
            return result
        if isinstance(whole, dict) and "records" in whole:
            return [(obj, index + 1, None) for index, obj in enumerate(whole["records"])]
    objects: list[tuple[Any, int, int | None]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise record_corrupt(
                "JSONL line is not valid JSON",
                record_index=len(objects),
                line_no=line_no,
                format_name="jsonl",
                json_error=exc.msg,
                column=exc.colno,
            ) from exc
        objects.append((obj, line_no, None))
    return objects


def decode_jsonl(data: bytes, *, import_time_ms: int) -> list[ParsedRecord]:
    parsed: list[ParsedRecord] = []
    cumulative_error_us = 0
    for obj, line_no, shard_id in _iter_json_objects(data):
        parsed_record, cumulative_error_us = _record_from_json(
            obj,
            record_index=len(parsed),
            line_no=line_no,
            import_time_ms=import_time_ms,
            shard_id=shard_id,
            cumulative_error_us=cumulative_error_us,
        )
        parsed.append(parsed_record)
    return parsed


def detect_format(data: bytes) -> str:
    raw = bytes(data)
    if raw[:4] == BINARY_MAGIC:
        return "binary"
    stripped = raw.lstrip()
    if stripped[:1] in (b"{", b"["):
        return "jsonl"
    # JSONL with a BOM is still recognized.
    if stripped.startswith(b"\xef\xbb\xbf"):
        rest = stripped[3:].lstrip()
        if rest[:1] in (b"{", b"["):
            return "jsonl"
    return "unknown"


def negotiate_format(data: bytes, requested: str = "auto") -> tuple[str, dict[str, Any]]:
    normalized = requested.lower().strip()
    aliases = {
        "auto": "auto",
        "binary": "binary",
        "bin": "binary",
        "jsonl": "jsonl",
        "json": "jsonl",
        "ndjson": "jsonl",
        "json-lines": "jsonl",
    }
    if normalized not in aliases:
        raise StoreError(
            "E_FORMAT_UNSUPPORTED",
            "requested import format is unsupported",
            {"requested_format": requested, "supported": sorted(set(aliases))},
        )
    selected = aliases[normalized]
    detected = detect_format(data)
    if detected == "unknown":
        raise StoreError(
            "E_FORMAT_UNSUPPORTED",
            "could not negotiate an input format",
            {
                "requested_format": requested,
                "detected_format": "unknown",
                "supported": ["binary (KVSG)", "jsonl / ndjson"],
            },
        )
    if selected != "auto" and selected != detected:
        raise StoreError(
            "E_FORMAT_MISMATCH",
            "declared format does not match the detected format",
            {"requested_format": requested, "detected_format": detected},
        )
    report: dict[str, Any] = {
        "requested_format": requested,
        "detected_format": detected,
        "selected_format": detected,
        "compatible": True,
        "timestamp_rule": "truncate_to_internal_milliseconds_toward_negative_infinity",
    }
    if detected == "binary":
        raw = bytes(data)
        report["format_version"] = raw[4] if len(raw) > 4 else None
        marker = raw[5:6]
        report["endianness"] = ENDIAN_BY_MARKER.get(marker)
        if len(raw) >= BINARY_HEADER_SIZE and marker in ENDIAN_BY_MARKER:
            prefix = _struct_prefix(ENDIAN_BY_MARKER[marker])
            report["declared_record_count"] = struct.unpack_from(prefix + "I", raw, 8)[0]
        report["timestamp_unit"] = "milliseconds"
    else:
        report["format_version"] = SUPPORTED_JSON_SHARD_FORMAT_VERSION
        report["timestamp_unit"] = "microseconds"
        report["defaults"] = {
            "value": "empty UTF-8 string",
            "version": 1,
            "write_timestamp": "import time in microseconds",
            "expires_at": None,
            "relative_ttl_base": "record write timestamp unless base_time_us is present",
        }
        report["field_aliases"] = {
            "write_timestamp": ["write_timestamp_us", "write_timestamp", "write_ts", "timestamp"],
            "expires_at": ["expires_at_us", "expires_at"],
            "relative_ttl": ["ttl_us", "ttl", "ttl_ms", "ttl_seconds"],
        }
    return detected, report


def decode_import(
    data: bytes,
    *,
    requested_format: str = "auto",
    import_time_ms: int,
) -> tuple[list[ParsedRecord], dict[str, Any]]:
    detected, report = negotiate_format(data, requested_format)
    if detected == "binary":
        decoded = decode_binary(data)
        parsed: list[ParsedRecord] = []
        for item in decoded:
            record = item.record
            write_us = record.write_ts_ms * 1000
            expires_us = (
                None if record.expires_at_ms is None else record.expires_at_ms * 1000
            )
            normalization = {
                "rule": "already_milliseconds",
                "write": {
                    "source_us": write_us,
                    "normalized_ms": record.write_ts_ms,
                    "rounding_remainder_us": 0,
                    "rounding_error_us": 0,
                    "cumulative_rounding_error_us": 0,
                },
                "expires": None,
                "cumulative_rounding_error_us": 0,
            }
            if expires_us is not None:
                normalization["expires"] = {
                    "source_us": expires_us,
                    "normalized_ms": record.expires_at_ms,
                    "rounding_remainder_us": 0,
                    "rounding_error_us": 0,
                    "cumulative_rounding_error_us": 0,
                }
            parsed.append(
                ParsedRecord(
                    record=record,
                    record_index=item.record_index,
                    offset=item.offset,
                    length=item.length,
                    normalization=normalization,
                )
            )
        report["actual_record_count"] = len(parsed)
        report["cumulative_rounding_error_us"] = 0
        report["rounding_report"] = [
            {
                "record_index": item.record_index,
                "line_no": None,
                "shard_id": None,
                "key": item.record.key,
                **item.normalization,
            }
            for item in parsed
        ]
        return parsed, report

    parsed = decode_jsonl(data, import_time_ms=import_time_ms)
    report["actual_record_count"] = len(parsed)
    report["cumulative_rounding_error_us"] = (
        parsed[-1].normalization["cumulative_rounding_error_us"] if parsed else 0
    )
    report["rounding_report"] = [
        {
            "record_index": item.record_index,
            "line_no": item.line_no,
            "shard_id": item.shard_id,
            "key": item.record.key,
            **item.normalization,
        }
        for item in parsed
    ]
    return parsed, report

