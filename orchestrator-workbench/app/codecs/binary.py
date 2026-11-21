"""Compact binary snapshot codec.

Wire layout (all multi-byte integers are big-endian/network order):

  Header (34 bytes)
    magic          4 bytes  b"ROWB"
    format_version uint8    currently 1; 2 reserved for compatibility tests
    flags          uint8    bit0 = unknown required fields follow
    snapshot_seq   uint32
    generated_at   16 bytes ISO-8601 UTC timestamp, zero padded / truncated
    record_count   uint32
    crc32          uint32    CRC32 over the preceding header bytes and the full record body

  For each record:
    length_prefix  uint32   byte length of the record payload
    payload        bytes
      resource_id      length-prefixed utf-8 string (uint16 length + bytes)
      present_bitmap   uint32 bit mask of optional fields actually present
      budget           uint32          (bitmap bit 0)
      weight_permille  int32           (bitmap bit 1)
      expected_version uint32          (bitmap bit 2)
      parents          uint16 count + length-prefixed utf-8 strings (bit 3)
    record_crc32   uint32   CRC32 over the record payload bytes only
"""
from __future__ import annotations

import struct
import zlib

from app.errors import ErrorCode, WorkbenchError
from app.models import DecodedPayload, FormatName, RawRecord, SnapshotHeader

MAGIC = b"ROWB"
SUPPORTED_VERSION = 1
HIGHEST_KNOWN_VERSION = 2
TS_FIELD_SIZE = 16
FLAGS_OFFSET = 4
SEQ_OFFSET = 6
TS_OFFSET = 10
RECORD_COUNT_OFFSET = 26
CRC_OFFSET = 30
HEADER_SIZE = 34

BIT_BUDGET = 1 << 0
BIT_WEIGHT = 1 << 1
BIT_EXPECTED_VERSION = 1 << 2
BIT_PARENTS = 1 << 3
BIT_UNKNOWN_REQUIRED = 1 << 31


def _encode_text(value: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFF:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"text too long: {value!r}")
    return struct.pack("!H", len(raw)) + raw


def _read_text(buf: bytes, offset: int) -> tuple[str, int]:
    if offset + 2 > len(buf):
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "truncated string length prefix")
    (length,) = struct.unpack_from("!H", buf, offset)
    offset += 2
    if offset + length > len(buf):
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "truncated string body")
    return buf[offset : offset + length].decode("utf-8"), offset + length


def encode(payload: DecodedPayload) -> bytes:
    header = payload.header
    if header.format is not FormatName.BINARY:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "binary codec requires binary format")
    if header.format_version != SUPPORTED_VERSION:
        raise WorkbenchError(
            ErrorCode.E_VERSION_UNSUPPORTED,
            f"writer only emits binary v{SUPPORTED_VERSION}, got v{header.format_version}",
        )

    body = bytearray()
    for record in payload.records:
        present = 0
        payload_bytes = bytearray()
        payload_bytes += _encode_text(record.resource_id)
        if record.budget is not None:
            present |= BIT_BUDGET
        if record.weight_permille is not None:
            present |= BIT_WEIGHT
        if record.expected_version is not None:
            present |= BIT_EXPECTED_VERSION
        if record.parents:
            present |= BIT_PARENTS
        if header.unknown_required_fields:
            present |= BIT_UNKNOWN_REQUIRED
        payload_bytes += struct.pack("!I", present)
        if record.budget is not None:
            payload_bytes += struct.pack("!I", record.budget)
        if record.weight_permille is not None:
            payload_bytes += struct.pack("!i", record.weight_permille)
        if record.expected_version is not None:
            payload_bytes += struct.pack("!I", record.expected_version)
        if record.parents:
            payload_bytes += struct.pack("!H", len(record.parents))
            for parent in record.parents:
                payload_bytes += _encode_text(parent)
        body += struct.pack("!I", len(payload_bytes))
        body += payload_bytes
        body += struct.pack("!I", zlib.crc32(bytes(payload_bytes)) & 0xFFFFFFFF)

    ts = header.generated_at.encode("utf-8")[:TS_FIELD_SIZE]
    ts = ts.ljust(TS_FIELD_SIZE, b"\x00")
    flags = 1 if header.unknown_required_fields else 0
    head_no_crc = (
        MAGIC
        + struct.pack("!BBI", header.format_version, flags, header.snapshot_seq)
        + ts
        + struct.pack("!I", len(payload.records))
    )
    crc = zlib.crc32(head_no_crc + bytes(body)) & 0xFFFFFFFF
    return head_no_crc + struct.pack("!I", crc) + bytes(body)


def _invalid(message: str) -> WorkbenchError:
    return WorkbenchError(ErrorCode.E_FORMAT_INVALID, message)


def decode(data: bytes) -> DecodedPayload:
    if len(data) < HEADER_SIZE:
        raise _invalid("snapshot shorter than binary header")
    if data[:4] != MAGIC:
        raise _invalid("bad magic number")
    format_version, flags, snapshot_seq = struct.unpack_from("!BBI", data, FLAGS_OFFSET)
    ts_raw = data[TS_OFFSET:RECORD_COUNT_OFFSET]
    (record_count,) = struct.unpack_from("!I", data, RECORD_COUNT_OFFSET)
    (stored_crc,) = struct.unpack_from("!I", data, CRC_OFFSET)
    head_for_crc = data[:CRC_OFFSET]
    body = data[HEADER_SIZE:]
    actual_crc = zlib.crc32(head_for_crc + body) & 0xFFFFFFFF
    if stored_crc != actual_crc:
        raise _invalid("snapshot CRC32 mismatch")

    generated_at = ts_raw.rstrip(b"\x00").decode("utf-8")
    unknown_required: list[str] = []
    if flags & 1:
        # Forward-compat marker: exact names live outside the v1 record set.
        unknown_required.append("unknown_required_field_marker")

    records: list[RawRecord] = []
    offset = 0
    for index in range(record_count):
        if offset + 4 > len(body):
            raise _invalid(f"truncated length prefix on record {index}")
        (length,) = struct.unpack_from("!I", body, offset)
        offset += 4
        if offset + length + 4 > len(body):
            raise _invalid(f"truncated record payload on record {index}")
        payload_bytes = body[offset : offset + length]
        (record_crc,) = struct.unpack_from("!I", body, offset + length)
        if zlib.crc32(payload_bytes) & 0xFFFFFFFF != record_crc:
            raise _invalid(f"record {index} CRC32 mismatch")
        offset += length + 4
        records.append(_decode_record(payload_bytes, index, unknown_required))

    header = SnapshotHeader(
        format=FormatName.BINARY,
        format_version=format_version,
        snapshot_seq=snapshot_seq,
        generated_at=generated_at,
        unknown_required_fields=unknown_required,
    )
    return DecodedPayload(header=header, records=records)


def _decode_record(buf: bytes, index: int, unknown_required: list[str]) -> RawRecord:
    offset = 0
    resource_id, offset = _read_text(buf, offset)
    if offset + 4 > len(buf):
        raise _invalid(f"record {index} missing presence bitmap")
    (bitmap,) = struct.unpack_from("!I", buf, offset)
    offset += 4
    present_fields: list[str] = []
    budget = weight = expected_version = None
    parents: list[str] = []

    if bitmap & BIT_BUDGET:
        if offset + 4 > len(buf):
            raise _invalid(f"record {index} truncated budget")
        (budget,) = struct.unpack_from("!I", buf, offset)
        offset += 4
        present_fields.append("budget")
    if bitmap & BIT_WEIGHT:
        if offset + 4 > len(buf):
            raise _invalid(f"record {index} truncated weight")
        (weight,) = struct.unpack_from("!i", buf, offset)
        offset += 4
        present_fields.append("weight_permille")
    if bitmap & BIT_EXPECTED_VERSION:
        if offset + 4 > len(buf):
            raise _invalid(f"record {index} truncated expected_version")
        (expected_version,) = struct.unpack_from("!I", buf, offset)
        offset += 4
        present_fields.append("expected_version")
    if bitmap & BIT_PARENTS:
        if offset + 2 > len(buf):
            raise _invalid(f"record {index} truncated parent count")
        (parent_count,) = struct.unpack_from("!H", buf, offset)
        offset += 2
        for parent_index in range(parent_count):
            parent, offset = _read_text(buf, offset)
            parents.append(parent)
        present_fields.append("parents")
    if bitmap & BIT_UNKNOWN_REQUIRED and "unknown_required_field_marker" not in unknown_required:
        unknown_required.append("unknown_required_field_marker")
    if offset != len(buf):
        raise _invalid(f"record {index} has {len(buf) - offset} trailing bytes")

    return RawRecord(
        resource_id=resource_id,
        parents=parents,
        budget=budget,
        weight_permille=weight,
        expected_version=expected_version,
        present_fields=present_fields,
    )
