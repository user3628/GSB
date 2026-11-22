"""Interchangeable new-model persistence codecs.

Two formats are supported and must be semantically interchangeable:

* compact binary: magic, format version byte, per-record length prefix,
  CRC32 checksum and an optional-field presence bitmap;
* sharded JSON lines: one new-model policy record per line.

A record decoded from either format produces an identical canonical digest
when its semantic content is identical.
"""

import json
import struct
import zlib

from .errors import FormatUnsupported, RecordCorrupt
from .model import NewPolicy, parse_new_records

MAGIC = b"PMIGBIN1"
BINARY_FORMAT_VERSION = 1
JSONL_FORMAT_VERSION = 1
_SUPPORTED_VERSIONS = {1}

_FLAG_PARENT = 0x01
_FLAG_PRIORITY = 0x02
_FLAG_EXTENSIONS = 0x04
_KNOWN_FLAGS = _FLAG_PARENT | _FLAG_PRIORITY | _FLAG_EXTENSIONS


def _encode_text(text):
    raw = text.encode("utf-8")
    return struct.pack(">H", len(raw)) + raw


def _decode_text(buf, offset):
    (length,) = struct.unpack_from(">H", buf, offset)
    offset += 2
    end = offset + length
    if end > len(buf):
        raise RecordCorrupt(
            "truncated text field", {"record_index": None}
        )
    return buf[offset:end].decode("utf-8"), end


def encode_binary(policies):
    frames = [MAGIC, bytes([BINARY_FORMAT_VERSION]), struct.pack(">I", len(policies))]
    for policy in policies:
        flags = 0
        body = bytearray()
        body += _encode_text(policy.tenant_id)
        if policy.parent_ref is not None:
            flags |= _FLAG_PARENT
            body += bytes([1])
            body += _encode_text(policy.parent_ref)
        else:
            body += bytes([0])
        body += struct.pack(">I", policy.concurrency_limit)
        body += struct.pack(">B", policy.preempt_tier)
        body += struct.pack(">q", policy.lifetime_start)
        body += struct.pack(">q", policy.lifetime_end)
        if policy.priority_min is not None and policy.priority_max is not None:
            flags |= _FLAG_PRIORITY
            body += struct.pack(">ii", policy.priority_min, policy.priority_max)
        if policy.extensions:
            flags |= _FLAG_EXTENSIONS
            raw = json.dumps(
                policy.extensions, sort_keys=True, separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            body += struct.pack(">H", len(raw)) + raw
        payload = bytes([flags]) + bytes(body)
        frames.append(struct.pack(">I", len(payload)))
        frames.append(payload)
        frames.append(struct.pack(">I", zlib.crc32(payload) & 0xFFFFFFFF))
    return b"".join(frames)


def _decode_record(buf, offset, index):
    if offset + 4 > len(buf):
        raise RecordCorrupt(
            "truncated length prefix for record %d" % index,
            {"record_index": index},
        )
    (length,) = struct.unpack_from(">I", buf, offset)
    offset += 4
    end = offset + length + 4
    if length == 0 or offset + length + 4 > len(buf):
        raise RecordCorrupt(
            "truncated frame for record %d" % index, {"record_index": index}
        )
    payload = buf[offset:offset + length]
    (crc,) = struct.unpack_from(">I", buf, offset + length)
    actual = zlib.crc32(payload) & 0xFFFFFFFF
    if actual != crc:
        raise RecordCorrupt(
            "CRC32 mismatch on record %d" % index,
            {"record_index": index, "expected_crc32": crc, "actual_crc32": actual},
        )
    flags = payload[0]
    if flags & ~_KNOWN_FLAGS:
        raise RecordCorrupt(
            "unknown presence bitmap bits 0x%02x on record %d" % (flags, index),
            {"record_index": index, "flags": flags},
        )
    pos = 1
    try:
        tenant_id, pos = _decode_text(payload, pos)
        (parent_present,) = struct.unpack_from(">B", payload, pos)
        pos += 1
        parent_ref = None
        if parent_present:
            parent_ref, pos = _decode_text(payload, pos)
        concurrency_limit, preempt_tier = struct.unpack_from(">IB", payload, pos)
        pos += 5
        lifetime_start, lifetime_end = struct.unpack_from(">qq", payload, pos)
        pos += 16
        priority_min = priority_max = None
        if flags & _FLAG_PRIORITY:
            priority_min, priority_max = struct.unpack_from(">ii", payload, pos)
            pos += 8
        extensions = {}
        if flags & _FLAG_EXTENSIONS:
            (ext_len,) = struct.unpack_from(">H", payload, pos)
            pos += 2
            extensions = json.loads(payload[pos:pos + ext_len].decode("utf-8"))
            pos += ext_len
        if pos != len(payload):
            raise RecordCorrupt(
                "trailing bytes inside record %d" % index,
                {"record_index": index, "consumed": pos, "length": len(payload)},
            )
    except RecordCorrupt:
        raise
    except (struct.error, UnicodeError, json.JSONDecodeError) as exc:
        raise RecordCorrupt(
            "malformed record %d: %s" % (index, exc),
            {"record_index": index},
        )
    return NewPolicy(
        tenant_id, parent_ref, concurrency_limit, preempt_tier,
        lifetime_start, lifetime_end, priority_min, priority_max, extensions,
    ), end


def decode_binary(data):
    if not isinstance(data, (bytes, bytearray)):
        raise RecordCorrupt("binary payload must be bytes", {"record_index": -1})
    data = bytes(data)
    if len(data) < 13 or not data.startswith(MAGIC):
        raise RecordCorrupt(
            "bad or missing magic number", {"record_index": -1, "offset": 0}
        )
    version = data[8]
    if version not in _SUPPORTED_VERSIONS:
        raise FormatUnsupported(
            "binary format version %d is not supported" % version,
            {"detected_format": "binary", "detected_version": version,
             "supported_versions": sorted(_SUPPORTED_VERSIONS)},
        )
    (count,) = struct.unpack_from(">I", data, 9)
    offset = 13
    policies = []
    for index in range(count):
        policy, offset = _decode_record(data, offset, index)
        policies.append(policy)
    if offset != len(data):
        raise RecordCorrupt(
            "trailing bytes after last record",
            {"record_index": count, "offset": offset, "total_length": len(data)},
        )
    return parse_new_records(policies)


def detect_binary_version(data):
    if isinstance(data, (bytes, bytearray)) and data[:8] == MAGIC:
        return data[8] if len(data) >= 9 else None
    return None


def encode_jsonl(policies):
    lines = [
        json.dumps(policy.to_dict(), ensure_ascii=False, separators=(",", ":"))
        for policy in policies
    ]
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def decode_jsonl(data):
    if isinstance(data, (bytes, bytearray)):
        text = bytes(data).decode("utf-8")
    else:
        text = data
    records = []
    for index, line in enumerate(text.splitlines()):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise RecordCorrupt(
                "invalid JSON on line %d: %s" % (index, exc),
                {"record_index": index},
            )
    return parse_new_records(records)
