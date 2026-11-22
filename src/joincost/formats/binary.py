from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from typing import Any

from ..compat import DEGRADE_ACCEPT, decide, degradation, ensure_supported
from ..errors import BusinessError, E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED
from ..fixed import RATIO_SCALE
from ..model import RelationStats, StatsSnapshot, finalize_snapshot, normalize_relation

MAGIC = b"JCSB"
HEADER_SIZE = 40
FORMAT_NAME = "binary"
CURRENT_VERSION = 2
ENDIAN_LITTLE = 0x4C
ENDIAN_BIG = 0x42
FLAG_BODY_CRC32 = 0x0001
FLAG_OPTIONAL_BITMAP = 0x0002
KNOWN_HEADER_FLAGS = FLAG_BODY_CRC32 | FLAG_OPTIONAL_BITMAP
PRESENCE_NDV = 0x01
PRESENCE_CORRELATION = 0x02
PRESENCE_STALE_KEYS = 0x04
KNOWN_PRESENCE = PRESENCE_NDV | PRESENCE_CORRELATION | PRESENCE_STALE_KEYS


@dataclass(frozen=True)
class DecodedSnapshot:
    snapshot: StatsSnapshot
    report: dict[str, Any]


def _prefix(endian: str) -> str:
    return "<" if endian == "little" else ">"


def _crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def encode_binary(
    snapshot: StatsSnapshot,
    *,
    endian: str = "little",
    format_version: int = CURRENT_VERSION,
) -> bytes:
    if endian not in {"little", "big"}:
        raise ValueError("endian must be little or big")
    if format_version not in {1, 2}:
        raise ValueError("encoder supports format versions 1 and 2")
    order = _prefix(endian)
    body_parts: list[bytes] = []
    for relation in snapshot.relations:
        name = relation.name.encode("utf-8")
        if len(name) > 0xFFFF:
            raise ValueError("relation name is too long")
        presence = PRESENCE_NDV
        if format_version >= 2:
            presence |= PRESENCE_CORRELATION
        if relation.stale_keys:
            presence |= PRESENCE_STALE_KEYS
        record = bytearray()
        record += struct.pack(f"{order}H", len(name))
        record += name
        record += struct.pack("B", presence)
        record += struct.pack(f"{order}Q", relation.row_count)
        if format_version == 1:
            record += struct.pack(f"{order}f", relation.ndv_ratio_fixed / RATIO_SCALE)
        else:
            record += struct.pack(f"{order}d", relation.ndv_ratio_fixed / RATIO_SCALE)
        if presence & PRESENCE_CORRELATION:
            record += struct.pack(f"{order}d", relation.correlation_fixed / RATIO_SCALE)
        if presence & PRESENCE_STALE_KEYS:
            record += struct.pack(f"{order}H", len(relation.stale_keys))
            for stale in relation.stale_keys:
                encoded = stale.encode("utf-8")
                if len(encoded) > 0xFFFF:
                    raise ValueError("stale key marker is too long")
                record += struct.pack(f"{order}H", len(encoded))
                record += encoded
        body_parts.append(struct.pack(f"{order}I", len(record)) + bytes(record))
    body = b"".join(body_parts)
    flags = FLAG_BODY_CRC32 | FLAG_OPTIONAL_BITMAP
    partial = struct.pack(
        f"{order}4sBBHIQIQ",
        MAGIC,
        format_version,
        ENDIAN_LITTLE if endian == "little" else ENDIAN_BIG,
        HEADER_SIZE,
        flags,
        snapshot.version,
        len(snapshot.relations),
        len(body),
    )
    body_crc = _crc32(body)
    partial += struct.pack(f"{order}I", body_crc)
    if len(partial) != HEADER_SIZE - 4:
        raise AssertionError("binary header layout mismatch")
    header_crc = _crc32(partial)
    return partial + struct.pack(f"{order}I", header_crc) + body


def _report(
    *,
    version: int,
    decision: str,
    compatibility: dict[str, Any],
    degradations: list[dict[str, Any]],
    digest: str | None,
    record_count: int,
    endian: str | None,
    body_crc32: int | None,
) -> dict[str, Any]:
    return {
        "detected_format": FORMAT_NAME,
        "format_version": version,
        "decision": decision,
        "compatibility": compatibility,
        "degradations": degradations,
        "snapshot_version": None,
        "digest": digest,
        "record_count": record_count,
        "wire": {
            "endian": endian,
            "header_size": HEADER_SIZE,
            "body_crc32": f"{body_crc32:08x}" if body_crc32 is not None else None,
        },
    }


def _corrupt(message: str, *, offset: int | None = None) -> BusinessError:
    details: dict[str, Any] = {"format": FORMAT_NAME}
    if offset is not None:
        details["offset"] = offset
    return BusinessError(E_FORMAT_CORRUPT, message, details)


def decode_binary(data: bytes) -> DecodedSnapshot:
    if len(data) < 4:
        raise _corrupt("binary snapshot is shorter than the magic number")
    if data[:4] != MAGIC:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary magic does not match JCSB",
            {"format": FORMAT_NAME, "magic": data[:4].hex()},
        )
    if len(data) < HEADER_SIZE:
        raise _corrupt("binary header is truncated")
    endian_byte = data[5]
    if endian_byte == ENDIAN_LITTLE:
        endian = "little"
    elif endian_byte == ENDIAN_BIG:
        endian = "big"
    else:
        raise _corrupt("invalid endian marker", offset=5)
    order = _prefix(endian)
    (
        magic,
        format_version,
        _endian_byte,
        header_size,
        flags,
        version,
        record_count,
        body_length,
        body_crc32,
        header_crc32,
    ) = struct.unpack(f"{order}4sBBHIQIQII", data[:HEADER_SIZE])
    if magic != MAGIC:
        raise _corrupt("magic mismatch")
    if header_size != HEADER_SIZE:
        raise _corrupt("header size mismatch", offset=6)
    if _crc32(data[: HEADER_SIZE - 4]) != header_crc32:
        raise _corrupt("header CRC32 mismatch")
    if flags & ~KNOWN_HEADER_FLAGS:
        report = _report(
            version=format_version,
            decision="REJECT",
            compatibility={"known_flags": KNOWN_HEADER_FLAGS, "flags": flags},
            degradations=[],
            digest=None,
            record_count=record_count,
            endian=endian,
            body_crc32=body_crc32,
        )
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary snapshot declares unknown required header flags",
            {"import_report": report, "unknown_flags": flags & ~KNOWN_HEADER_FLAGS},
        )
    if len(data) != HEADER_SIZE + body_length:
        raise _corrupt(
            "body length does not match the snapshot size",
            offset=HEADER_SIZE,
        )
    body = data[HEADER_SIZE:]
    if _crc32(body) != body_crc32:
        raise _corrupt("body CRC32 mismatch")
    verdict = decide(FORMAT_NAME, format_version)
    base_report = _report(
        version=format_version,
        decision=verdict.decision,
        compatibility=verdict.public_dict(),
        degradations=[],
        digest=None,
        record_count=record_count,
        endian=endian,
        body_crc32=body_crc32,
    )
    base_report["snapshot_version"] = version
    ensure_supported(verdict, base_report)
    if format_version == 1:
        ndv_format = f"{order}f"
        ndv_width = 4
    else:
        ndv_format = f"{order}d"
        ndv_width = 8
    offset = 0
    relations: list[RelationStats] = []
    degradations: list[dict[str, Any]] = []
    if format_version == 1:
        degradations.append(
            degradation(
                relation=None,
                field_name="ndv_ratio",
                action="accept_reduced_precision",
                reason="format v1 stores distinct-value ratios as float32",
            )
        )
    for record_index in range(record_count):
        if offset + 4 > len(body):
            raise _corrupt("record length prefix is truncated", offset=offset)
        (record_length,) = struct.unpack(f"{order}I", body[offset : offset + 4])
        record_start = offset + 4
        record_end = record_start + record_length
        if record_length < 9 or record_end > len(body):
            raise _corrupt(
                "record length is inconsistent with the containing body", offset=offset
            )
        cursor = record_start
        if cursor + 2 > record_end:
            raise _corrupt("record name length is truncated", offset=cursor)
        (name_length,) = struct.unpack(f"{order}H", body[cursor : cursor + 2])
        cursor += 2
        if cursor + name_length + 1 + 8 + ndv_width > record_end:
            raise _corrupt("record fields are truncated", offset=cursor)
        try:
            relation_name = body[cursor : cursor + name_length].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _corrupt("relation name is not valid UTF-8", offset=cursor) from exc
        cursor += name_length
        (presence,) = struct.unpack("B", body[cursor : cursor + 1])
        cursor += 1
        if presence & ~KNOWN_PRESENCE:
            raise _corrupt("record contains unknown presence bits", offset=cursor - 1)
        if not (presence & PRESENCE_NDV):
            raise _corrupt("record is missing its mandatory NDV ratio", offset=cursor - 1)
        (row_count,) = struct.unpack(f"{order}Q", body[cursor : cursor + 8])
        cursor += 8
        (ndv_ratio,) = struct.unpack(ndv_format, body[cursor : cursor + ndv_width])
        cursor += ndv_width
        if format_version == 1 and presence & PRESENCE_CORRELATION:
            raise _corrupt("format v1 cannot carry a correlation coefficient", offset=cursor)
        if presence & PRESENCE_CORRELATION:
            if cursor + 8 > record_end:
                raise _corrupt("correlation coefficient is truncated", offset=cursor)
            (correlation,) = struct.unpack(f"{order}d", body[cursor : cursor + 8])
            cursor += 8
        else:
            correlation = 1.0
            if format_version >= 2:
                degradations.append(
                    degradation(
                        relation=relation_name,
                        field_name="correlation_coefficient",
                        action="default",
                        reason="optional correlation coefficient is absent",
                        new_value="1.000000000",
                    )
                )
            else:
                degradations.append(
                    degradation(
                        relation=relation_name,
                        field_name="correlation_coefficient",
                        action="default",
                        reason="format v1 omits correlation coefficients",
                        new_value="1.000000000",
                    )
                )
        stale_keys: list[str] = []
        if presence & PRESENCE_STALE_KEYS:
            if cursor + 2 > record_end:
                raise _corrupt("stale-key count is truncated", offset=cursor)
            (stale_count,) = struct.unpack(f"{order}H", body[cursor : cursor + 2])
            cursor += 2
            for _ in range(stale_count):
                if cursor + 2 > record_end:
                    raise _corrupt("stale-key length is truncated", offset=cursor)
                (key_length,) = struct.unpack(f"{order}H", body[cursor : cursor + 2])
                cursor += 2
                if cursor + key_length > record_end:
                    raise _corrupt("stale-key value is truncated", offset=cursor)
                try:
                    stale_keys.append(body[cursor : cursor + key_length].decode("utf-8"))
                except UnicodeDecodeError as exc:
                    raise _corrupt("stale key is not valid UTF-8", offset=cursor) from exc
                cursor += key_length
        if cursor != record_end:
            raise _corrupt(
                "record length does not match the decoded fields",
                offset=record_start - 4,
            )
        relation = normalize_relation(
            name=relation_name,
            row_count=row_count,
            ndv_ratio_value=ndv_ratio,
            correlation_value=correlation,
            stale_keys=stale_keys,
            source_kind="float32_ratio" if format_version == 1 else "float64_ratio",
            source_precision_places=7 if format_version == 1 else 15,
        )
        relations.append(relation)
        offset = record_end
    if offset != len(body):
        raise _corrupt("trailing bytes after final record", offset=offset)
    effective_decision = DEGRADE_ACCEPT if degradations else verdict.decision
    snapshot = finalize_snapshot(
        version=version,
        relations=relations,
        format_name=FORMAT_NAME,
        format_version=format_version,
        decision=effective_decision,
        degradations=degradations,
    )
    report = _report(
        version=format_version,
        decision=effective_decision,
        compatibility=verdict.public_dict(),
        degradations=degradations,
        digest=snapshot.digest,
        record_count=record_count,
        endian=endian,
        body_crc32=body_crc32,
    )
    report["snapshot_version"] = version
    report["normalization"] = snapshot.normalization
    return DecodedSnapshot(snapshot=snapshot, report=report)

