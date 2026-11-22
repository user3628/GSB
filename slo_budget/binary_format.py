"""Compact binary snapshot codec.

Layout
------
Header (40 bytes):
  0..8   magic ``SLOSNAP\\x00``
  8      format version (u8)
  9      endian marker: 0x4C for little, 0x42 for big
  10..12 header flags (u16)
  12..20 snapshot sequence (u64)
  20..28 generated-at milliseconds (u64)
  28..32 record count (u32)
  32..36 CRC32 of bytes 0..32 (u32)
  36..40 reserved, must be zero

Record:
  4-byte length prefix (bytes after the prefix, including final CRC)
  body (length - 4)
  4-byte CRC32 of body

Body:
  u32 optional-field bitmap
  u16 service length + UTF-8 service
  u16 SLI-type length + UTF-8 SLI type
  version-dependent time/count fields

Version 1 stores ``window_end_ago_ms`` and omits late_count.  Version 2 stores
``window_start_ms`` and requires late_count.  Version 3 is intentionally rejected
by the compatibility layer as an unknown required-field schema.
"""

from __future__ import annotations

import hashlib
import os
import struct
import tempfile
import zlib
from pathlib import Path

from .compat import CURRENT_FORMAT_VERSION, FUTURE_FORMAT_VERSION, negotiate
from .errors import SloError, corrupt
from .models import (
    NegotiationResult,
    ParsedBatch,
    SnapshotManifest,
    WindowRecord,
    sorted_records,
)
from .policy import BoundaryPolicy
from .timeutil import binary_ms_rounding_info


MAGIC = b"SLOSNAP\x00"
HEADER_SIZE = 40
LITTLE_ENDIAN_MARKER = 0x4C
BIG_ENDIAN_MARKER = 0x42
FLAG_REQUIRED_EXTENSIONS = 0x0001
BIT_LATE_COUNT = 0x00000001
BIT_LAST_WRITE = 0x00000002
BIT_UNKNOWN_REQUIRED = 0x80000000


def _endian_prefix(value: str) -> tuple[str, int]:
    normalized = value.lower()
    if normalized in {"little", "<", "le"}:
        return "<", LITTLE_ENDIAN_MARKER
    if normalized in {"big", ">", "be"}:
        return ">", BIG_ENDIAN_MARKER
    raise SloError(
        "E_CONFIG_INVALID",
        "endian must be one of little, big, <, >",
        details={"endian": value},
    )


def _header_crc(prefix: bytes) -> int:
    return zlib.crc32(prefix) & 0xFFFFFFFF


def _read_header(data: bytes, path: str) -> tuple[SnapshotManifest, str, int]:
    if len(data) < 10:
        raise corrupt(
            path,
            "truncated binary signature/version/endian header",
            len(data),
            details={"required": 10, "available": len(data)},
        )
    if data[:8] != MAGIC:
        raise SloError(
            "E_FORMAT_UNSUPPORTED",
            "binary magic does not match SLOSNAP",
            path=path,
            offset=0,
            details={"expected_magic_hex": MAGIC.hex(), "actual_hex": data[:8].hex()},
        )
    version = data[8]
    marker = data[9]
    if marker == LITTLE_ENDIAN_MARKER:
        endian = "<"
    elif marker == BIG_ENDIAN_MARKER:
        endian = ">"
    else:
        raise corrupt(
            path,
            "invalid endian marker",
            9,
            details={"marker": marker},
        )
    if len(data) < HEADER_SIZE:
        raise corrupt(
            path,
            "truncated binary header",
            len(data),
            details={"required": HEADER_SIZE, "available": len(data)},
        )
    flags, sequence, generated_at_ms, record_count = struct.unpack_from(
        f"{endian}HQQI", data, 10
    )
    stored_crc = struct.unpack_from(f"{endian}I", data, 32)[0]
    reserved = struct.unpack_from(f"{endian}I", data, 36)[0]
    calculated_crc = _header_crc(data[:32])
    if stored_crc != calculated_crc:
        raise corrupt(
            path,
            "binary header CRC32 mismatch",
            32,
            details={"stored": stored_crc, "calculated": calculated_crc},
        )
    if reserved != 0:
        raise corrupt(
            path,
            "binary header reserved bytes must be zero",
            36,
            details={"reserved": reserved},
        )
    required = ("header.required_extension_bit",) if flags & FLAG_REQUIRED_EXTENSIONS else ()
    manifest = SnapshotManifest(
        file_format="binary",
        format_version=version,
        snapshot_sequence=sequence,
        generated_at_ms=generated_at_ms,
        record_count=record_count,
        required_extensions=required,
        source_path=path,
        header_size=HEADER_SIZE,
    )
    return manifest, endian, flags


def preflight_binary(path: str | Path, _boundaries: BoundaryPolicy | None = None) -> NegotiationResult:
    source = str(path)
    try:
        with open(path, "rb") as handle:
            data = handle.read(HEADER_SIZE + 1)
    except OSError as exc:
        raise SloError("E_IO", f"cannot read {source}: {exc}", path=source) from exc
    manifest, _endian, _flags = _read_header(data, source)
    return negotiate("binary", manifest)


def _require(data: bytes, pos: int, size: int, path: str, what: str) -> None:
    if size < 0 or pos + size > len(data):
        raise corrupt(
            path,
            f"truncated {what}",
            min(pos, len(data)),
            details={"required": size, "available": max(0, len(data) - pos)},
        )


def _decode_utf8(
    raw: bytes,
    *,
    path: str,
    offset: int,
    field_name: str,
    limit_bytes: int,
) -> str:
    if len(raw) > limit_bytes:
        raise corrupt(
            path,
            f"{field_name} byte length is out of bounds",
            offset,
            details={"length": len(raw), "maximum": limit_bytes},
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise corrupt(
            path,
            f"{field_name} is not valid UTF-8",
            offset + exc.start,
            details={"field": field_name},
        ) from exc
    if not text:
        raise corrupt(
            path,
            f"{field_name} must not be empty",
            offset,
            details={"field": field_name},
        )
    return text


def _validate_counts(
    *,
    path: str,
    offset: int,
    good: int,
    total: int,
    late: int,
) -> None:
    if good > total:
        raise SloError(
            "E_COUNT_INVALID",
            "good_events must not exceed total_events",
            path=path,
            offset=offset,
            details={"good_events": good, "total_events": total},
        )
    if late > total:
        raise SloError(
            "E_COUNT_INVALID",
            "late_count must not exceed total_events",
            path=path,
            offset=offset,
            details={"late_count": late, "total_events": total},
        )


def decode_binary(
    path: str | Path,
    *,
    truncation_rule: str = "floor",
    boundaries: BoundaryPolicy | None = None,
) -> ParsedBatch:
    source = str(path)
    boundaries = boundaries or BoundaryPolicy()
    boundaries.validate()
    try:
        data = Path(path).read_bytes()
    except OSError as exc:
        raise SloError("E_IO", f"cannot read {source}: {exc}", path=source) from exc
    manifest, endian, _flags = _read_header(data, source)
    negotiation = negotiate("binary", manifest)
    if negotiation.verdict == "REJECT":
        raise SloError(
            negotiation.code or "E_FORMAT_UNSUPPORTED",
            "binary format rejected by compatibility matrix",
            path=source,
            offset=8,
            details=negotiation.to_dict(),
        )

    pos = HEADER_SIZE
    records: list[WindowRecord] = []
    rounding = []
    degradations: list[dict] = []
    seen_keys: dict[tuple[str, str, int], int] = {}

    for index in range(manifest.record_count):
        record_start = pos
        _require(data, pos, 4, source, f"record {index} length prefix")
        record_length = struct.unpack_from(f"{endian}I", data, pos)[0]
        pos += 4
        if record_length < 4:
            raise corrupt(
                source,
                "record length prefix is smaller than its CRC32 field",
                record_start,
                details={"record_index": index, "record_length": record_length},
            )
        if pos + record_length > len(data):
            raise corrupt(
                source,
                "record extends past end of file",
                record_start,
                details={
                    "record_index": index,
                    "record_length": record_length,
                    "available_after_prefix": len(data) - pos,
                },
            )
        body_end = pos + record_length - 4
        body = data[pos:body_end]
        stored_crc = struct.unpack_from(f"{endian}I", data, body_end)[0]
        calculated_crc = zlib.crc32(body) & 0xFFFFFFFF
        if stored_crc != calculated_crc:
            raise corrupt(
                source,
                "record CRC32 mismatch",
                body_end,
                details={
                    "record_index": index,
                    "stored": stored_crc,
                    "calculated": calculated_crc,
                },
            )

        body_pos = 0
        _require(body, body_pos, 4, source, "optional-field bitmap")
        bitmap = struct.unpack_from(f"{endian}I", body, body_pos)[0]
        bitmap_offset = pos + body_pos
        body_pos += 4
        if bitmap & BIT_UNKNOWN_REQUIRED:
            raise SloError(
                "E_FORMAT_UNSUPPORTED",
                "record requests an unknown required extension",
                path=source,
                offset=bitmap_offset,
                details={"bitmap_hex": f"0x{bitmap:08x}", "record_index": index},
            )
        unknown_optional = bitmap & ~(BIT_LATE_COUNT | BIT_LAST_WRITE | BIT_UNKNOWN_REQUIRED)
        if unknown_optional:
            raise SloError(
                "E_FORMAT_UNSUPPORTED",
                "record contains unsupported optional-field bitmap bits",
                path=source,
                offset=bitmap_offset,
                details={"bitmap_hex": f"0x{bitmap:08x}", "unknown_bits": unknown_optional},
            )

        _require(body, body_pos, 2, source, "service-name length")
        service_len = struct.unpack_from(f"{endian}H", body, body_pos)[0]
        body_pos += 2
        _require(body, body_pos, service_len, source, "service-name bytes")
        service_offset = pos + body_pos
        service = _decode_utf8(
            body[body_pos : body_pos + service_len],
            path=source,
            offset=service_offset,
            field_name="service",
            limit_bytes=boundaries.max_service_name_bytes,
        )
        body_pos += service_len

        _require(body, body_pos, 2, source, "SLI-type length")
        sli_len = struct.unpack_from(f"{endian}H", body, body_pos)[0]
        body_pos += 2
        _require(body, body_pos, sli_len, source, "SLI-type bytes")
        sli_offset = pos + body_pos
        sli_type = _decode_utf8(
            body[body_pos : body_pos + sli_len],
            path=source,
            offset=sli_offset,
            field_name="sli_type",
            limit_bytes=boundaries.max_sli_type_bytes,
        )
        body_pos += sli_len

        if manifest.format_version == 1:
            _require(body, body_pos, 8 + 4 + 8 + 8, source, "version 1 record fields")
            ago_offset = pos + body_pos
            window_end_ago_ms = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
            body_pos += 8
            length_offset = pos + body_pos
            window_length_ms = struct.unpack_from(f"{endian}I", body, body_pos)[0]
            body_pos += 4
            good_offset = pos + body_pos
            good_events = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
            body_pos += 8
            total_events = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
            body_pos += 8

            late_count = 0
            late_degraded = True
            if bitmap & BIT_LATE_COUNT:
                _require(body, body_pos, 4, source, "late_count")
                late_count = struct.unpack_from(f"{endian}I", body, body_pos)[0]
                body_pos += 4
                late_degraded = False
            last_write_ms = manifest.generated_at_ms
            if bitmap & BIT_LAST_WRITE:
                _require(body, body_pos, 8, source, "last_write_ms")
                last_write_ms = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
                body_pos += 8

            absolute_end_ms = manifest.generated_at_ms - window_end_ago_ms
            window_start_ms = absolute_end_ms - window_length_ms
            if window_start_ms < 0:
                raise corrupt(
                    source,
                    "version 1 relative window resolves before the Unix epoch",
                    ago_offset,
                    details={
                        "generated_at_ms": manifest.generated_at_ms,
                        "window_end_ago_ms": window_end_ago_ms,
                        "window_length_ms": window_length_ms,
                    },
                )
            time_rounding = binary_ms_rounding_info(
                key=(service, sli_type, window_start_ms),
                source_field="window_end_ago_ms",
                source_value=window_end_ago_ms,
                normalized_ms=window_start_ms,
                offset=ago_offset,
            )
            actions: list[dict] = [
                {
                    "action": "relative_time_converted",
                    "rule": "generated_at_ms - window_end_ago_ms - window_length_ms",
                    "source": {
                        "generated_at_ms": manifest.generated_at_ms,
                        "window_end_ago_ms": window_end_ago_ms,
                        "window_length_ms": window_length_ms,
                    },
                    "result_window_start_ms": window_start_ms,
                }
            ]
            if late_degraded:
                actions.append(
                    {
                        "action": "late_count_defaulted",
                        "from": None,
                        "to": 0,
                        "rule": "version_1_missing_late_count",
                    }
                )
            degradations.append(
                {
                    "record_index": index,
                    "offset": record_start,
                    "key": [service, sli_type, window_start_ms],
                    "actions": actions,
                }
            )
        else:
            _require(body, body_pos, 8 + 4 + 8 + 8, source, "version 2 record fields")
            start_offset = pos + body_pos
            window_start_ms = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
            body_pos += 8
            length_offset = pos + body_pos
            window_length_ms = struct.unpack_from(f"{endian}I", body, body_pos)[0]
            body_pos += 4
            good_offset = pos + body_pos
            good_events = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
            body_pos += 8
            total_offset = pos + body_pos
            total_events = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
            body_pos += 8
            if not (bitmap & BIT_LATE_COUNT):
                raise corrupt(
                    source,
                    "version 2 record is missing required late_count",
                    bitmap_offset,
                    details={"record_index": index, "required_bit": BIT_LATE_COUNT},
                )
            _require(body, body_pos, 4, source, "late_count")
            late_count = struct.unpack_from(f"{endian}I", body, body_pos)[0]
            body_pos += 4
            last_write_ms = manifest.generated_at_ms
            if bitmap & BIT_LAST_WRITE:
                _require(body, body_pos, 8, source, "last_write_ms")
                last_write_ms = struct.unpack_from(f"{endian}Q", body, body_pos)[0]
                body_pos += 8
            time_rounding = binary_ms_rounding_info(
                key=(service, sli_type, window_start_ms),
                source_field="window_start_ms",
                source_value=window_start_ms,
                normalized_ms=window_start_ms,
                offset=start_offset,
            )

        if body_pos != len(body):
            raise corrupt(
                source,
                "record body has trailing bytes not described by its bitmap",
                pos + body_pos,
                details={
                    "record_index": index,
                    "trailing_bytes": len(body) - body_pos,
                },
            )
        if window_length_ms <= 0 or window_length_ms > boundaries.max_window_length_ms:
            raise SloError(
                "E_RECORD_CORRUPT",
                "window_length_ms is outside declared bounds",
                path=source,
                offset=length_offset,
                details={
                    "window_length_ms": window_length_ms,
                    "minimum": 1,
                    "maximum": boundaries.max_window_length_ms,
                },
            )
        _validate_counts(
            path=source,
            offset=good_offset,
            good=good_events,
            total=total_events,
            late=late_count,
        )

        key = (service, sli_type, window_start_ms)
        if key in seen_keys:
            raise corrupt(
                source,
                "duplicate record key in snapshot",
                record_start,
                details={
                    "record_index": index,
                    "first_offset": seen_keys[key],
                    "key": list(key),
                },
            )
        seen_keys[key] = record_start
        records.append(
            WindowRecord(
                service=service,
                sli_type=sli_type,
                window_start_ms=window_start_ms,
                window_length_ms=window_length_ms,
                good_events=good_events,
                total_events=total_events,
                late_count=late_count,
                last_write_ms=last_write_ms,
                format_version=manifest.format_version,
                snapshot_sequence=manifest.snapshot_sequence,
                source_format="binary",
                source_offset=record_start,
            )
        )
        rounding.append(time_rounding)
        pos = body_end + 4

    if pos != len(data):
        raise corrupt(
            source,
            "trailing bytes after declared binary records",
            pos,
            details={"remaining": len(data) - pos, "declared_records": manifest.record_count},
        )
    if len(records) != manifest.record_count:
        raise corrupt(
            source,
            "binary record count does not match header",
            pos,
            details={"declared": manifest.record_count, "decoded": len(records)},
        )

    return ParsedBatch(
        manifest=manifest,
        negotiation=negotiation,
        records=tuple(records),
        rounding=tuple(rounding),
        cumulative_rounding_error_us=0,
        degradations=tuple(degradations),
        checksum_sha256=hashlib.sha256(data).hexdigest(),
    )


def encode_binary_bytes(
    records: tuple[WindowRecord, ...] | list[WindowRecord],
    manifest: SnapshotManifest,
    *,
    endian: str = "little",
    version: int = CURRENT_FORMAT_VERSION,
    required_extension: bytes = b"tenant-shard-v3",
) -> bytes:
    prefix, marker = _endian_prefix(endian)
    ordered = sorted_records(records)
    flags = FLAG_REQUIRED_EXTENSIONS if required_extension and version >= FUTURE_FORMAT_VERSION else 0
    header_prefix = (
        MAGIC
        + bytes((version, marker))
        + struct.pack(
            f"{prefix}HQQI",
            flags,
            manifest.snapshot_sequence,
            manifest.generated_at_ms,
            len(ordered),
        )
    )
    assert len(header_prefix) == 32
    header = (
        header_prefix
        + struct.pack(f"{prefix}I", _header_crc(header_prefix))
        + struct.pack(f"{prefix}I", 0)
    )

    chunks = [header]
    for record in ordered:
        bitmap = 0
        fields = bytearray()
        service = record.service.encode("utf-8")
        sli_type = record.sli_type.encode("utf-8")

        if version == 1:
            ended_ago_ms = manifest.generated_at_ms - record.window_end_ms
            if ended_ago_ms < 0:
                raise SloError(
                    "E_SNAPSHOT_INVALID",
                    "version 1 export cannot represent a window ending after generated_at_ms",
                    details={
                        "window_end_ms": record.window_end_ms,
                        "generated_at_ms": manifest.generated_at_ms,
                    },
                )
            fields.extend(struct.pack(f"{prefix}Q", ended_ago_ms))
            fields.extend(struct.pack(f"{prefix}I", record.window_length_ms))
            fields.extend(struct.pack(f"{prefix}Q", record.good_events))
            fields.extend(struct.pack(f"{prefix}Q", record.total_events))
            if record.last_write_ms:
                bitmap |= BIT_LAST_WRITE
                fields.extend(struct.pack(f"{prefix}Q", record.last_write_ms))
        else:
            fields.extend(struct.pack(f"{prefix}Q", record.window_start_ms))
            fields.extend(struct.pack(f"{prefix}I", record.window_length_ms))
            fields.extend(struct.pack(f"{prefix}Q", record.good_events))
            fields.extend(struct.pack(f"{prefix}Q", record.total_events))
            bitmap |= BIT_LATE_COUNT
            fields.extend(struct.pack(f"{prefix}I", record.late_count))
            if record.last_write_ms:
                bitmap |= BIT_LAST_WRITE
                fields.extend(struct.pack(f"{prefix}Q", record.last_write_ms))

        if required_extension and version >= FUTURE_FORMAT_VERSION:
            bitmap |= BIT_UNKNOWN_REQUIRED
            fields.extend(struct.pack(f"{prefix}H", len(required_extension)))
            fields.extend(required_extension)

        body = (
            struct.pack(f"{prefix}I", bitmap)
            + struct.pack(f"{prefix}H", len(service))
            + service
            + struct.pack(f"{prefix}H", len(sli_type))
            + sli_type
            + bytes(fields)
        )
        record_length = len(body) + 4
        chunks.append(struct.pack(f"{prefix}I", record_length))
        chunks.append(body)
        chunks.append(struct.pack(f"{prefix}I", zlib.crc32(body) & 0xFFFFFFFF))
    return b"".join(chunks)


def write_binary_snapshot(
    path: str | Path,
    records: tuple[WindowRecord, ...] | list[WindowRecord],
    manifest: SnapshotManifest,
    *,
    endian: str = "little",
    version: int = CURRENT_FORMAT_VERSION,
) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = encode_binary_bytes(records, manifest, endian=endian, version=version)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
    return hashlib.sha256(payload).hexdigest()
