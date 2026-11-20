from __future__ import annotations

import json
import os
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .errors import FormatInvalid, InvalidArgument, StorageError
from .model import IndexEntry, Record
from .storage_utils import fsync_directory


SEGMENT_MAGIC = b"KVSG0001"
SEGMENT_FORMAT_VERSION = 1
FRAME_LENGTH_BYTES = 4
FRAME_CRC_BYTES = 4
_MAX_FRAME_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class SegmentHeader:
    format_version: int
    segment_id: str
    kind: str
    snapshot_seq: int
    generated_at_us: int
    header_end: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "format_version": self.format_version,
            "segment_id": self.segment_id,
            "kind": self.kind,
            "snapshot_seq": self.snapshot_seq,
            "generated_at_us": self.generated_at_us,
            "record_endianness": "big",
        }


@dataclass(frozen=True)
class AppendedRecord:
    record: Record
    offset: int
    frame_size: int


def _record_payload(record: Record) -> bytes:
    return json.dumps(
        record.payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def record_frame_size(record: Record) -> int:
    return FRAME_LENGTH_BYTES + len(_record_payload(record)) + FRAME_CRC_BYTES


def _frame(record: Record) -> bytes:
    payload = _record_payload(record)
    if len(payload) > _MAX_FRAME_BYTES:
        raise StorageError("record is too large for the segment format", {"key": record.key})
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return struct.pack(">I", len(payload)) + payload + struct.pack(">I", crc)


def _header_bytes(header: SegmentHeader) -> bytes:
    payload = json.dumps(
        header.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return SEGMENT_MAGIC + struct.pack(">I", len(payload)) + payload + struct.pack(">I", crc)


def create_segment(
    path: Path,
    *,
    segment_id: str,
    kind: str = "segment",
    snapshot_seq: int = 0,
    generated_at_us: int,
    records: Iterable[Record] = (),
) -> int:
    header = SegmentHeader(
        format_version=SEGMENT_FORMAT_VERSION,
        segment_id=segment_id,
        kind=kind,
        snapshot_seq=snapshot_seq,
        generated_at_us=generated_at_us,
        header_end=0,
    )
    header_bytes = _header_bytes(header)
    frame_bytes = b"".join(_frame(record) for record in records)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = None, None
    try:
        import tempfile

        fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        with os.fdopen(fd, "wb") as handle:
            handle.write(header_bytes)
            handle.write(frame_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        fsync_directory(path.parent)
    except Exception:
        if temp_name:
            try:
                Path(temp_name).unlink()
            except FileNotFoundError:
                pass
        raise
    return len(header_bytes) + len(frame_bytes)


def read_header(path: Path) -> SegmentHeader:
    try:
        with path.open("rb") as handle:
            fixed = handle.read(12)
            if len(fixed) != 12 or fixed[:8] != SEGMENT_MAGIC:
                raise FormatInvalid("segment magic is invalid", {"path": str(path)})
            payload_len = struct.unpack(">I", fixed[8:12])[0]
            if payload_len > 1024 * 1024:
                raise FormatInvalid("segment header is unreasonably large", {"path": str(path)})
            payload = handle.read(payload_len)
            crc_bytes = handle.read(4)
            if len(payload) != payload_len or len(crc_bytes) != 4:
                raise FormatInvalid("segment header is truncated", {"path": str(path)})
            expected_crc = struct.unpack(">I", crc_bytes)[0]
            actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
            if expected_crc != actual_crc:
                raise FormatInvalid("segment header CRC32 mismatch", {"path": str(path)})
            try:
                header_obj = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise FormatInvalid("segment header JSON is malformed", {"path": str(path)}) from exc
            if header_obj.get("format_version") != SEGMENT_FORMAT_VERSION:
                raise FormatInvalid(
                    "unsupported segment format version",
                    {"path": str(path), "format_version": header_obj.get("format_version")},
                )
            header = SegmentHeader(
                format_version=header_obj["format_version"],
                segment_id=header_obj["segment_id"],
                kind=header_obj.get("kind", "segment"),
                snapshot_seq=int(header_obj.get("snapshot_seq", 0)),
                generated_at_us=int(header_obj["generated_at_us"]),
                header_end=12 + payload_len + 4,
            )
            if header.kind not in {"segment", "snapshot"}:
                raise FormatInvalid("unknown segment kind", {"path": str(path), "kind": header.kind})
            return header
    except FileNotFoundError as exc:
        raise StorageError("segment file does not exist", {"path": str(path)}) from exc


def append_records(path: Path, records: Iterable[Record]) -> Tuple[int, List[AppendedRecord]]:
    materialized = list(records)
    read_header(path)
    appended: List[AppendedRecord] = []
    try:
        with path.open("r+b") as handle:
            handle.seek(0, os.SEEK_END)
            offset = handle.tell()
            start_offset = offset
            for record in materialized:
                frame = _frame(record)
                handle.write(frame)
                appended.append(AppendedRecord(record=record, offset=offset, frame_size=len(frame)))
                offset += len(frame)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception as exc:
        raise StorageError("failed while appending to segment", {"path": str(path), "reason": str(exc)}) from exc
    return start_offset, appended


def read_record_at(path: Path, offset: int, expected_key: Optional[str] = None) -> Tuple[Record, int]:
    try:
        with path.open("rb") as handle:
            handle.seek(offset)
            length_bytes = handle.read(4)
            if len(length_bytes) != 4:
                raise FormatInvalid("record length prefix is truncated", {"path": str(path), "offset": offset})
            payload_len = struct.unpack(">I", length_bytes)[0]
            if payload_len > _MAX_FRAME_BYTES:
                raise FormatInvalid("record frame is unreasonably large", {"path": str(path), "offset": offset})
            payload = handle.read(payload_len)
            crc_bytes = handle.read(4)
            if len(payload) != payload_len or len(crc_bytes) != 4:
                raise FormatInvalid("record frame is truncated", {"path": str(path), "offset": offset})
            expected_crc = struct.unpack(">I", crc_bytes)[0]
            actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
            if expected_crc != actual_crc:
                raise FormatInvalid(
                    "record CRC32 mismatch",
                    {
                        "path": str(path),
                        "offset": offset,
                        "expected_crc32": expected_crc,
                        "actual_crc32": actual_crc,
                    },
                )
    except FileNotFoundError as exc:
        raise FormatInvalid("segment file disappeared during read", {"path": str(path)}) from exc
    try:
        payload_obj = json.loads(payload.decode("utf-8"))
        record = Record.from_payload(payload_obj)
    except (UnicodeDecodeError, json.JSONDecodeError, FormatInvalid, InvalidArgument, TypeError) as exc:
        raise FormatInvalid("segment record payload is malformed", {"path": str(path), "offset": offset}) from exc
    if expected_key is not None and record.key != expected_key:
        raise FormatInvalid(
            "index offset points to a different key",
            {"path": str(path), "offset": offset, "expected_key": expected_key, "actual_key": record.key},
        )
    return record, FRAME_LENGTH_BYTES + payload_len + FRAME_CRC_BYTES


def iter_records(path: Path) -> Iterator[Tuple[Record, int, int]]:
    header = read_header(path)
    file_size = path.stat().st_size
    offset = header.header_end
    if offset > file_size:
        raise FormatInvalid("segment header extends beyond end of file", {"path": str(path)})
    while offset < file_size:
        frame_start = offset
        record, frame_size = read_record_at(path, frame_start)
        yield record, frame_start, frame_size
        offset += frame_size


def rebuild_index(path: Path) -> Tuple[SegmentHeader, Dict[str, IndexEntry], int]:
    header = read_header(path)
    index: Dict[str, IndexEntry] = {}
    record_count = 0
    for record, offset, frame_size in iter_records(path):
        record_count += 1
        index[record.key] = IndexEntry(
            key=record.key,
            version=record.version,
            written_at_us=record.written_at_us,
            expires_at_us=record.expires_at_us,
            deleted=record.deleted,
            segment_path=str(path),
            offset=offset,
            frame_size=frame_size,
        )
    return header, index, record_count


def truncate_to(path: Path, size: int) -> None:
    if not path.exists():
        return
    current = path.stat().st_size
    if current <= size:
        return
    with path.open("r+b") as handle:
        handle.truncate(size)
        handle.flush()
        os.fsync(handle.fileno())


def sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
