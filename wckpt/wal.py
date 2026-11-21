"""Append-only WAL with rotating, checksummed, truncatable segment files."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .errors import WalCorruptError
from .models import WalRecord
from .util import (
    MAGIC_FOOTER,
    MAGIC_HEADER,
    MAGIC_RECORD,
    canonical_json,
    crc32_hex,
    fsync_dir,
)


@dataclass
class SegmentInfo:
    seg_id: int
    path: str
    start_lsn: int
    end_lsn: int
    record_count: int
    sealed: bool
    corrupt: bool
    empty: bool = False
    error: str = ""

    @property
    def name(self) -> str:
        return os.path.basename(self.path)


class WalManager:
    def __init__(
        self,
        wal_dir: str,
        segment_max_bytes: int,
        fsync_wal: bool = True,
        fsync_dirs: bool = True,
        crash_hook=None,
    ):
        self.wal_dir = wal_dir
        self.segment_max_bytes = segment_max_bytes
        self.fsync_wal = fsync_wal
        self.fsync_dirs = fsync_dirs
        # Optional callable(stage: str) used by crash tests to kill the process.
        self.crash_hook = crash_hook
        os.makedirs(self.wal_dir, exist_ok=True)
        self._segments: List[SegmentInfo] = []
        self._open_file = None
        self._open_seg: Optional[SegmentInfo] = None
        self._next_lsn = 1
        self._scan()

    # ------------------------------------------------------------- naming
    def _seg_path(self, seg_id: int) -> str:
        return os.path.join(self.wal_dir, f"seg_{seg_id:010d}.wal")

    @staticmethod
    def seg_id_from_name(name: str) -> Optional[int]:
        if not (name.startswith("seg_") and name.endswith(".wal")):
            return None
        try:
            return int(name[4:-4])
        except ValueError:
            return None

    # ------------------------------------------------------------- scanning
    def _scan(self):
        """Read every segment, validate it and rebuild the LSN high-water mark."""
        self._segments = []
        max_lsn = 0
        names = sorted(
            n for n in os.listdir(self.wal_dir) if self.seg_id_from_name(n) is not None
        )
        for name in names:
            path = os.path.join(self.wal_dir, name)
            info = self._validate_segment(path)
            if info.empty:
                # A rotation-created segment that never received a record because
                # the process was killed is harmless; drop it from active tracking.
                try:
                    os.remove(path)
                    if self.fsync_dirs:
                        fsync_dir(self.wal_dir)
                except OSError:
                    pass
                continue
            self._segments.append(info)
            if not info.corrupt and info.record_count:
                max_lsn = max(max_lsn, info.end_lsn)
        self._next_lsn = max_lsn + 1
        self._open_seg = None
        self._open_file = None

    def _validate_segment(self, path: str) -> SegmentInfo:
        seg_id = self.seg_id_from_name(os.path.basename(path))
        try:
            with open(path, "r", encoding="utf-8") as fh:
                lines = [ln for ln in (line.strip() for line in fh) if ln]
            if not lines:
                raise ValueError("empty segment")
            header = json.loads(lines[0])
            if header.get("magic") != MAGIC_HEADER:
                raise ValueError("bad segment header magic")
            header_payload = {k: v for k, v in header.items() if k != "crc"}
            if crc32_hex(header_payload) != header.get("crc"):
                raise ValueError("segment header checksum mismatch")
            start_lsn = int(header["start_lsn"])
            if int(header["seg_id"]) != seg_id:
                raise ValueError("segment id mismatch")

            body_end = len(lines)
            sealed = False
            if len(lines) > 1 and json.loads(lines[-1]).get("magic") == MAGIC_FOOTER:
                sealed = True
                body_end = len(lines) - 1

            records: List[WalRecord] = []
            prev_lsn = None
            for line in lines[1:body_end]:
                obj = json.loads(line)
                if obj.get("magic") != MAGIC_RECORD:
                    raise ValueError("unexpected line in segment body")
                rec = WalRecord.from_dict(obj)
                if prev_lsn is not None and rec.lsn != prev_lsn + 1:
                    raise ValueError(f"LSN gap inside segment near lsn={rec.lsn}")
                if prev_lsn is None and rec.lsn != start_lsn:
                    raise ValueError("header start_lsn does not match first record")
                records.append(rec)
                prev_lsn = rec.lsn

            if sealed:
                footer = json.loads(lines[-1])
                if int(footer.get("seg_id")) != seg_id:
                    raise ValueError("footer segment id mismatch")
                if int(footer.get("record_count")) != len(records):
                    raise ValueError("footer record count mismatch")
                if int(footer.get("end_lsn")) != (records[-1].lsn if records else start_lsn - 1):
                    raise ValueError("footer end_lsn mismatch")
                fp = {k: v for k, v in footer.items() if k != "crc"}
                if crc32_hex(fp) != footer.get("crc"):
                    raise ValueError("footer checksum mismatch")

            if not records:
                # Valid header but zero records: an in-progress rotation interrupted
                # by a hard crash. It carries no LSN and is safe to ignore.
                return SegmentInfo(
                    seg_id=seg_id,
                    path=path,
                    start_lsn=start_lsn,
                    end_lsn=start_lsn - 1,
                    record_count=0,
                    sealed=sealed,
                    corrupt=False,
                    empty=True,
                )
            return SegmentInfo(
                seg_id=seg_id,
                path=path,
                start_lsn=start_lsn,
                end_lsn=records[-1].lsn,
                record_count=len(records),
                sealed=sealed,
                corrupt=False,
            )
        except (ValueError, KeyError, json.JSONDecodeError, OSError) as exc:
            return SegmentInfo(
                seg_id=seg_id,
                path=path,
                start_lsn=-1,
                end_lsn=-1,
                record_count=0,
                sealed=False,
                corrupt=True,
                error=str(exc),
            )

    # ------------------------------------------------------------- properties
    @property
    def next_lsn(self) -> int:
        return self._next_lsn

    def segments(self) -> List[SegmentInfo]:
        return list(self._segments)

    def healthy_segments(self) -> List[SegmentInfo]:
        return [s for s in self._segments if not s.corrupt]

    def max_durable_lsn(self) -> int:
        return self._next_lsn - 1

    # ------------------------------------------------------------- append
    def _new_segment(self, seg_id: int, start_lsn: int):
        path = self._seg_path(seg_id)
        header = {
            "magic": MAGIC_HEADER,
            "seg_id": seg_id,
            "start_lsn": start_lsn,
        }
        header["crc"] = crc32_hex(header)
        fh = open(path, "a", encoding="utf-8")
        fh.write(json.dumps(header, sort_keys=True) + "\n")
        fh.flush()
        if self.fsync_wal:
            os.fsync(fh.fileno())
        if self.fsync_dirs:
            fsync_dir(self.wal_dir)
        info = SegmentInfo(
            seg_id=seg_id,
            path=path,
            start_lsn=start_lsn,
            end_lsn=start_lsn - 1,
            record_count=0,
            sealed=False,
            corrupt=False,
        )
        self._segments.append(info)
        self._open_seg = info
        self._open_file = fh

    def _next_seg_id(self) -> int:
        return (max((s.seg_id for s in self._segments), default=0) + 1)

    def _should_rotate(self) -> bool:
        if self._open_file is None:
            return True
        try:
            size = os.path.getsize(self._open_seg.path)
        except OSError:
            size = 0
        return size >= self.segment_max_bytes and self._open_seg.record_count > 0

    def _seal_current(self):
        if self._open_file is None or self._open_seg is None:
            return
        info = self._open_seg
        footer = {
            "magic": MAGIC_FOOTER,
            "seg_id": info.seg_id,
            "start_lsn": info.start_lsn,
            "end_lsn": info.end_lsn,
            "record_count": info.record_count,
        }
        footer["crc"] = crc32_hex(footer)
        self._open_file.write(json.dumps(footer, sort_keys=True) + "\n")
        self._open_file.flush()
        if self.fsync_wal:
            os.fsync(self._open_file.fileno())
        self._open_file.close()
        info.sealed = True
        self._open_file = None
        self._open_seg = None

    def append(self, record: WalRecord):
        """Persist one record; LSN is assigned by the caller and must be next_lsn."""
        if record.lsn != self._next_lsn:
            raise WalCorruptError(
                "non-contiguous LSN append",
                {"expected_lsn": self._next_lsn, "actual_lsn": record.lsn},
            )
        if self._should_rotate():
            if self._open_file is not None:
                self._seal_current()
            self._new_segment(self._next_seg_id(), record.lsn)

        line_obj = dict(record.to_dict())
        line_obj["magic"] = MAGIC_RECORD
        ordered = {
            "magic": line_obj["magic"],
            "lsn": line_obj["lsn"],
            "page_no": line_obj["page_no"],
            "before_digest": line_obj["before_digest"],
            "after_digest": line_obj["after_digest"],
            "txn_id": line_obj["txn_id"],
            "ts": line_obj["ts"],
            "crc": line_obj["crc"],
        }
        if self.crash_hook is not None:
            self.crash_hook("wal_before_write")
        self._open_file.write(json.dumps(ordered, sort_keys=True) + "\n")
        self._open_file.flush()
        if self.fsync_wal:
            os.fsync(self._open_file.fileno())
        if self.crash_hook is not None:
            self.crash_hook("wal_after_write")

        info = self._open_seg
        info.end_lsn = record.lsn
        info.record_count += 1
        self._next_lsn += 1

    def close(self):
        if self._open_file is not None:
            self._seal_current()

    def flush(self):
        if self._open_file is not None:
            self._open_file.flush()
            if self.fsync_wal:
                os.fsync(self._open_file.fileno())

    # ------------------------------------------------------------- reading
    def _iter_segment_records(self, info: SegmentInfo) -> Iterator[WalRecord]:
        if info.corrupt:
            raise WalCorruptError(
                f"segment {info.name} is corrupt and cannot participate in replay",
                {"segment": info.name, "reason": info.error},
            )
        with open(info.path, "r", encoding="utf-8") as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]
        body_end = len(lines)
        if json.loads(lines[-1]).get("magic") == MAGIC_FOOTER:
            body_end -= 1
        for line in lines[1:body_end]:
            obj = json.loads(line)
            yield WalRecord.from_dict(obj)

    def iter_records(
        self, from_lsn: Optional[int] = None, to_lsn: Optional[int] = None
    ) -> Iterator[WalRecord]:
        for info in self._segments:
            if info.corrupt:
                raise WalCorruptError(
                    f"segment {info.name} is corrupt and cannot participate in replay",
                    {"segment": info.name, "reason": info.error},
                )
            if to_lsn is not None and info.start_lsn > to_lsn:
                return
            if from_lsn is not None and info.end_lsn < from_lsn:
                continue
            for rec in self._iter_segment_records(info):
                if from_lsn is not None and rec.lsn < from_lsn:
                    continue
                if to_lsn is not None and rec.lsn > to_lsn:
                    return
                yield rec

    def segment_of(self, lsn: int) -> SegmentInfo:
        for info in self._segments:
            if not info.corrupt and info.start_lsn <= lsn <= info.end_lsn:
                return info
        raise WalCorruptError("lsn not found in any healthy segment", {"lsn": lsn})

    def range_query(self, from_lsn: int, to_lsn: int) -> List[Dict[str, Any]]:
        out = []
        for rec in self.iter_records(from_lsn, to_lsn):
            out.append(
                {
                    "record": rec.to_dict(),
                    "segment": self.segment_of(rec.lsn).name,
                    "seg_id": self.segment_of(rec.lsn).seg_id,
                }
            )
        return out

    def count_records(self, from_lsn: Optional[int] = None) -> Tuple[int, set]:
        count = 0
        pages = set()
        for rec in self.iter_records(from_lsn=from_lsn):
            count += 1
            pages.add(rec.page_no)
        return count, pages

    # ------------------------------------------------------------- truncation
    def truncatable_segments(self, safe_lsn: int) -> List[SegmentInfo]:
        """A segment is reusable once its last record LSN < safe_lsn."""
        return [
            s
            for s in self._segments
            if not s.corrupt and s.record_count and s.end_lsn < safe_lsn
        ]

    def truncate(self, safe_lsn: int, archive_dir: Optional[str] = None) -> List[str]:
        """Archive then remove every segment fully covered by safe_lsn."""
        archived = []
        for info in list(self.truncatable_segments(safe_lsn)):
            target = info.path
            if archive_dir:
                os.makedirs(archive_dir, exist_ok=True)
                dest = os.path.join(archive_dir, os.path.basename(info.path))
                os.replace(info.path, dest)
                target = dest
            else:
                os.remove(info.path)
            if self.fsync_dirs:
                fsync_dir(self.wal_dir)
                if archive_dir:
                    fsync_dir(archive_dir)
            archived.append(os.path.basename(info.path))
            self._segments = [s for s in self._segments if s.seg_id != info.seg_id]
        return archived

    def unsafe_intervals(self, safe_lsn: int) -> List[Dict[str, int]]:
        """LSN intervals of segments that truncation must not touch."""
        out = []
        for info in self._segments:
            if info.corrupt:
                continue
            if not (info.record_count and info.end_lsn < safe_lsn):
                if info.record_count:
                    out.append(
                        {
                            "segment": info.name,
                            "start_lsn": info.start_lsn,
                            "end_lsn": info.end_lsn,
                        }
                    )
        return out

    def rescan(self):
        self._scan()
