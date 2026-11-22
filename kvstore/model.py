from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .heap import ExpiryIndex


@dataclass(frozen=True, slots=True)
class Record:
    key: str
    value: bytes
    version: int
    write_ts_ms: int
    expires_at_ms: int | None
    deleted: bool = False

    def content_key(self) -> tuple[Any, ...]:
        return (
            self.key,
            self.value,
            self.version,
            self.write_ts_ms,
            self.expires_at_ms,
            self.deleted,
        )


@dataclass(frozen=True, slots=True)
class DecodedRecord:
    record: Record
    offset: int
    length: int
    record_index: int
    shard_id: int | None = None
    line_no: int | None = None


@dataclass(slots=True)
class ParsedRecord:
    record: Record
    record_index: int
    offset: int | None = None
    length: int | None = None
    line_no: int | None = None
    shard_id: int | None = None
    defaults_used: list[str] = field(default_factory=list)
    normalization: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KeyRef:
    key: str
    version: int
    segment_id: str
    offset: int
    length: int
    write_ts_ms: int
    expires_at_ms: int | None
    token: str

    def public(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "version": self.version,
            "segment_id": self.segment_id,
            "offset": self.offset,
            "length": self.length,
            "write_timestamp_ms": self.write_ts_ms,
            "expires_at_ms": self.expires_at_ms,
            "token": self.token,
        }


@dataclass(frozen=True, slots=True)
class Tombstone:
    key: str
    version: int
    write_ts_ms: int
    segment_id: str
    reason: str = "deleted"


@dataclass(frozen=True, slots=True)
class SegmentInfo:
    segment_id: str
    path: str
    meta_path: str
    seq: int
    kind: str
    sha256: str
    record_count: int
    created_at_ms: int
    batch_id: str | None = None
    snapshot_seq: int | None = None

    @property
    def filename(self) -> str:
        return Path(self.path).name

    def public(self) -> dict[str, Any]:
        return {
            "segment_id": self.segment_id,
            "filename": self.filename,
            "seq": self.seq,
            "kind": self.kind,
            "sha256": self.sha256,
            "record_count": self.record_count,
            "created_at_ms": self.created_at_ms,
            "batch_id": self.batch_id,
            "snapshot_seq": self.snapshot_seq,
        }


@dataclass(slots=True)
class StoreState:
    """Published read state.

    A published instance is never mutated. Writers clone it, modify the clone,
    then swap the pointer. Readers can therefore retain an old State while a
    writer is working and still observe a byte-consistent pre-write snapshot.
    """

    state_version: int
    index: dict[str, KeyRef]
    tombstones: dict[str, Tombstone]
    expiry: ExpiryIndex
    segments: dict[str, SegmentInfo]
    max_segment_seq: int
    last_snapshot_seq: int
    baseline_snapshot_segment_id: str | None = None
    created_at_ms: int = 0

    def clone(self) -> "StoreState":
        return StoreState(
            state_version=self.state_version,
            index=dict(self.index),
            tombstones=dict(self.tombstones),
            expiry=self.expiry.clone(),
            segments=dict(self.segments),
            max_segment_seq=self.max_segment_seq,
            last_snapshot_seq=self.last_snapshot_seq,
            baseline_snapshot_segment_id=self.baseline_snapshot_segment_id,
            created_at_ms=self.created_at_ms,
        )

    def stats(self) -> dict[str, Any]:
        return {
            "state_version": self.state_version,
            "key_count": len(self.index),
            "tombstone_count": len(self.tombstones),
            "expiry_heap_size": len(self.expiry),
            "segment_count": len(self.segments),
            "max_segment_seq": self.max_segment_seq,
            "last_snapshot_seq": self.last_snapshot_seq,
        }

