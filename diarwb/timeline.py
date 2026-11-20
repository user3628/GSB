"""Unified millisecond timeline model and edit operations.

A segment is an immutable record on a single shard's integer millisecond
axis.  Edits return a new :class:`Timeline` with a monotonically increasing
version and an :class:`EditRecord` describing the origin and affected range.
All edits enforce the axis invariants: non-negative ranges, start < end and
no segment beyond the shard duration.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .errors import ENotFound, WorkbenchError


class ETimelineInvalid(WorkbenchError):
    code = "E_TIMELINE_INVALID"
    http_status = 422


@dataclass(frozen=True)
class Segment:
    seg_id: str
    start_ms: int
    end_ms: int
    speaker: str
    confidence: str = "high"        # high | low
    overlap: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EditRecord:
    version: int
    operation: str
    origin: str
    affected_ms: Tuple[int, int]
    detail: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "operation": self.operation,
            "origin": self.origin,
            "affected_ms": list(self.affected_ms),
            "detail": self.detail,
        }


def validate_segment(seg: Segment, duration_ms: int) -> None:
    if seg.start_ms < 0 or seg.end_ms < 0:
        raise ETimelineInvalid("negative timestamps are not allowed",
                               {"segment": seg.to_dict()})
    if seg.end_ms <= seg.start_ms:
        raise ETimelineInvalid("end must be strictly greater than start",
                               {"segment": seg.to_dict()})
    if seg.end_ms > duration_ms:
        raise ETimelineInvalid("segment exceeds shard duration",
                               {"segment": seg.to_dict(),
                                "duration_ms": duration_ms})
    if seg.confidence not in ("high", "low"):
        raise ETimelineInvalid("confidence must be high or low",
                               {"segment": seg.to_dict()})


def normalize_segments(segments: List[Segment], duration_ms: int
                       ) -> List[Segment]:
    for seg in segments:
        validate_segment(seg, duration_ms)
    return sorted(segments, key=lambda s: (s.start_ms, s.end_ms, s.seg_id))


class Timeline:
    def __init__(self, shard_id: str, duration_ms: int,
                 segments: Optional[List[Segment]] = None,
                 version: int = 0,
                 history: Optional[List[EditRecord]] = None):
        self.shard_id = shard_id
        self.duration_ms = int(duration_ms)
        self.version = int(version)
        self.segments: List[Segment] = normalize_segments(
            list(segments or []), self.duration_ms)
        self.history: List[EditRecord] = list(history or [])

    # ---------- introspection ----------
    def get(self, seg_id: str) -> Segment:
        for seg in self.segments:
            if seg.seg_id == seg_id:
                return seg
        raise ENotFound(f"segment {seg_id!r} not found",
                        {"shard_id": self.shard_id, "seg_id": seg_id})

    def speakers(self) -> List[str]:
        return sorted({s.speaker for s in self.segments})

    def fingerprint_payload(self) -> List[Dict[str, Any]]:
        return [s.to_dict() for s in self.segments]

    def clone(self, segments: List[Segment], operation: str, origin: str,
              affected_ms: Tuple[int, int], detail: Optional[Dict[str, Any]] = None
              ) -> "Timeline":
        normalized = normalize_segments(segments, self.duration_ms)
        record = EditRecord(self.version + 1, operation, origin,
                            affected_ms, dict(detail or {}))
        return Timeline(self.shard_id, self.duration_ms, normalized,
                        self.version + 1, self.history + [record])

    # ---------- edit operations ----------
    def split(self, seg_id: str, at_ms: int, origin: str) -> "Timeline":
        seg = self.get(seg_id)
        if not seg.start_ms < at_ms < seg.end_ms:
            raise ETimelineInvalid("split point must lie inside the segment",
                                   {"segment": seg.to_dict(), "at_ms": at_ms})
        left = Segment(seg.seg_id + "a", seg.start_ms, at_ms, seg.speaker,
                       seg.confidence, seg.overlap)
        right = Segment(seg.seg_id + "b", at_ms, seg.end_ms, seg.speaker,
                        seg.confidence, seg.overlap)
        rest = [s for s in self.segments if s.seg_id != seg_id]
        return self.clone(rest + [left, right], "split", origin,
                          (seg.start_ms, seg.end_ms),
                          {"seg_id": seg_id, "at_ms": at_ms})

    def merge_adjacent(self, seg_ids: List[str], origin: str) -> "Timeline":
        chosen = [self.get(sid) for sid in seg_ids]
        speaker = chosen[0].speaker
        if any(s.speaker != speaker for s in chosen):
            raise ETimelineInvalid("only same-speaker segments can merge",
                                   {"speakers": [s.speaker for s in chosen]})
        ordered = sorted(chosen, key=lambda s: s.start_ms)
        for prev, nxt in zip(ordered, ordered[1:]):
            if prev.end_ms != nxt.start_ms:
                raise ETimelineInvalid("segments must be adjacent to merge",
                                       {"gap": [prev.end_ms, nxt.start_ms]})
        start = ordered[0].start_ms
        end = ordered[-1].end_ms
        merged = Segment("m" + ordered[0].seg_id, start, end, speaker,
                         "high" if all(s.confidence == "high" for s in ordered)
                         else "low",
                         any(s.overlap for s in ordered))
        ids = {s.seg_id for s in chosen}
        rest = [s for s in self.segments if s.seg_id not in ids]
        return self.clone(rest + [merged], "merge_adjacent", origin,
                          (start, end), {"merged": list(ids)})

    def rename_speaker(self, old: str, new: str, origin: str) -> "Timeline":
        if old not in self.speakers():
            raise ENotFound(f"speaker {old!r} not found",
                            {"shard_id": self.shard_id, "speaker": old})
        if not new:
            raise ETimelineInvalid("new speaker name must be non-empty")
        if old == new:
            return self
        updated = [Segment(s.seg_id, s.start_ms, s.end_ms, new,
                           s.confidence, s.overlap)
                   if s.speaker == old else s for s in self.segments]
        bounds = [(s.start_ms, s.end_ms) for s in self.segments
                  if s.speaker == old]
        affected = (min(b[0] for b in bounds), max(b[1] for b in bounds))
        return self.clone(updated, "rename_speaker", origin, affected,
                          {"old": old, "new": new})

    def merge_speakers(self, source: str, target: str, origin: str) -> "Timeline":
        names = self.speakers()
        if source not in names or target not in names:
            raise ENotFound("both speakers must exist",
                            {"source": source, "target": target})
        if source == target:
            raise ETimelineInvalid("cannot merge a speaker into itself")
        updated = [Segment(s.seg_id, s.start_ms, s.end_ms, target,
                           s.confidence, s.overlap)
                   if s.speaker == source else s for s in self.segments]
        ids = {s.seg_id for s in self.segments
               if s.speaker in (source, target)}
        affected_segs = [s for s in self.segments if s.seg_id in ids]
        affected = (min(s.start_ms for s in affected_segs),
                    max(s.end_ms for s in affected_segs))
        return self.clone(updated, "merge_speakers", origin, affected,
                          {"source": source, "target": target})

    def upsert_segment(self, segment: Segment, origin: str) -> "Timeline":
        """Create or replace a segment while re-checking all axis invariants."""
        rest = [s for s in self.segments if s.seg_id != segment.seg_id]
        return self.clone(rest + [segment], "upsert_segment", origin,
                          (segment.start_ms, segment.end_ms),
                          {"seg_id": segment.seg_id})

    def delete_segment(self, seg_id: str, origin: str) -> "Timeline":
        seg = self.get(seg_id)
        rest = [s for s in self.segments if s.seg_id != seg_id]
        return self.clone(rest, "delete_segment", origin,
                          (seg.start_ms, seg.end_ms), {"seg_id": seg_id})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "duration_ms": self.duration_ms,
            "version": self.version,
            "segments": [s.to_dict() for s in self.segments],
            "history": [h.to_dict() for h in self.history],
        }


def timeline_from_dict(data: Dict[str, Any]) -> Timeline:
    segments = [Segment(**s) for s in data.get("segments", [])]
    history = [EditRecord(h["version"], h["operation"], h["origin"],
                          tuple(h["affected_ms"]), h.get("detail", {}))
               for h in data.get("history", [])]
    return Timeline(data["shard_id"], data["duration_ms"], segments,
                    data.get("version", 0), history)
