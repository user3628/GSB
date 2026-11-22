from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PartRecord:
    seq: int
    size: int
    sha256: str
    arrival_ms: int
    first_version: int
    duplicate_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "size": self.size,
            "sha256": self.sha256,
            "arrival_ms": self.arrival_ms,
            "first_version": self.first_version,
            "duplicate_count": self.duplicate_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PartRecord":
        return cls(
            seq=int(data["seq"]),
            size=int(data["size"]),
            sha256=str(data["sha256"]),
            arrival_ms=int(data["arrival_ms"]),
            first_version=int(data["first_version"]),
            duplicate_count=int(data.get("duplicate_count", 0)),
        )


@dataclass
class ContentRecord:
    sha256: str
    size: int
    ref_count: int
    state: str = "active"
    first_version: int = 0
    gc_version: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "size": self.size,
            "ref_count": self.ref_count,
            "state": self.state,
            "first_version": self.first_version,
            "gc_version": self.gc_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ContentRecord":
        gc_version = data.get("gc_version")
        return cls(
            sha256=str(data["sha256"]),
            size=int(data["size"]),
            ref_count=int(data["ref_count"]),
            state=str(data.get("state", "active")),
            first_version=int(data.get("first_version", 0)),
            gc_version=None if gc_version is None else int(gc_version),
        )


@dataclass
class SessionState:
    session_id: str
    object_key: str
    generation: int
    lower_bound: int
    status: str
    conflict_policy: str
    created_version: int
    updated_version: int
    duplicate_commits: int = 0
    completed_version: int | None = None
    cancelled_version: int | None = None
    parts: dict[int, PartRecord] = field(default_factory=dict)
    intervals: list[list[int]] = field(default_factory=list)

    def add_interval(self, seq: int) -> None:
        """Incrementally add one sequence number to the sorted disjoint ranges."""
        ranges = self.intervals
        i = bisect_left(ranges, [seq, seq])
        if i > 0 and ranges[i - 1][0] <= seq <= ranges[i - 1][1]:
            return
        if i < len(ranges) and ranges[i][0] <= seq <= ranges[i][1]:
            return

        start = seq
        end = seq
        remove_from = i
        remove_to = i
        if i > 0 and ranges[i - 1][1] >= seq - 1:
            start = ranges[i - 1][0]
            remove_from = i - 1
        if i < len(ranges) and ranges[i][0] <= seq + 1:
            end = ranges[i][1]
            remove_to = i + 1
        ranges[remove_from:remove_to] = [[start, end]]

    def rebuild_intervals(self) -> None:
        self.intervals = []
        for seq in sorted(self.parts):
            self.add_interval(seq)

    def remove_interval(self, seq: int) -> None:
        """Incrementally remove one sequence number from the range set."""
        ranges = self.intervals
        i = bisect_left(ranges, [seq, seq])
        if i >= len(ranges) or ranges[i][0] > seq:
            i -= 1
        if i < 0 or i >= len(ranges) or not (ranges[i][0] <= seq <= ranges[i][1]):
            return
        start, end = ranges[i]
        if start == end:
            del ranges[i]
        elif start == seq:
            ranges[i][0] = seq + 1
        elif end == seq:
            ranges[i][1] = seq - 1
        else:
            ranges[i : i + 1] = [[start, seq - 1], [seq + 1, end]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "object_key": self.object_key,
            "generation": self.generation,
            "lower_bound": self.lower_bound,
            "status": self.status,
            "conflict_policy": self.conflict_policy,
            "created_version": self.created_version,
            "updated_version": self.updated_version,
            "duplicate_commits": self.duplicate_commits,
            "completed_version": self.completed_version,
            "cancelled_version": self.cancelled_version,
            "parts": {str(seq): part.to_dict() for seq, part in sorted(self.parts.items())},
            "intervals": [list(interval) for interval in self.intervals],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionState":
        session = cls(
            session_id=str(data["session_id"]),
            object_key=str(data["object_key"]),
            generation=int(data["generation"]),
            lower_bound=int(data.get("lower_bound", 1)),
            status=str(data["status"]),
            conflict_policy=str(data.get("conflict_policy", "reject")),
            created_version=int(data.get("created_version", 0)),
            updated_version=int(data.get("updated_version", 0)),
            duplicate_commits=int(data.get("duplicate_commits", 0)),
            completed_version=(
                None if data.get("completed_version") is None else int(data["completed_version"])
            ),
            cancelled_version=(
                None if data.get("cancelled_version") is None else int(data["cancelled_version"])
            ),
        )
        session.parts = {
            int(seq): PartRecord.from_dict(record)
            for seq, record in data.get("parts", {}).items()
        }
        intervals = data.get("intervals")
        if intervals is None:
            session.rebuild_intervals()
        else:
            session.intervals = [[int(a), int(b)] for a, b in intervals]
        return session


@dataclass
class IndexState:
    sessions: dict[str, SessionState] = field(default_factory=dict)
    dedup: dict[str, ContentRecord] = field(default_factory=dict)
    removed: dict[str, ContentRecord] = field(default_factory=dict)
    objects: dict[str, dict[str, Any]] = field(default_factory=dict)

    def apply_event(self, event: dict[str, Any]) -> None:
        version = int(event["version"])
        operation = str(event["operation"])
        payload = event.get("payload") or {}

        if operation == "content_purged":
            for sha in payload.get("sha256", []):
                content = self.dedup.pop(str(sha), None)
                if content is not None:
                    content.state = "removed"
                    content.gc_version = version
                    self.removed[content.sha256] = content
            return

        if operation == "session_created":
            sid = str(payload["session_id"])
            self.sessions[sid] = SessionState(
                session_id=sid,
                object_key=str(payload["object_key"]),
                generation=int(payload["generation"]),
                lower_bound=int(payload["lower_bound"]),
                status="active",
                conflict_policy=str(payload.get("conflict_policy", "reject")),
                created_version=version,
                updated_version=version,
            )
            return

        sid = str(event.get("session_id") or payload.get("session_id") or "")
        session = self.sessions.get(sid)
        if session is None:
            return

        if operation == "part_committed":
            part_data = payload["part"]
            seq = int(part_data["seq"])
            sha = str(part_data["sha256"])
            size = int(part_data["size"])
            session.parts[seq] = PartRecord(
                seq=seq,
                size=size,
                sha256=sha,
                arrival_ms=int(part_data["arrival_ms"]),
                first_version=version,
                duplicate_count=0,
            )
            session.add_interval(seq)
            content = self.dedup.get(sha)
            if content is None:
                content = ContentRecord(
                    sha256=sha,
                    size=size,
                    ref_count=0,
                    state="active",
                    first_version=version,
                )
                self.dedup[sha] = content
            content.ref_count += 1
            content.state = "active"
            content.gc_version = None
            session.updated_version = version
            return

        if operation == "part_duplicate":
            seq = int(payload["seq"])
            part = session.parts.get(seq)
            if part is not None:
                part.duplicate_count = int(payload.get("duplicate_count", part.duplicate_count + 1))
            session.duplicate_commits = int(
                payload.get("session_duplicate_commits", session.duplicate_commits + 1)
            )
            session.updated_version = version
            return

        if operation == "part_deleted":
            seq = int(payload["seq"])
            part = session.parts.pop(seq, None)
            if part is not None:
                content = self.dedup.get(part.sha256)
                if content is not None and content.ref_count > 0:
                    content.ref_count -= 1
                    if content.ref_count == 0:
                        content.state = "pending_gc"
                        content.gc_version = version
                session.remove_interval(seq)
            session.updated_version = version
            return

        if operation == "session_cancelled":
            for part_data in payload.get("parts", []):
                sha = str(part_data["sha256"])
                content = self.dedup.get(sha)
                if content is not None and content.ref_count > 0:
                    content.ref_count -= 1
                    if content.ref_count == 0:
                        content.state = "pending_gc"
                        content.gc_version = version
            session.parts.clear()
            session.intervals.clear()
            session.status = "cancelled"
            session.cancelled_version = version
            session.updated_version = version
            return

        if operation == "session_merged":
            obj = dict(payload["object"])
            session.status = "completed"
            session.completed_version = version
            session.updated_version = version
            self.objects[str(obj["object_key"])] = obj
            return

    def to_dict(self) -> dict[str, Any]:
        return {
            "sessions": {sid: session.to_dict() for sid, session in self.sessions.items()},
            "dedup": {sha: content.to_dict() for sha, content in self.dedup.items()},
            "removed": {sha: content.to_dict() for sha, content in self.removed.items()},
            "objects": self.objects,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IndexState":
        state = cls()
        state.sessions = {
            sid: SessionState.from_dict(record)
            for sid, record in data.get("sessions", {}).items()
        }
        state.dedup = {
            sha: ContentRecord.from_dict(record)
            for sha, record in data.get("dedup", {}).items()
        }
        state.removed = {
            sha: ContentRecord.from_dict(record)
            for sha, record in data.get("removed", {}).items()
        }
        state.objects = dict(data.get("objects", {}))
        return state

    def generation_index(self, object_key: str) -> dict[str, Any]:
        sessions = [
            session
            for session in self.sessions.values()
            if session.object_key == object_key
        ]
        sessions.sort(key=lambda session: session.generation)
        return {
            "object_key": object_key,
            "current_generation": max((s.generation for s in sessions), default=0),
            "sessions": [
                {
                    "session_id": s.session_id,
                    "generation": s.generation,
                    "status": s.status,
                }
                for s in sessions
            ],
        }

