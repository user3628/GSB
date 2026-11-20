from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from app.errors import SpecInvalidError
from app.fixedpoint import compensated_sum_us
from app.spec import ModelSpec, NormalizedTrace, RequestSpec, model_contribution
from app.units import (
    checked_add,
    format_bytes,
    format_fixed_ms,
    round_half_up_div,
    us_to_ms_x100,
)


@dataclass
class MemorySegment:
    start: int
    size: int
    state: str
    label: str
    owner_batch_id: str | None = None
    owner_char: str | None = None

    @property
    def end(self) -> int:
        return self.start + self.size

    def as_dict(self) -> dict[str, Any]:
        return {
            "start_bytes": self.start,
            "size_bytes": self.size,
            "end_bytes": self.end,
            "state": self.state,
            "label": self.label,
            "owner_batch_id": self.owner_batch_id,
            "owner_char": self.owner_char,
        }


class MemoryArena:
    def __init__(
        self,
        total_bytes: int,
        initial_segments: list[MemorySegment],
        requested_block_bytes: int | None = None,
    ) -> None:
        self.total_bytes = total_bytes
        self.segments = list(initial_segments)
        self.initial_segments = [MemorySegment(**vars(segment)) for segment in initial_segments]
        self.block_bytes = requested_block_bytes or self._default_block_bytes(total_bytes)
        if self.block_bytes <= 0:
            raise SpecInvalidError("bitmap block size must be greater than zero")
        self.initial_free_bytes = self.free_bytes

    @staticmethod
    def _default_block_bytes(total_bytes: int) -> int:
        target = max(1, total_bytes // 64)
        block = 1
        while block * 2 <= target:
            block *= 2
        return max(1024, block)

    @property
    def free_bytes(self) -> int:
        return sum(segment.size for segment in self.segments if segment.state == "free")

    @property
    def largest_free_segment(self) -> MemorySegment | None:
        candidates = [segment for segment in self.segments if segment.state == "free"]
        if not candidates:
            return None
        return min(candidates, key=lambda segment: (-segment.size, segment.start))

    def allocate(self, required_bytes: int, batch_id: str, batch_char: str) -> MemorySegment | None:
        candidates = [
            segment
            for segment in self.segments
            if segment.state == "free" and segment.size >= required_bytes
        ]
        if not candidates:
            return None
        target = min(candidates, key=lambda segment: (segment.size, segment.start))
        self.segments.remove(target)
        allocated = MemorySegment(
            start=target.start,
            size=required_bytes,
            state="allocated",
            label=f"batch-{batch_char}",
            owner_batch_id=batch_id,
            owner_char=batch_char,
        )
        replacement = [allocated]
        remainder = target.size - required_bytes
        if remainder:
            replacement.append(
                MemorySegment(
                    start=target.start + required_bytes,
                    size=remainder,
                    state="free",
                    label="free",
                )
            )
        self.segments.extend(replacement)
        self.segments.sort(key=lambda segment: (segment.start, segment.end))
        return allocated

    def bitmap(self) -> dict[str, Any]:
        block_count = (self.total_bytes + self.block_bytes - 1) // self.block_bytes
        chars: list[str] = []
        for index in range(block_count):
            start = index * self.block_bytes
            end = min(start + self.block_bytes, self.total_bytes)
            overlapping = [
                segment
                for segment in self.segments
                if segment.start < end and segment.end > start
            ]
            allocated = [segment for segment in overlapping if segment.state == "allocated"]
            if allocated:
                chars.append(allocated[-1].owner_char or "?")
            elif any(segment.state == "occupied" for segment in overlapping):
                chars.append("!")
            elif any(segment.state == "reserved" for segment in overlapping):
                chars.append("#")
            else:
                chars.append(".")
        return {
            "block_bytes": self.block_bytes,
            "block_count": block_count,
            "text": "".join(chars),
            "legend": {
                ".": "free",
                "#": "reserved",
                "!": "occupied-fragment",
                "A-Z": "allocated batch",
            },
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "total_bytes": self.total_bytes,
            "free_bytes": self.free_bytes,
            "segments": [segment.as_dict() for segment in self.segments],
        }


@dataclass
class RequestSlice:
    request: RequestSpec
    samples: int
    origin: str


@dataclass
class MemberPlan:
    request: RequestSpec
    model: ModelSpec
    samples: int
    memory_bytes: int
    latency_us: int
    origin: str
    part_index: int = 0
    part_count: int = 0

    def copy_with_samples(self, samples: int) -> "MemberPlan":
        return MemberPlan(
            request=self.request,
            model=self.model,
            samples=samples,
            memory_bytes=self.model.memory_bytes * samples,
            latency_us=self.model.cost_us * samples,
            origin=self.origin,
            part_index=self.part_index,
            part_count=self.part_count,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request.request_id,
            "request_seq": self.request.seq,
            "arrival_us": self.request.arrival_us,
            "model_id": self.model.model_id,
            "samples": self.samples,
            "part_index": self.part_index,
            "part_count": self.part_count,
            "sample_memory_bytes": self.memory_bytes,
            "sample_latency_us": self.latency_us,
            "expected_batch_latency_us": self.request.expected_latency_us,
            "priority": self.request.priority,
            "origin": self.origin,
        }


@dataclass
class BatchPlan:
    ordinal: int
    batch_id: str
    model_id: str
    priority: int
    members: list[MemberPlan]
    memory_bytes: int
    latency_us: int
    first_arrival_us: int
    last_arrival_us: int
    first_seq: int
    last_seq: int
    region_start: int
    region_size: int
    split_reasons: list[str] = field(default_factory=list)
    split_notes: list[str] = field(default_factory=list)
    bitmap_after: dict[str, Any] = field(default_factory=dict)
    fragmentation_event_id: str | None = None
    timeline_start_us: int = 0
    timeline_end_us: int = 0

    @property
    def sample_count(self) -> int:
        return sum(member.samples for member in self.members)

    @property
    def member_count(self) -> int:
        return len(self.members)

    @property
    def min_expected_latency_us(self) -> int:
        return min(member.request.expected_latency_us for member in self.members)

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "ordinal": self.ordinal,
            "model_id": self.model_id,
            "priority": self.priority,
            "memory_bytes": self.memory_bytes,
            "latency_us": self.latency_us,
            "first_arrival_us": self.first_arrival_us,
            "last_arrival_us": self.last_arrival_us,
            "region_start": self.region_start,
            "region_size": self.region_size,
            "split_reasons": self.split_reasons,
            "members": [
                {
                    "request_id": member.request.request_id,
                    "request_seq": member.request.seq,
                    "samples": member.samples,
                    "part_index": member.part_index,
                    "part_count": member.part_count,
                    "memory_bytes": member.memory_bytes,
                    "latency_us": member.latency_us,
                }
                for member in self.members
            ],
        }


@dataclass
class FragmentationEvent:
    event_id: str
    batch_id: str
    request_id: str
    model_id: str
    requested_batch_samples: int
    allocated_batch_samples: int
    largest_free_bytes: int
    requested_batch_bytes: int
    before_bitmap: dict[str, Any]
    after_bitmap: dict[str, Any]
    before_segments: dict[str, Any]
    after_segments: dict[str, Any]
    mixed_rejection: dict[str, Any] | None
    constraint_assertions: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "batch_id": self.batch_id,
            "request_id": self.request_id,
            "model_id": self.model_id,
            "requested_batch_samples": self.requested_batch_samples,
            "allocated_batch_samples": self.allocated_batch_samples,
            "largest_free_bytes": self.largest_free_bytes,
            "largest_free_display": format_bytes(self.largest_free_bytes),
            "requested_batch_bytes": self.requested_batch_bytes,
            "before_bitmap": self.before_bitmap,
            "after_bitmap": self.after_bitmap,
            "before_segments": self.before_segments,
            "after_segments": self.after_segments,
            "mixed_rejection": self.mixed_rejection,
            "constraint_assertions": self.constraint_assertions,
        }


@dataclass
class ScheduleResult:
    batches: list[BatchPlan]
    fragmentation_events: list[FragmentationEvent]
    mixed_rejections: list[dict[str, Any]]
    initial_arena: dict[str, Any]
    final_arena: dict[str, Any]
    initial_bitmap: dict[str, Any]
    final_bitmap: dict[str, Any]
    node_total_bytes: int
    node_initial_free_bytes: int
    node_final_free_bytes: int
    deterministic_digest: str
    algorithm_version: str

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "algorithm_version": self.algorithm_version,
            "node_total_bytes": self.node_total_bytes,
            "node_initial_free_bytes": self.node_initial_free_bytes,
            "batches": [batch.canonical_payload() for batch in self.batches],
            "fragmentation_events": [
                {
                    "event_id": event.event_id,
                    "batch_id": event.batch_id,
                    "request_id": event.request_id,
                    "requested_batch_samples": event.requested_batch_samples,
                    "allocated_batch_samples": event.allocated_batch_samples,
                    "largest_free_bytes": event.largest_free_bytes,
                    "mixed_rejection": event.mixed_rejection,
                }
                for event in self.fragmentation_events
            ],
            "mixed_rejections": self.mixed_rejections,
        }

    @property
    def digest(self) -> str:
        text = json.dumps(self.canonical_payload(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _batch_char(ordinal: int) -> str:
    if 1 <= ordinal <= 26:
        return chr(ord("A") + ordinal - 1)
    return "?"


def _initial_slices(request: RequestSpec, model: ModelSpec) -> list[RequestSlice]:
    max_by_latency = max(1, request.expected_latency_us // model.cost_us)
    chunk = min(model.max_batch_samples, max_by_latency)
    pieces: list[RequestSlice] = []
    remaining = request.samples
    while remaining:
        take = min(remaining, chunk)
        reason = "original"
        if request.samples > take:
            if take == max_by_latency < model.max_batch_samples:
                reason = "request-latency-cap"
            else:
                reason = "request-model-max-samples"
        pieces.append(RequestSlice(request=request, samples=take, origin=reason))
        remaining -= take
    return pieces


def _min_cap(members: list[MemberPlan], extra: RequestSpec | None = None) -> int:
    caps = [member.request.expected_latency_us for member in members]
    if extra is not None:
        caps.append(extra.expected_latency_us)
    return min(caps)


def schedule_trace(trace: NormalizedTrace) -> ScheduleResult:
    initial_segments = [
        MemorySegment(
            start=region.start_bytes,
            size=region.size_bytes,
            state=region.state,
            label=region.label,
        )
        for region in trace.node.regions
    ]
    arena = MemoryArena(
        total_bytes=trace.node.total_memory_bytes,
        initial_segments=initial_segments,
        requested_block_bytes=trace.node.requested_block_bytes,
    )
    initial_arena = arena.snapshot()
    initial_bitmap = arena.bitmap()

    grouped: dict[tuple[str, int], list[RequestSpec]] = defaultdict(list)
    for request in trace.requests:
        grouped[(request.model_id, request.priority)].append(request)

    ordered_keys = sorted(
        grouped,
        key=lambda key: (
            min(request.arrival_us for request in grouped[key]),
            key[0],
            key[1],
        ),
    )
    for key in grouped:
        grouped[key].sort(key=lambda request: (request.arrival_us, request.seq))

    batches: list[BatchPlan] = []
    fragmentation_events: list[FragmentationEvent] = []
    mixed_rejections: list[dict[str, Any]] = []
    consumed_counts: Counter[str] = Counter()

    def other_model_probe(model_id: str, priority: int) -> RequestSpec | None:
        candidates: list[RequestSpec] = []
        for key in ordered_keys:
            if key == (model_id, priority):
                continue
            for request in grouped[key]:
                if request.model_id != model_id and consumed_counts[request.request_id] < request.samples:
                    candidates.append(request)
                    break
        if not candidates:
            return None
        return min(candidates, key=lambda request: (request.arrival_us, request.seq, request.model_id))

    def mixed_rejection_probe(
        model: ModelSpec,
        prefix: list[MemberPlan],
        prefix_bytes: int,
        prefix_latency_us: int,
        free_segment: MemorySegment,
    ) -> dict[str, Any] | None:
        probe = other_model_probe(model.model_id, prefix[0].request.priority)
        if probe is None:
            return None
        probe_model = trace.models[probe.model_id]
        combined_memory = prefix_bytes + probe_model.memory_bytes
        if combined_memory > free_segment.size:
            return None
        combined_latency = prefix_latency_us + probe_model.cost_us
        cap = _min_cap(prefix, probe)
        if combined_latency <= cap:
            return None
        return {
            "code": "MIXED_BATCH_REJECTED_LATENCY_CAP",
            "model_id": model.model_id,
            "probe_model_id": probe.model_id,
            "probe_request_id": probe.request_id,
            "probe_samples": 1,
            "region_start_bytes": free_segment.start,
            "region_size_bytes": free_segment.size,
            "combined_memory_bytes": combined_memory,
            "combined_latency_us": combined_latency,
            "latency_cap_us": cap,
            "reason": "Mixing would fit in memory but exceed the strictest expected batch latency cap.",
        }

    def allocate_batch(
        members: list[MemberPlan],
        split_reasons: list[str],
        split_notes: list[str],
    ) -> tuple[BatchPlan | None, list[MemberPlan], str]:
        ordinal = len(batches) + 1
        batch_id = f"b{ordinal:04d}"
        batch_char = _batch_char(ordinal)
        required_bytes = sum(member.memory_bytes for member in members)
        requested_samples = sum(member.samples for member in members)
        before_bitmap = arena.bitmap()
        before_segments = arena.snapshot()
        allocated = arena.allocate(required_bytes, batch_id, batch_char)
        if allocated is not None:
            latency_sum = compensated_sum_us(member.latency_us for member in members)
            batch = BatchPlan(
                ordinal=ordinal,
                batch_id=batch_id,
                model_id=members[0].model.model_id,
                priority=members[0].request.priority,
                members=members,
                memory_bytes=required_bytes,
                latency_us=latency_sum.value,
                first_arrival_us=min(member.request.arrival_us for member in members),
                last_arrival_us=max(member.request.arrival_us for member in members),
                first_seq=min(member.request.seq for member in members),
                last_seq=max(member.request.seq for member in members),
                region_start=allocated.start,
                region_size=allocated.size,
                split_reasons=list(dict.fromkeys(split_reasons)),
                split_notes=list(dict.fromkeys(split_notes)),
            )
            batch.bitmap_after = arena.bitmap()
            batches.append(batch)
            return batch, [], "allocated"

        free_segment = arena.largest_free_segment
        if free_segment is None:
            raise SpecInvalidError(
                "node has no free memory region for the next batch",
                details={"batch_ordinal": ordinal, "required_bytes": required_bytes},
            )
        model = members[0].model
        capacity_samples = free_segment.size // model.memory_bytes
        if capacity_samples <= 0:
            raise SpecInvalidError(
                "largest free region cannot hold one sample for this model",
                details={
                    "model_id": model.model_id,
                    "sample_memory_bytes": model.memory_bytes,
                    "largest_free_bytes": free_segment.size,
                },
            )
        capacity_samples = min(capacity_samples, model.max_batch_samples)

        prefix: list[MemberPlan] = []
        remainder: list[MemberPlan] = []
        left = capacity_samples
        prefix_bytes = 0
        prefix_latency = 0
        for index, member in enumerate(members):
            if left <= 0:
                remainder.append(member)
                continue
            take = min(member.samples, left)
            prefix_member = member.copy_with_samples(take) if take != member.samples else member
            prefix.append(prefix_member)
            prefix_bytes += prefix_member.memory_bytes
            prefix_latency += prefix_member.latency_us
            left -= take
            if take < member.samples:
                remainder.append(member.copy_with_samples(member.samples - take))
        if not prefix:
            raise SpecInvalidError("fragment fallback could not make progress")

        mixed_rejection = mixed_rejection_probe(
            model, prefix, prefix_bytes, prefix_latency, free_segment
        )
        allocated = arena.allocate(prefix_bytes, batch_id, batch_char)
        if allocated is None:
            raise RuntimeError("fragmentation planner failed to allocate a region it sized")
        batch = BatchPlan(
            ordinal=ordinal,
            batch_id=batch_id,
            model_id=model.model_id,
            priority=prefix[0].request.priority,
            members=prefix,
            memory_bytes=prefix_bytes,
            latency_us=prefix_latency,
            first_arrival_us=min(member.request.arrival_us for member in prefix),
            last_arrival_us=max(member.request.arrival_us for member in prefix),
            first_seq=min(member.request.seq for member in prefix),
            last_seq=max(member.request.seq for member in prefix),
            region_start=allocated.start,
            region_size=allocated.size,
            split_reasons=list(dict.fromkeys([*split_reasons, "MEMORY_FRAGMENTATION"])),
            split_notes=list(
                dict.fromkeys(
                    [
                        *split_notes,
                        (
                            f"Largest continuous free region holds {capacity_samples} samples; "
                            f"the requested batch held {requested_samples}."
                        ),
                    ]
                )
            ),
        )
        batch.bitmap_after = arena.bitmap()
        batches.append(batch)
        event_id = f"frag-{len(fragmentation_events) + 1:04d}"
        event = FragmentationEvent(
            event_id=event_id,
            batch_id=batch_id,
            request_id=prefix[-1].request.request_id,
            model_id=model.model_id,
            requested_batch_samples=requested_samples,
            allocated_batch_samples=sum(member.samples for member in prefix),
            largest_free_bytes=free_segment.size,
            requested_batch_bytes=required_bytes,
            before_bitmap=before_bitmap,
            after_bitmap=batch.bitmap_after,
            before_segments=before_segments,
            after_segments=arena.snapshot(),
            mixed_rejection=mixed_rejection,
            constraint_assertions={
                "chunk_samples": sum(member.samples for member in prefix),
                "model_max_batch_samples": model.max_batch_samples,
                "within_model_max": all(
                    member.samples <= model.max_batch_samples for member in prefix
                )
                and sum(member.samples for member in prefix) <= model.max_batch_samples,
                "per_member_samples": [
                    {
                        "request_id": member.request.request_id,
                        "samples": member.samples,
                        "within_model_max": member.samples <= model.max_batch_samples,
                    }
                    for member in prefix
                ],
            },
        )
        batch.fragmentation_event_id = event_id
        fragmentation_events.append(event)
        if mixed_rejection is not None:
            mixed_rejections.append({**mixed_rejection, "event_id": event_id, "batch_id": batch_id})
        return batch, remainder, "fragmented"

    for key in ordered_keys:
        model_id, priority = key
        model = trace.models[model_id]
        work: deque[MemberPlan] = deque()
        for request in grouped[key]:
            for piece in _initial_slices(request, model):
                work.append(
                    MemberPlan(
                        request=request,
                        model=model,
                        samples=piece.samples,
                        memory_bytes=model.memory_bytes * piece.samples,
                        latency_us=model.cost_us * piece.samples,
                        origin=piece.origin,
                    )
                )

        open_members: list[MemberPlan] = []
        open_samples = 0
        open_latency = 0
        close_reasons: list[str] = ["TRACE_GROUP_BOUNDARY"]
        close_notes: list[str] = [
            f"Batch group is isolated by model={model_id} and priority={priority}."
        ]

        while work or open_members:
            if not work:
                batch, remainder, _ = allocate_batch(open_members, close_reasons, close_notes)
                if remainder:
                    work.extendleft(reversed(remainder))
                    open_members = []
                    open_samples = 0
                    open_latency = 0
                    close_reasons = ["MEMORY_FRAGMENTATION"]
                    close_notes = ["Remainder of a fragment-downscaled batch is requeued in order."]
                    continue
                break

            item = work.popleft()
            candidate_samples = open_samples + item.samples
            candidate_latency = open_latency + item.latency_us
            candidate_cap = _min_cap(open_members, item.request)
            reason: str | None = None
            if candidate_samples > model.max_batch_samples:
                reason = "MAX_BATCH_SAMPLES"
            elif candidate_latency > candidate_cap:
                reason = "LATENCY_CAP"

            if reason and open_members:
                batch, remainder, _ = allocate_batch(
                    open_members, close_reasons, close_notes
                )
                if remainder:
                    work.extendleft(reversed([item, *remainder]))
                else:
                    work.appendleft(item)
                open_members = []
                open_samples = 0
                open_latency = 0
                close_reasons = [reason]
                close_notes = [
                    "The next request would violate the model maximum-sample or latency-cap contract."
                ]
                continue
            if reason and not open_members:
                raise SpecInvalidError(
                    "a single pre-sliced request cannot satisfy batch constraints",
                    details={"request_id": item.request.request_id, "reason": reason},
                )
            open_members.append(item)
            open_samples = candidate_samples
            open_latency = candidate_latency

            if item.origin == "request-model-max-samples":
                close_reasons = ["REQUEST_SAMPLE_LIMIT"]
                close_notes = ["Request samples exceeded model maximum and were split deterministically."]
            elif item.origin == "request-latency-cap":
                close_reasons = ["REQUEST_LATENCY_LIMIT"]
                close_notes = ["Request would exceed its expected batch latency cap without splitting."]

        for request in grouped[key]:
            consumed_counts[request.request_id] += request.samples

    # Add final part numbering only after fragment fallback has stopped changing membership.
    parts_by_request: dict[str, list[MemberPlan]] = defaultdict(list)
    for batch in batches:
        for member in batch.members:
            parts_by_request[member.request.request_id].append(member)
    for request_id, members in parts_by_request.items():
        ordered = sorted(
            members,
            key=lambda member: (
                next(batch.ordinal for batch in batches if member in batch.members),
                member.request.seq,
            ),
        )
        for index, member in enumerate(ordered, start=1):
            member.part_index = index
            member.part_count = len(ordered)

    # Timeline is a deterministic serial replay; gaps are represented explicitly.
    timeline_cursor = 0
    for batch in batches:
        batch_start = max(timeline_cursor, batch.first_arrival_us)
        batch.timeline_start_us = batch_start
        batch.timeline_end_us = batch_start + batch.latency_us
        timeline_cursor = batch.timeline_end_us

    result = ScheduleResult(
        batches=batches,
        fragmentation_events=fragmentation_events,
        mixed_rejections=mixed_rejections,
        initial_arena=initial_arena,
        final_arena=arena.snapshot(),
        initial_bitmap=initial_bitmap,
        final_bitmap=arena.bitmap(),
        node_total_bytes=trace.node.total_memory_bytes,
        node_initial_free_bytes=arena.initial_free_bytes,
        node_final_free_bytes=arena.free_bytes,
        deterministic_digest="",
        algorithm_version="batch-fixedpoint-v1",
    )
    result.deterministic_digest = result.digest
    return result


def batch_budget_detail(batch: BatchPlan, schedule: ScheduleResult) -> dict[str, Any]:
    initial_free = schedule.node_initial_free_bytes
    utilization_ppm = (
        round_half_up_div(batch.memory_bytes * 1_000_000, initial_free) if initial_free else None
    )
    return {
        "batch_id": batch.batch_id,
        "memory_bytes": batch.memory_bytes,
        "memory_display": format_bytes(batch.memory_bytes),
        "region_start_bytes": batch.region_start,
        "region_end_bytes": batch.region_start + batch.region_size,
        "node_initial_free_bytes": initial_free,
        "node_initial_free_display": format_bytes(initial_free),
        "node_total_bytes": schedule.node_total_bytes,
        "node_total_display": format_bytes(schedule.node_total_bytes),
        "utilization_of_initial_free_ppm": utilization_ppm,
        "per_member": [
            {
                "request_id": member.request.request_id,
                "samples": member.samples,
                "sample_memory_bytes": member.model.memory_bytes,
                "member_memory_bytes": member.memory_bytes,
            }
            for member in batch.members
        ],
        "bitmap": batch.bitmap_after,
        "latency_ms_fixed": us_to_ms_x100(batch.latency_us),
        "latency_ms": format_fixed_ms(us_to_ms_x100(batch.latency_us)),
    }
