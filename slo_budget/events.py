"""Local deterministic request-event parsing and aggregation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .errors import SloError, corrupt
from .models import RequestEvent, WindowRecord
from .policy import BoundaryPolicy, CalculationPolicy


@dataclass(frozen=True)
class EventWindowCounts:
    good_events: int
    total_events: int
    late_count: int
    expired_late_events: int

    @property
    def bad_events(self) -> int:
        return self.total_events - self.good_events


def load_request_events(
    path: str | Path,
    *,
    boundaries: BoundaryPolicy | None = None,
) -> tuple[tuple[RequestEvent, ...], str]:
    source = str(path)
    boundaries = boundaries or BoundaryPolicy()
    boundaries.validate()
    try:
        data = Path(path).read_bytes()
    except OSError as exc:
        raise SloError("E_IO", f"cannot read {source}: {exc}", path=source) from exc

    events: list[RequestEvent] = []
    offset = 0
    for line_number, raw in enumerate(data.splitlines(keepends=True), start=1):
        stripped = raw.rstrip(b"\r\n")
        if not stripped.strip():
            offset += len(raw)
            continue
        try:
            obj = json.loads(stripped.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise corrupt(
                source,
                "request event is not valid UTF-8",
                offset + exc.start,
                details={"line": line_number},
            ) from exc
        except json.JSONDecodeError as exc:
            raise corrupt(
                source,
                "request event is not valid JSON",
                offset + exc.pos,
                details={"line": line_number},
            ) from exc
        if not isinstance(obj, dict):
            raise corrupt(
                source,
                "request event line must be a JSON object",
                offset,
                details={"line": line_number},
            )
        timestamp_ms = obj.get("timestamp_ms")
        if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int):
            raise corrupt(
                source,
                "request event timestamp_ms must be an integer",
                offset,
                details={"line": line_number},
            )
        if timestamp_ms < 0:
            raise corrupt(
                source,
                "request event timestamp_ms must be non-negative",
                offset,
                details={"line": line_number, "timestamp_ms": timestamp_ms},
            )
        service = obj.get("service")
        sli_type = obj.get("sli_type")
        for field_name, value, limit in (
            ("service", service, boundaries.max_service_name_bytes),
            ("sli_type", sli_type, boundaries.max_sli_type_bytes),
        ):
            if not isinstance(value, str) or not value:
                raise corrupt(
                    source,
                    f"request event {field_name} must be a non-empty string",
                    offset,
                    details={"line": line_number, "field": field_name},
                )
            if len(value.encode("utf-8")) > limit:
                raise corrupt(
                    source,
                    f"request event {field_name} byte length is out of bounds",
                    offset,
                    details={"line": line_number, "field": field_name, "maximum": limit},
                )
        good = obj.get("good")
        late = obj.get("late")
        if not isinstance(good, bool):
            raise corrupt(
                source,
                "request event good must be a boolean",
                offset,
                details={"line": line_number},
            )
        if not isinstance(late, bool):
            raise corrupt(
                source,
                "request event late must be a boolean",
                offset,
                details={"line": line_number},
            )
        events.append(
            RequestEvent(
                timestamp_ms=timestamp_ms,
                service=service,
                sli_type=sli_type,
                good=good,
                late=late,
                source_line=line_number,
            )
        )
        offset += len(raw)

    events.sort(
        key=lambda item: (
            item.timestamp_ms,
            item.service,
            item.sli_type,
            item.good,
            item.late,
            item.source_line,
        )
    )
    return tuple(events), hashlib.sha256(data).hexdigest()


def aggregate_events_to_windows(
    events: Iterable[RequestEvent],
    *,
    bucket_ms: int,
    generated_at_ms: int,
    boundaries: BoundaryPolicy | None = None,
) -> tuple[WindowRecord, ...]:
    boundaries = boundaries or BoundaryPolicy()
    boundaries.validate()
    if bucket_ms <= 0 or bucket_ms > boundaries.max_window_length_ms:
        raise SloError(
            "E_CONFIG_INVALID",
            "bucket_ms is outside declared bounds",
            details={"bucket_ms": bucket_ms},
        )
    counters: dict[tuple[str, str, int], list[int]] = {}
    for event in events:
        bucket_start = (event.timestamp_ms // bucket_ms) * bucket_ms
        key = (event.service, event.sli_type, bucket_start)
        counter = counters.setdefault(key, [0, 0, 0])
        counter[0] += 1
        if event.good:
            counter[1] += 1
        if event.late:
            counter[2] += 1
    result = []
    for (service, sli_type, start_ms), (total, good, late) in sorted(counters.items()):
        result.append(
            WindowRecord(
                service=service,
                sli_type=sli_type,
                window_start_ms=start_ms,
                window_length_ms=bucket_ms,
                good_events=good,
                total_events=total,
                late_count=late,
                last_write_ms=generated_at_ms,
                format_version=2,
                snapshot_sequence=0,
                source_format="events",
            )
        )
    return tuple(result)


def count_events_in_window(
    events: Iterable[RequestEvent],
    *,
    service: str,
    sli_type: str,
    window_start_ms: int,
    window_end_ms: int,
    as_of_ms: int,
    policy: CalculationPolicy,
    boundaries: BoundaryPolicy,
) -> EventWindowCounts:
    good = 0
    total = 0
    late_included = 0
    expired_late = 0
    for event in events:
        if event.service != service or event.sli_type != sli_type:
            continue
        if not (window_start_ms <= event.timestamp_ms < window_end_ms):
            continue
        if event.late:
            age_ms = as_of_ms - event.timestamp_ms
            within_tolerance = (
                age_ms <= boundaries.late_tolerance_ms
                if boundaries.late_tolerance_inclusive
                else age_ms < boundaries.late_tolerance_ms
            )
            if not policy.include_late_events or not within_tolerance:
                expired_late += 1
                continue
            late_included += 1
        total += 1
        if event.good:
            good += 1
    return EventWindowCounts(
        good_events=good,
        total_events=total,
        late_count=late_included,
        expired_late_events=expired_late,
    )
