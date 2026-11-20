from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .errors import BusinessError


SEND = "SEND"
ACK = "ACK"
LOSS = "LOSS"
TIMEOUT = "TIMEOUT"
DUP_ACK = "DUP_ACK"

VALID_EVENT_TYPES = {SEND, ACK, LOSS, TIMEOUT, DUP_ACK}

_TYPE_ALIASES = {
    "send": SEND,
    "发送": SEND,
    "ack": ACK,
    "确认": ACK,
    "loss": LOSS,
    "丢包": LOSS,
    "timeout": TIMEOUT,
    "超时": TIMEOUT,
    "dup_ack": DUP_ACK,
    "dupack": DUP_ACK,
    "duplicate_ack": DUP_ACK,
    "重复确认": DUP_ACK,
}


@dataclass(frozen=True)
class TraceEvent:
    index: int
    ts_ms: int
    conn_id: str
    event_type: str
    payload_bytes: int
    ack_seq: int | None

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "index": self.index,
            "ts_ms": self.ts_ms,
            "conn_id": self.conn_id,
            "event_type": self.event_type,
            "payload_bytes": self.payload_bytes,
        }
        if self.ack_seq is not None:
            value["ack_seq"] = self.ack_seq
        return value


def _require_int(value: Any, field: str, index: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BusinessError(
            "E_TRACE_INVALID",
            context={
                "record_index": index,
                "field": field,
                "value": value,
                "rule": "integer_required",
            },
        )
    return value


def parse_events(
    raw_events: Iterable[Any],
    *,
    previous_ts_ms: int = -1,
    max_events: int,
) -> list[TraceEvent]:
    if not isinstance(raw_events, list):
        raise BusinessError(
            "E_TRACE_INVALID",
            context={"rule": "events_must_be_array", "actual_type": type(raw_events).__name__},
        )
    if len(raw_events) > max_events:
        raise BusinessError(
            "E_TRACE_INVALID",
            context={
                "rule": "too_many_events",
                "event_count": len(raw_events),
                "max_events_per_batch": max_events,
            },
        )

    parsed: list[TraceEvent] = []
    last_ts = previous_ts_ms
    for local_index, raw in enumerate(raw_events):
        if not isinstance(raw, dict):
            raise BusinessError(
                "E_TRACE_INVALID",
                context={"record_index": local_index, "rule": "record_must_be_object"},
            )
        ts_ms = _require_int(raw.get("ts_ms"), "ts_ms", local_index)
        if ts_ms < 0:
            raise BusinessError(
                "E_TRACE_INVALID",
                context={"record_index": local_index, "field": "ts_ms", "value": ts_ms, "rule": "non_negative"},
            )
        if ts_ms < last_ts:
            raise BusinessError(
                "E_TRACE_INVALID",
                context={
                    "record_index": local_index,
                    "field": "ts_ms",
                    "value": ts_ms,
                    "previous_ts_ms": last_ts,
                    "rule": "non_decreasing_time",
                },
            )

        conn_id = raw.get("conn_id")
        if not isinstance(conn_id, str) or not conn_id.strip():
            raise BusinessError(
                "E_TRACE_INVALID",
                context={"record_index": local_index, "field": "conn_id", "rule": "non_empty_string"},
            )

        raw_type = raw.get("event_type")
        if not isinstance(raw_type, str):
            raise BusinessError(
                "E_TRACE_INVALID",
                context={"record_index": local_index, "field": "event_type", "rule": "string_required"},
            )
        event_type = _TYPE_ALIASES.get(raw_type.strip().lower())
        if event_type is None:
            raise BusinessError(
                "E_TRACE_INVALID",
                context={
                    "record_index": local_index,
                    "field": "event_type",
                    "value": raw_type,
                    "allowed": sorted(VALID_EVENT_TYPES),
                },
            )

        payload_bytes = _require_int(raw.get("payload_bytes"), "payload_bytes", local_index)
        if payload_bytes < 0:
            raise BusinessError(
                "E_TRACE_INVALID",
                context={
                    "record_index": local_index,
                    "field": "payload_bytes",
                    "value": payload_bytes,
                    "rule": "non_negative",
                },
            )

        ack_seq_raw = raw.get("ack_seq")
        ack_seq: int | None = None
        if ack_seq_raw is not None:
            ack_seq = _require_int(ack_seq_raw, "ack_seq", local_index)
            if ack_seq < 0:
                raise BusinessError(
                    "E_TRACE_INVALID",
                    context={
                        "record_index": local_index,
                        "field": "ack_seq",
                        "value": ack_seq,
                        "rule": "non_negative",
                    },
                )
            if event_type not in {ACK, DUP_ACK}:
                raise BusinessError(
                    "E_TRACE_INVALID",
                    context={
                        "record_index": local_index,
                        "field": "ack_seq",
                        "rule": "only_ack_events_may_carry_ack_seq",
                    },
                )

        parsed.append(
            TraceEvent(
                index=-1,
                ts_ms=ts_ms,
                conn_id=conn_id,
                event_type=event_type,
                payload_bytes=payload_bytes,
                ack_seq=ack_seq,
            )
        )
        last_ts = ts_ms

    return parsed


def assign_global_indices(events: list[TraceEvent], start_index: int) -> list[TraceEvent]:
    return [
        TraceEvent(
            index=start_index + offset,
            ts_ms=event.ts_ms,
            conn_id=event.conn_id,
            event_type=event.event_type,
            payload_bytes=event.payload_bytes,
            ack_seq=event.ack_seq,
        )
        for offset, event in enumerate(events)
    ]
