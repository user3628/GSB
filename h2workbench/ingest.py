from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import BusinessError
from .models import (
    ACTIVE_STATES,
    SENDABLE_LOCAL_STATES,
    ConnectionConfig,
    FrameSpec,
    FrameType,
    JobStatus,
    SampleSpec,
    SimConnection,
    SimJob,
    SimStream,
    StreamState,
    Transition,
    Violation,
)


@dataclass
class IngestPlan:
    spec: SampleSpec
    connection: SimConnection
    streams: dict[int, SimStream]
    jobs: list[SimJob]
    transitions: list[Transition]
    violations: list[Violation]
    batches: list[dict[str, int]]
    new_connection: bool
    cancel_all_existing_jobs: bool
    cancel_existing_stream_ids: set[int]


def _invalid_transition(
    conn: SimConnection,
    frame: FrameSpec,
    message: str,
    **context: Any,
) -> BusinessError:
    return BusinessError(
        code="E_INVALID_TRANSITION",
        message=message,
        connection_id=conn.connection_id,
        context={
            "frame_index": frame.index,
            "stream_id": frame.stream_id,
            "frame_type": frame.frame_type.value,
            **context,
        },
    )


def _transition(
    transitions: list[Transition],
    stream_id: int,
    from_state: StreamState,
    to_state: StreamState,
    trigger: str,
    frame_index: int,
) -> None:
    if from_state is to_state:
        return
    transitions.append(
        Transition(
            stream_id=stream_id,
            from_state=from_state,
            to_state=to_state,
            trigger=trigger,
            frame_index=frame_index,
        )
    )


def _batch_ranges(count: int, batch_size: int) -> list[dict[str, int]]:
    batches: list[dict[str, int]] = []
    for batch_index, start in enumerate(range(0, count, batch_size)):
        end = min(count, start + batch_size)
        batches.append(
            {
                "batch_index": batch_index,
                "frame_start": start,
                "frame_end_exclusive": end,
                "frame_count": end - start,
            }
        )
    return batches


def build_ingest_plan(
    spec: SampleSpec,
    *,
    connection: SimConnection | None,
    streams: dict[int, SimStream] | None,
) -> IngestPlan:
    new_connection = connection is None
    if connection is None:
        config = spec.config or ConnectionConfig()
        conn = SimConnection(
            connection_id=spec.connection_id,
            initial_connection_window=config.connection_window,
            connection_window=config.connection_window,
            initial_stream_window=config.stream_window,
            closed=False,
            config=config,
        )
    else:
        conn = SimConnection(
            connection_id=connection.connection_id,
            initial_connection_window=connection.initial_connection_window,
            connection_window=connection.connection_window,
            initial_stream_window=connection.initial_stream_window,
            closed=connection.closed,
            config=connection.config,
            protocol_violations=connection.protocol_violations,
        )

    working_streams = {
        stream_id: SimStream(
            stream_id=stream.stream_id,
            state=stream.state,
            initial_window=stream.initial_window,
            window=stream.window,
            sent_unacked=stream.sent_unacked,
            pending_bytes=stream.pending_bytes,
            weight=stream.weight,
            priority=stream.priority,
            sent_total=stream.sent_total,
            pending_end_stream=stream.pending_end_stream,
            created=False,
        )
        for stream_id, stream in (streams or {}).items()
    }

    jobs: list[SimJob] = []
    transitions: list[Transition] = []
    violations: list[Violation] = []
    cancel_existing_stream_ids: set[int] = set()
    cancel_all_existing_jobs = False

    for frame in spec.frames:
        if conn.closed:
            raise _invalid_transition(
                conn,
                frame,
                "connection is closed; no further frame may advance state",
                connection_closed=True,
            )

        if frame.frame_type is FrameType.HEADERS:
            stream = working_streams.get(frame.stream_id)
            if stream is None:
                stream = SimStream(
                    stream_id=frame.stream_id,
                    state=StreamState.IDLE,
                    initial_window=conn.initial_stream_window,
                    window=conn.initial_stream_window,
                    sent_unacked=0,
                    pending_bytes=0,
                    weight=frame.weight or 8,
                    priority=frame.priority if frame.priority is not None else 16,
                    created=True,
                )
                working_streams[frame.stream_id] = stream
            else:
                if stream.state in {StreamState.CLOSED, StreamState.RESET}:
                    raise _invalid_transition(
                        conn,
                        frame,
                        "HEADERS on a closed or reset stream is not allowed",
                        state=stream.state.value,
                    )
                if (
                    frame.direction == "local"
                    and "END_STREAM" in frame.flags
                    and stream.pending_bytes > 0
                ):
                    raise _invalid_transition(
                        conn,
                        frame,
                        "local HEADERS END_STREAM cannot overtake queued DATA",
                        state=stream.state.value,
                        pending_bytes=stream.pending_bytes,
                    )
                if stream.pending_end_stream:
                    raise _invalid_transition(
                        conn,
                        frame,
                        "HEADERS cannot follow queued END_STREAM DATA",
                        state=stream.state.value,
                    )
                if frame.weight is not None:
                    stream.weight = frame.weight
                if frame.priority is not None:
                    stream.priority = frame.priority

            before = stream.state
            if stream.state is StreamState.IDLE:
                stream.state = StreamState.OPEN
            elif frame.direction == "local":
                if stream.state is StreamState.HALF_CLOSED_LOCAL:
                    raise _invalid_transition(
                        conn,
                        frame,
                        "local HEADERS on HALF_CLOSED_LOCAL is not allowed",
                        state=stream.state.value,
                    )
                if stream.state is StreamState.HALF_CLOSED_REMOTE and "END_STREAM" in frame.flags:
                    stream.state = StreamState.CLOSED
            else:
                if stream.state is StreamState.HALF_CLOSED_REMOTE:
                    raise _invalid_transition(
                        conn,
                        frame,
                        "remote HEADERS on HALF_CLOSED_REMOTE is not allowed",
                        state=stream.state.value,
                    )
                if stream.state is StreamState.HALF_CLOSED_LOCAL and "END_STREAM" in frame.flags:
                    stream.state = StreamState.CLOSED

            if "END_STREAM" in frame.flags and stream.state is StreamState.OPEN:
                if frame.direction == "local":
                    stream.state = StreamState.HALF_CLOSED_LOCAL
                else:
                    stream.state = StreamState.HALF_CLOSED_REMOTE
            trigger = (
                "headers_end_stream_local"
                if frame.direction == "local" and "END_STREAM" in frame.flags
                else "headers_end_stream_remote"
                if frame.direction == "remote" and "END_STREAM" in frame.flags
                else "headers_local"
                if frame.direction == "local"
                else "headers_remote"
            )
            _transition(
                transitions,
                stream.stream_id,
                before,
                stream.state,
                trigger,
                frame.index,
            )
            continue

        if frame.frame_type is FrameType.DATA:
            stream = working_streams.get(frame.stream_id)
            if stream is None or stream.state is StreamState.IDLE:
                raise BusinessError(
                    code="E_FRAME_INVALID",
                    message="DATA frame was received on an unopened stream",
                    connection_id=conn.connection_id,
                    context={
                        "frame_index": frame.index,
                        "stream_id": frame.stream_id,
                        "state": "IDLE",
                        "frame_type": frame.frame_type.value,
                        "reason": "DATA on an unopened stream",
                    },
                )
            if stream.state not in SENDABLE_LOCAL_STATES:
                raise _invalid_transition(
                    conn,
                    frame,
                    "local DATA is not allowed in the current stream state",
                    state=stream.state.value,
                )
            if stream.pending_end_stream:
                raise _invalid_transition(
                    conn,
                    frame,
                    "DATA cannot follow a queued END_STREAM DATA frame",
                    state=stream.state.value,
                )
            if frame.weight is not None:
                stream.weight = frame.weight
            if frame.priority is not None:
                stream.priority = frame.priority
            job = SimJob(
                job_key=frame.index,
                sample_id=spec.sample_id,
                frame_index=frame.index,
                stream_id=stream.stream_id,
                remaining_bytes=frame.payload_bytes,
                original_bytes=frame.payload_bytes,
                end_stream="END_STREAM" in frame.flags,
            )
            jobs.append(job)
            stream.pending_bytes += frame.payload_bytes
            if job.end_stream:
                stream.pending_end_stream = True
            continue

        if frame.frame_type is FrameType.WINDOW_UPDATE:
            if frame.stream_id == 0:
                current = conn.connection_window
                limit = conn.initial_connection_window
            else:
                stream = working_streams.get(frame.stream_id)
                if stream is None or stream.state is StreamState.IDLE:
                    raise _invalid_transition(
                        conn,
                        frame,
                        "WINDOW_UPDATE on an idle or unknown stream is not allowed",
                        state="IDLE",
                    )
                if stream.state in {StreamState.CLOSED, StreamState.RESET}:
                    raise _invalid_transition(
                        conn,
                        frame,
                        "WINDOW_UPDATE on a closed or reset stream is not allowed",
                        state=stream.state.value,
                    )
                current = stream.window
                limit = stream.initial_window

            room = max(0, limit - current)
            applied = min(frame.window_increment, room)
            excess = frame.window_increment - applied
            if frame.window_increment == 0:
                violations.append(
                    Violation(
                        stream_id=frame.stream_id,
                        code="WINDOW_UPDATE_ZERO",
                        requested_increment=0,
                        applied_increment=0,
                        current_before=current,
                        initial_limit=limit,
                        frame_index=frame.index,
                        details={"action": "ignored zero increment"},
                    )
                )
                conn.protocol_violations += 1
            elif excess > 0:
                violations.append(
                    Violation(
                        stream_id=frame.stream_id,
                        code="WINDOW_UPDATE_OVERFLOW_TRUNCATED",
                        requested_increment=frame.window_increment,
                        applied_increment=applied,
                        current_before=current,
                        initial_limit=limit,
                        frame_index=frame.index,
                        details={
                            "excess": excess,
                            "rule": "window is capped at its configured initial limit",
                        },
                    )
                )
                conn.protocol_violations += 1

            if frame.stream_id == 0:
                conn.connection_window = min(limit, current + applied)
            else:
                stream = working_streams[frame.stream_id]
                stream.window = min(limit, current + applied)
                stream.sent_unacked = max(0, stream.sent_unacked - applied)
            continue

        if frame.frame_type is FrameType.RST_STREAM:
            stream = working_streams.get(frame.stream_id)
            if stream is None or stream.state is StreamState.IDLE:
                raise _invalid_transition(
                    conn,
                    frame,
                    "RST_STREAM on an idle or unknown stream is not allowed",
                    state="IDLE",
                )
            if stream.state not in ACTIVE_STATES:
                raise _invalid_transition(
                    conn,
                    frame,
                    "RST_STREAM is only valid on an active stream",
                    state=stream.state.value,
                )
            before = stream.state
            stream.state = StreamState.RESET
            stream.pending_bytes = 0
            stream.pending_end_stream = False
            for job in jobs:
                if job.stream_id == stream.stream_id and job.status == JobStatus.PENDING.value:
                    job.status = JobStatus.CANCELLED.value
                    job.remaining_bytes = 0
            cancel_existing_stream_ids.add(stream.stream_id)
            _transition(
                transitions,
                stream.stream_id,
                before,
                StreamState.RESET,
                "rst_stream",
                frame.index,
            )
            continue

        if frame.frame_type is FrameType.SETTINGS:
            if "CONNECTION_CLOSE" in frame.flags or "CLOSE" in frame.flags:
                if conn.closed:
                    raise _invalid_transition(
                        conn, frame, "connection is already closed", state="CLOSED"
                    )
                conn.closed = True
                cancel_all_existing_jobs = True
                for stream in working_streams.values():
                    if stream.state in ACTIVE_STATES:
                        before = stream.state
                        stream.state = StreamState.CLOSED
                        stream.pending_bytes = 0
                        stream.pending_end_stream = False
                        _transition(
                            transitions,
                            stream.stream_id,
                            before,
                            StreamState.CLOSED,
                            "settings_connection_close",
                            frame.index,
                        )
                for job in jobs:
                    if job.status == JobStatus.PENDING.value:
                        job.status = JobStatus.CANCELLED.value
                        job.remaining_bytes = 0
            continue

        raise BusinessError(
            code="E_FRAME_INVALID",
            message="unsupported frame type",
            connection_id=conn.connection_id,
            context={"frame_index": frame.index, "frame_type": frame.frame_type.value},
        )

    return IngestPlan(
        spec=spec,
        connection=conn,
        streams=working_streams,
        jobs=jobs,
        transitions=transitions,
        violations=violations,
        batches=_batch_ranges(len(spec.frames), spec.batch_size),
        new_connection=new_connection,
        cancel_all_existing_jobs=cancel_all_existing_jobs,
        cancel_existing_stream_ids=cancel_existing_stream_ids,
    )
