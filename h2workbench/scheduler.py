from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Iterable

from .models import (
    BlockedPlan,
    ConnectionConfig,
    JobStatus,
    RoundPlan,
    RoundStreamPlan,
    RunStatus,
    SENDABLE_LOCAL_STATES,
    SchedulePlan,
    SimConnection,
    SimJob,
    SimStream,
    StreamState,
)


@dataclass(frozen=True)
class ScheduleOptions:
    mode: str
    round_budget: int
    max_rounds: int
    candidate_threshold: int
    candidate_limit: int
    priority_scale: int

    @classmethod
    def from_config(
        cls,
        config: ConnectionConfig,
        *,
        mode: str | None = None,
        round_budget: int | None = None,
        max_rounds: int | None = None,
        candidate_threshold: int | None = None,
        candidate_limit: int | None = None,
    ) -> "ScheduleOptions":
        selected_mode = (mode or "auto").strip().lower()
        if selected_mode not in {"auto", "exact", "approx"}:
            raise ValueError("scheduler_mode must be auto, exact, or approx")
        threshold = candidate_threshold or config.candidate_threshold
        limit = candidate_limit or config.candidate_limit
        if limit > threshold:
            limit = threshold
        return cls(
            mode=selected_mode,
            round_budget=round_budget or config.round_budget,
            max_rounds=max_rounds or config.max_rounds,
            candidate_threshold=threshold,
            candidate_limit=limit,
            priority_scale=config.priority_scale,
        )


def _priority_weight(priority: int, scale: int) -> int:
    return max(1, scale // (1 << min(priority, 12)))


def _allocate_weighted(
    capacities: dict[int, int], weights: dict[int, int], budget: int
) -> dict[int, int]:
    """Deterministic integer apportionment with hard per-key capacities.

    Recomputes the proportional quotas whenever keys hit their capacity.  A
    final one-byte-at-a-time pass handles budgets smaller than the number of
    active keys; ties are always broken by key ascending.
    """

    allocation = {key: 0 for key in capacities}
    active = {key for key, cap in capacities.items() if cap > 0 and weights.get(key, 0) > 0}
    remaining = max(0, budget)
    while remaining > 0 and active:
        total_weight = sum(weights[key] for key in active)
        if total_weight <= 0:
            break
        raw = {key: (remaining * weights[key]) // total_weight for key in active}
        used = 0
        for key in sorted(active):
            take = min(raw[key], capacities[key] - allocation[key])
            if take > 0:
                allocation[key] += take
                used += take
        remaining -= used
        for key in list(active):
            if allocation[key] >= capacities[key]:
                active.remove(key)
        if used == 0:
            for key in sorted(active, key=lambda item: (-weights[item], item)):
                if remaining <= 0:
                    break
                if allocation[key] < capacities[key]:
                    allocation[key] += 1
                    remaining -= 1
            for key in list(active):
                if allocation[key] >= capacities[key]:
                    active.remove(key)
    return allocation


def _candidate_rank(stream: SimStream) -> tuple[int, int, int, int, int]:
    return (
        stream.priority,
        -stream.weight,
        stream.sent_unacked,
        -stream.pending_bytes,
        stream.stream_id,
    )


def _consume_stream_jobs(
    stream: SimStream,
    jobs_by_stream: dict[int, list[SimJob]],
    amount: int,
) -> tuple[list[tuple[int, int]], bool]:
    consumed: list[tuple[int, int]] = []
    left = amount
    ended = False
    for job in jobs_by_stream.get(stream.stream_id, []):
        if left <= 0:
            break
        if job.status != JobStatus.PENDING.value or job.remaining_bytes <= 0:
            continue
        take = min(left, job.remaining_bytes)
        job.remaining_bytes -= take
        stream.pending_bytes -= take
        left -= take
        consumed.append((job.job_key, take))
        if job.remaining_bytes == 0:
            job.status = JobStatus.SENT.value
            if job.end_stream:
                ended = True
                if stream.state is StreamState.OPEN:
                    stream.state = StreamState.HALF_CLOSED_LOCAL
                elif stream.state is StreamState.HALF_CLOSED_REMOTE:
                    stream.state = StreamState.CLOSED
                break
    return consumed, ended


def _apply_zero_length_round(
    conn: SimConnection,
    streams: dict[int, SimStream],
    jobs_by_stream: dict[int, list[SimJob]],
    round_index: int,
) -> RoundPlan | None:
    zero_jobs = [
        job
        for jobs in jobs_by_stream.values()
        for job in jobs
        if job.status == JobStatus.PENDING.value and job.remaining_bytes == 0
    ]
    if not zero_jobs:
        return None
    zero_jobs.sort(key=lambda job: (job.stream_id, job.job_key))
    selections: list[RoundStreamPlan] = []
    for job in zero_jobs:
        stream = streams[job.stream_id]
        before = stream.state
        job.status = JobStatus.SENT.value
        ended = False
        if job.end_stream:
            ended = True
            if stream.state is StreamState.OPEN:
                stream.state = StreamState.HALF_CLOSED_LOCAL
            elif stream.state is StreamState.HALF_CLOSED_REMOTE:
                stream.state = StreamState.CLOSED
        selections.append(
            RoundStreamPlan(
                stream_id=stream.stream_id,
                bytes=0,
                priority=stream.priority,
                weight=stream.weight,
                reason=(
                    "zero-length DATA; no flow-control bytes consumed"
                    + ("; END_STREAM transition applied" if ended else "")
                ),
                job_consumption=[(job.job_key, 0)],
                end_stream=ended,
                state_before=before,
                state_after=stream.state,
            )
        )
    return RoundPlan(
        round_index=round_index,
        algorithm="zero-length",
        candidate_count=0,
        selected_candidate_count=0,
        truncated_count=0,
        approximation={
            "note": "zero-length DATA frames need no candidate ranking or flow-control budget",
            "error_bound_bytes": 0,
        },
        selections=selections,
        blocked=[],
        connection_window_before=conn.connection_window,
        connection_window_after=conn.connection_window,
        selected_bytes=0,
    )


def _terminal_blocked(
    conn: SimConnection,
    streams: dict[int, SimStream],
    scope_stream_ids: set[int],
) -> list[BlockedPlan]:
    blocked: list[BlockedPlan] = []
    for stream_id in sorted(scope_stream_ids):
        stream = streams[stream_id]
        if stream.pending_bytes <= 0:
            continue
        if stream.state not in SENDABLE_LOCAL_STATES:
            code = "E_INVALID_TRANSITION"
            reason = f"stream state {stream.state.value} is not sendable"
        elif conn.connection_window <= 0:
            code = "E_FLOW_CONTROL_BLOCKED"
            reason = "connection flow-control window is exhausted"
        elif stream.window <= 0:
            code = "E_FLOW_CONTROL_BLOCKED"
            reason = "stream flow-control window is exhausted"
        else:
            code = "E_FLOW_CONTROL_BLOCKED"
            reason = "scheduler produced no progress for this stream"
        blocked.append(
            BlockedPlan(
                stream_id=stream_id,
                required_bytes=stream.pending_bytes,
                pending_bytes=stream.pending_bytes,
                connection_window=conn.connection_window,
                stream_window=stream.window,
                code=code,
                reason=reason,
            )
        )
    return blocked


def build_schedule_plan(
    *,
    run_id: str,
    conn: SimConnection,
    streams: dict[int, SimStream],
    jobs: Iterable[SimJob],
    options: ScheduleOptions,
) -> SchedulePlan:
    """Build a complete deterministic round plan against a cloned state."""

    jobs_list = sorted(jobs, key=lambda job: (job.job_key, job.stream_id))
    jobs_by_stream: dict[int, list[SimJob]] = {}
    scope_stream_ids: set[int] = set()
    for job in jobs_list:
        jobs_by_stream.setdefault(job.stream_id, []).append(job)
        scope_stream_ids.add(job.stream_id)

    for stream_id in scope_stream_ids:
        stream = streams[stream_id]
        stream.pending_bytes = sum(
            job.remaining_bytes
            for job in jobs_by_stream.get(stream_id, [])
            if job.status == JobStatus.PENDING.value
        )

    rounds: list[RoundPlan] = []
    round_index = 1

    zero_round = _apply_zero_length_round(conn, streams, jobs_by_stream, round_index)
    if zero_round is not None:
        rounds.append(zero_round)
        round_index += 1

    final_status = RunStatus.COMPLETED.value
    error_context: dict | None = None
    note: str | None = None

    while True:
        pending_streams = [
            streams[stream_id]
            for stream_id in scope_stream_ids
            if streams[stream_id].pending_bytes > 0
        ]
        if not pending_streams:
            break
        if len(rounds) >= options.max_rounds:
            final_status = RunStatus.LIMIT_REACHED.value
            note = (
                "max_rounds was reached with pending DATA; no extra unsupported exit result was inferred"
            )
            break

        window_before = conn.connection_window
        allocatable = [
            stream
            for stream in pending_streams
            if stream.state in SENDABLE_LOCAL_STATES
            and conn.connection_window > 0
            and stream.window > 0
        ]
        candidate_pool = allocatable
        truncated = 0
        if options.mode in {"auto", "approx"} and len(candidate_pool) > options.candidate_threshold:
            selected_candidates = heapq.nsmallest(
                options.candidate_limit, candidate_pool, key=_candidate_rank
            )
            truncated = len(candidate_pool) - len(selected_candidates)
        else:
            selected_candidates = sorted(candidate_pool, key=_candidate_rank)

        selected_ids = {stream.stream_id for stream in selected_candidates}
        omitted = [stream for stream in candidate_pool if stream.stream_id not in selected_ids]
        omitted_pending = sum(stream.pending_bytes for stream in omitted)
        total_weight = sum(stream.weight for stream in candidate_pool)
        omitted_weight = sum(stream.weight for stream in omitted)
        algorithm = "bounded_approx" if truncated else "exact"
        approximation = {
            "mode_requested": options.mode,
            "algorithm": algorithm,
            "candidate_count": len(candidate_pool),
            "selected_candidate_count": len(selected_candidates),
            "truncated_count": truncated,
            "omitted_pending_bytes": omitted_pending,
            "error_bound_bytes": min(options.round_budget, omitted_pending) if truncated else 0,
            "weighted_mass_omitted_ratio": (
                round(omitted_weight / total_weight, 12) if truncated and total_weight else 0.0
            ),
            "note": (
                "Candidate ranking was bounded; omitted streams are reconsidered next round."
                if truncated
                else "Exact candidate set; truncation error is zero."
            ),
        }

        selections: list[RoundStreamPlan] = []
        selected_bytes = 0
        budget = min(options.round_budget, conn.connection_window)

        if selected_candidates:
            groups: dict[int, list[SimStream]] = {}
            for stream in selected_candidates:
                groups.setdefault(stream.priority, []).append(stream)
            group_caps = {
                priority: sum(min(stream.pending_bytes, stream.window) for stream in group)
                for priority, group in groups.items()
            }
            group_weights = {
                priority: _priority_weight(priority, options.priority_scale)
                for priority in groups
            }
            group_budget = _allocate_weighted(group_caps, group_weights, budget)

            for priority in sorted(groups):
                group = sorted(groups[priority], key=lambda item: item.stream_id)
                stream_caps = {
                    stream.stream_id: min(stream.pending_bytes, stream.window)
                    for stream in group
                }
                stream_weights = {stream.stream_id: stream.weight for stream in group}
                allocations = _allocate_weighted(
                    stream_caps, stream_weights, group_budget[priority]
                )
                ordered_streams = sorted(
                    group,
                    key=lambda stream: (
                        -allocations[stream.stream_id],
                        -stream.weight,
                        stream.stream_id,
                    ),
                )
                for stream in ordered_streams:
                    amount = allocations[stream.stream_id]
                    if amount <= 0:
                        continue
                    before_state = stream.state
                    conn_window_before_stream = conn.connection_window
                    stream_window_before = stream.window
                    consumed, ended = _consume_stream_jobs(stream, jobs_by_stream, amount)
                    if not consumed:
                        continue
                    actual = sum(item[1] for item in consumed)
                    if actual <= 0:
                        continue
                    conn.connection_window -= actual
                    stream.window -= actual
                    stream.sent_unacked += actual
                    stream.sent_total += actual
                    selected_bytes += actual
                    reason = (
                        f"priority={priority};weight={stream.weight};"
                        f"priority_quantum={group_budget[priority]};"
                        f"weighted_share={amount};occupancy_before={stream.sent_unacked - actual};"
                        f"conn_window={conn_window_before_stream}->{conn.connection_window};"
                        f"stream_window={stream_window_before}->{stream.window};"
                        f"algorithm={algorithm}"
                    )
                    if ended:
                        reason += ";END_STREAM transmitted"
                    selections.append(
                        RoundStreamPlan(
                            stream_id=stream.stream_id,
                            bytes=actual,
                            priority=priority,
                            weight=stream.weight,
                            reason=reason,
                            job_consumption=consumed,
                            end_stream=ended,
                            state_before=before_state,
                            state_after=stream.state,
                        )
                    )

        blocked: list[BlockedPlan] = []
        if selected_bytes == 0 and pending_streams:
            blocked = _terminal_blocked(
                conn, streams, scope_stream_ids
            )
            final_status = RunStatus.BLOCKED.value
            error_context = {
                "connection_id": conn.connection_id,
                "connection_window": conn.connection_window,
                "required_bytes": max(
                    (entry.required_bytes for entry in blocked), default=0
                ),
                "streams": [
                    {
                        "stream_id": entry.stream_id,
                        "connection_window": entry.connection_window,
                        "stream_window": entry.stream_window,
                        "required_bytes": entry.required_bytes,
                        "pending_bytes": entry.pending_bytes,
                        "reason": entry.reason,
                    }
                    for entry in blocked
                ],
            }

        rounds.append(
            RoundPlan(
                round_index=round_index,
                algorithm=algorithm,
                candidate_count=len(candidate_pool),
                selected_candidate_count=len(selected_candidates),
                truncated_count=truncated,
                approximation=approximation,
                selections=selections,
                blocked=blocked,
                connection_window_before=window_before,
                connection_window_after=conn.connection_window,
                selected_bytes=selected_bytes,
                final_status=final_status if final_status != RunStatus.COMPLETED.value else None,
            )
        )
        round_index += 1
        if final_status == RunStatus.BLOCKED.value:
            break

    if final_status == RunStatus.COMPLETED.value:
        still_pending = [
            job
            for job in jobs_list
            if job.status == JobStatus.PENDING.value and job.remaining_bytes > 0
        ]
        if still_pending:
            final_status = RunStatus.LIMIT_REACHED.value
            note = "internal scheduler stopped with pending jobs"

    return SchedulePlan(
        run_id=run_id,
        connection_id=conn.connection_id,
        sample_id=None,
        rounds=rounds,
        final_status=final_status,
        error_context=error_context,
        note=note,
    )
