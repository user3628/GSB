"""Deterministic inference batch construction and sample-preserving splits."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .errors import E_BATCH_TOO_LARGE, E_BATCH_UNPLACEABLE, SchedulerError


@dataclass(frozen=True)
class RequestPlanInput:
    request_id: str
    model_id: str
    node_id: str
    quota_group: str
    memory_bytes: int
    compute_millis: int
    samples_per_batch: int
    expected_latency_millis: int
    priority: int
    submission_seq: int


@dataclass(frozen=True)
class Piece:
    request_id: str
    model_id: str
    node_id: str
    quota_group: str
    piece_index: int
    samples: int
    memory_bytes: int
    compute_millis: int
    expected_latency_millis: int
    priority: int
    submission_seq: int


@dataclass
class BatchPlan:
    model_id: str
    node_id: str
    quota_group: str
    strategy: str
    total_samples: int
    memory_bytes: int
    compute_millis: int
    estimated_latency_millis: int
    priority: int
    first_submission_seq: int
    members: list[Piece] = field(default_factory=list)
    reasons: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PlanResult:
    strategy: str
    batches: list[BatchPlan]
    planned_request_ids: list[str]
    unplanned_request_ids: list[str]
    adjustments: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)


def _partition(total: int, samples_parts: Sequence[int], total_samples: int) -> list[int]:
    if total <= 0:
        return [0 for _ in samples_parts]
    values: list[int] = []
    consumed_samples = 0
    consumed_value = 0
    for samples in samples_parts[:-1]:
        consumed_samples += samples
        boundary = (total * consumed_samples) // total_samples
        value = boundary - consumed_value
        values.append(value)
        consumed_value = boundary
    values.append(total - consumed_value)
    return values


def split_request(
    request: RequestPlanInput,
    *,
    max_batch_samples: int,
    unit_sample_cost_millis: int,
    batch_sample_target: int = 0,
) -> tuple[list[Piece], list[dict[str, Any]]]:
    """Split one request into sample-preserving pieces when needed."""

    if request.samples_per_batch <= 0:
        raise SchedulerError(
            E_BATCH_TOO_LARGE,
            "request has a non-positive sample count",
            details={"request_id": request.request_id,
                     "samples_per_batch": request.samples_per_batch},
        )
    latency_samples = request.expected_latency_millis // unit_sample_cost_millis
    if latency_samples < 1:
        raise SchedulerError(
            E_BATCH_TOO_LARGE,
            "even one sample exceeds the request latency limit",
            details={
                "request_id": request.request_id,
                "samples_per_batch": request.samples_per_batch,
                "expected_latency_millis": request.expected_latency_millis,
                "unit_sample_cost_millis": unit_sample_cost_millis,
                "minimum_one_sample_latency_millis": unit_sample_cost_millis,
            },
        )

    constraints: list[tuple[str, int]] = [("model_max_batch_samples", max_batch_samples)]
    if latency_samples < max_batch_samples:
        constraints.append(("latency_limit", latency_samples))
    if batch_sample_target > 0 and batch_sample_target < min(
        max_batch_samples, latency_samples
    ):
        constraints.append(("configured_batch_sample_target", batch_sample_target))
    piece_limit = min(value for _, value in constraints)
    if piece_limit <= 0:
        raise SchedulerError(
            E_BATCH_TOO_LARGE,
            "batch sample limit is zero",
            details={"request_id": request.request_id},
        )

    if request.samples_per_batch <= piece_limit:
        sample_parts = [request.samples_per_batch]
    else:
        remaining = request.samples_per_batch
        sample_parts = []
        while remaining > 0:
            sample_parts.append(min(piece_limit, remaining))
            remaining -= sample_parts[-1]

    memory_parts = _partition(
        request.memory_bytes, sample_parts, request.samples_per_batch
    )
    compute_parts = _partition(
        request.compute_millis, sample_parts, request.samples_per_batch
    )
    pieces = [
        Piece(
            request_id=request.request_id,
            model_id=request.model_id,
            node_id=request.node_id,
            quota_group=request.quota_group,
            piece_index=index,
            samples=samples,
            memory_bytes=memory_parts[index],
            compute_millis=compute_parts[index],
            expected_latency_millis=request.expected_latency_millis,
            priority=request.priority,
            submission_seq=request.submission_seq,
        )
        for index, samples in enumerate(sample_parts)
    ]
    adjustments: list[dict[str, Any]] = []
    if len(sample_parts) > 1:
        for index, samples in enumerate(sample_parts):
            adjustments.append(
                {
                    "type": "sample_split",
                    "reason": "batch_limit_exceeded",
                    "constraints": [name for name, value in constraints if value == piece_limit],
                    "request_id": request.request_id,
                    "piece_index": index,
                    "piece_samples": samples,
                    "original_samples": request.samples_per_batch,
                    "total_samples_preserved": sum(sample_parts)
                    == request.samples_per_batch,
                    "original_memory_bytes": request.memory_bytes,
                    "piece_memory_bytes": memory_parts[index],
                    "total_memory_preserved": sum(memory_parts)
                    == request.memory_bytes,
                }
            )
    return pieces, adjustments


def _request_piece_groups(
    pieces: Iterable[Piece],
) -> dict[str, list[Piece]]:
    grouped: dict[str, list[Piece]] = {}
    for piece in pieces:
        grouped.setdefault(piece.request_id, []).append(piece)
    for values in grouped.values():
        values.sort(key=lambda item: item.piece_index)
    return grouped


def _select_requests(
    request_order: Sequence[RequestPlanInput],
    grouped: Mapping[str, list[Piece]],
    *,
    node_memory_left: int,
    node_compute_left: int,
    reasons_by_request: Mapping[str, list[dict[str, Any]]],
) -> tuple[list[str], list[str], list[dict[str, Any]], int, int]:
    planned: list[str] = []
    unplanned: list[str] = []
    errors: list[dict[str, Any]] = []
    memory_left = node_memory_left
    compute_left = node_compute_left
    for request in request_order:
        request_pieces = grouped[request.request_id]
        memory = sum(piece.memory_bytes for piece in request_pieces)
        compute = sum(piece.compute_millis for piece in request_pieces)
        if memory > memory_left or compute > compute_left:
            unplanned.append(request.request_id)
            errors.append(
                {
                    "code": E_BATCH_UNPLACEABLE,
                    "message": "request does not fit in node remaining GPU resources",
                    "request_id": request.request_id,
                    "node_id": request.node_id,
                    "required_memory_bytes": memory,
                    "remaining_memory_bytes": memory_left,
                    "required_compute_millis": compute,
                    "remaining_compute_millis": compute_left,
                    "adjustments": reasons_by_request.get(request.request_id, []),
                }
            )
            continue
        planned.append(request.request_id)
        memory_left -= memory
        compute_left -= compute
    return planned, unplanned, errors, memory_left, compute_left


def _fits(batch: BatchPlan, piece: Piece, *, max_batch_samples: int,
          unit_sample_cost_millis: int) -> bool:
    new_samples = batch.total_samples + piece.samples
    if new_samples > max_batch_samples:
        return False
    latency_cap = min(
        [member.expected_latency_millis for member in batch.members]
        + [piece.expected_latency_millis]
    )
    return new_samples * unit_sample_cost_millis <= latency_cap


def _append_piece(
    batch: BatchPlan,
    piece: Piece,
    *,
    unit_sample_cost_millis: int,
) -> None:
    batch.members.append(piece)
    batch.total_samples += piece.samples
    batch.memory_bytes += piece.memory_bytes
    batch.compute_millis += piece.compute_millis
    batch.estimated_latency_millis = (
        batch.total_samples * unit_sample_cost_millis
    )
    if piece.priority > batch.priority:
        batch.priority = piece.priority
    batch.first_submission_seq = min(batch.first_submission_seq, piece.submission_seq)


def _new_batch(piece: Piece, strategy: str, model_id: str,
               unit_sample_cost_millis: int) -> BatchPlan:
    return BatchPlan(
        model_id=model_id,
        node_id=piece.node_id,
        quota_group=piece.quota_group,
        strategy=strategy,
        total_samples=piece.samples,
        memory_bytes=piece.memory_bytes,
        compute_millis=piece.compute_millis,
        estimated_latency_millis=piece.samples * unit_sample_cost_millis,
        priority=piece.priority,
        first_submission_seq=piece.submission_seq,
        members=[piece],
        reasons=[{"reason": "group_start", "request_id": piece.request_id}],
    )


def _pack_submission(
    pieces: Sequence[Piece],
    *,
    model_id: str,
    max_batch_samples: int,
    unit_sample_cost_millis: int,
) -> list[BatchPlan]:
    ordered = sorted(pieces, key=lambda item: (item.submission_seq, item.piece_index))
    batches: list[BatchPlan] = []
    current: BatchPlan | None = None
    for piece in ordered:
        if current is not None and _fits(
            current,
            piece,
            max_batch_samples=max_batch_samples,
            unit_sample_cost_millis=unit_sample_cost_millis,
        ):
            _append_piece(current, piece, unit_sample_cost_millis=unit_sample_cost_millis)
            continue
        reason = "group_start"
        if current is not None:
            if current.total_samples + piece.samples > max_batch_samples:
                reason = "batch_sample_limit"
            else:
                reason = "batch_latency_limit"
        current = _new_batch(piece, "greedy_submission", model_id, unit_sample_cost_millis)
        current.reasons = [{"reason": reason, "request_id": piece.request_id}]
        batches.append(current)
    return batches


def _pack_memory_descending(
    pieces: Sequence[Piece],
    *,
    model_id: str,
    max_batch_samples: int,
    unit_sample_cost_millis: int,
) -> list[BatchPlan]:
    ordered = sorted(
        pieces,
        key=lambda item: (-item.memory_bytes, item.submission_seq, item.piece_index),
    )
    batches: list[BatchPlan] = []
    for piece in ordered:
        placed = False
        for batch in batches:
            if _fits(
                batch,
                piece,
                max_batch_samples=max_batch_samples,
                unit_sample_cost_millis=unit_sample_cost_millis,
            ):
                _append_piece(batch, piece, unit_sample_cost_millis=unit_sample_cost_millis)
                placed = True
                break
        if not placed:
            batch = _new_batch(
                piece, "memory_descending", model_id, unit_sample_cost_millis
            )
            batch.reasons = [
                {"reason": "first_fit_new_batch", "request_id": piece.request_id}
            ]
            batches.append(batch)
    batches.sort(key=lambda batch: batch.first_submission_seq)
    return batches


def _validate_batches(
    batches: Sequence[BatchPlan],
    *,
    max_batch_samples: int,
    unit_sample_cost_millis: int,
) -> None:
    seen_pieces: set[tuple[str, int]] = set()
    for batch in batches:
        if batch.total_samples > max_batch_samples:
            raise SchedulerError(
                E_BATCH_TOO_LARGE,
                "batch exceeds model maximum sample count",
                details={
                    "batch_model_id": batch.model_id,
                    "total_samples": batch.total_samples,
                    "max_batch_samples": max_batch_samples,
                },
            )
        actual_latency = batch.total_samples * unit_sample_cost_millis
        if actual_latency != batch.estimated_latency_millis:
            raise SchedulerError(
                E_BATCH_TOO_LARGE,
                "batch latency accounting mismatch",
                details={"batch_model_id": batch.model_id},
            )
        latency_cap = min(
            piece.expected_latency_millis for piece in batch.members
        )
        if actual_latency > latency_cap:
            raise SchedulerError(
                E_BATCH_TOO_LARGE,
                "batch exceeds a member's expected latency limit",
                details={
                    "batch_model_id": batch.model_id,
                    "estimated_latency_millis": actual_latency,
                    "latency_limit_millis": latency_cap,
                },
            )
        for piece in batch.members:
            key = (piece.request_id, piece.piece_index)
            if key in seen_pieces:
                raise SchedulerError(
                    E_BATCH_TOO_LARGE,
                    "request piece was packed more than once",
                    details={"request_id": piece.request_id,
                             "piece_index": piece.piece_index},
                )
            seen_pieces.add(key)


def _planning_metrics(
    batches: Sequence[BatchPlan],
    planned_request_ids: Sequence[str],
    unplanned_request_ids: Sequence[str],
) -> dict[str, Any]:
    return {
        "batch_count": len(batches),
        "planned_request_count": len(set(planned_request_ids)),
        "unplanned_request_count": len(set(unplanned_request_ids)),
        "total_estimated_latency_millis": sum(
            batch.estimated_latency_millis for batch in batches
        ),
        "total_samples": sum(batch.total_samples for batch in batches),
        "total_memory_bytes": sum(batch.memory_bytes for batch in batches),
    }


def plan_group(
    requests: Sequence[RequestPlanInput],
    *,
    model_id: str,
    max_batch_samples: int,
    unit_sample_cost_millis: int,
    node_memory_left: int,
    node_compute_left: int,
    strategy: str,
    auto_strategy_threshold: int,
    batch_sample_target: int = 0,
) -> PlanResult:
    """Plan one already-adjacent model/node group.

    Requests too large for the model or a latency cap are split into pieces
    whose samples and memory sum back to the original request exactly.  A
    request that still cannot fit due to occupied node resources is left
    ADMITTED with an explicit E_BATCH_UNPLACEABLE diagnostic.
    """

    if not requests:
        return PlanResult(
            strategy=strategy,
            batches=[],
            planned_request_ids=[],
            unplanned_request_ids=[],
            metrics=_planning_metrics([], [], []),
        )
    ordered_requests = sorted(
        requests, key=lambda request: (request.submission_seq, request.request_id)
    )
    pieces_by_request: dict[str, list[Piece]] = {}
    adjustments: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    valid_requests: list[RequestPlanInput] = []
    for request in ordered_requests:
        try:
            pieces, request_adjustments = split_request(
                request,
                max_batch_samples=max_batch_samples,
                unit_sample_cost_millis=unit_sample_cost_millis,
                batch_sample_target=batch_sample_target,
            )
            pieces_by_request[request.request_id] = pieces
            adjustments.extend(request_adjustments)
            valid_requests.append(request)
        except SchedulerError as exc:
            errors.append(
                {
                    "request_id": request.request_id,
                    **exc.to_dict(),
                }
            )

    strategies: list[str]
    explicit = strategy
    if explicit == "auto":
        if len(valid_requests) > auto_strategy_threshold:
            strategies = ["greedy_submission", "memory_descending"]
        else:
            strategies = ["greedy_submission"]
    elif explicit in {"greedy", "submission", "greedy_submission"}:
        strategies = ["greedy_submission"]
    elif explicit in {"memory", "memory_descending"}:
        strategies = ["memory_descending"]
    else:
        raise SchedulerError(
            "E_VALIDATION", f"unknown batch strategy {strategy!r}",
            details={"strategy": strategy},
        )

    candidates: list[PlanResult] = []
    for candidate_strategy in strategies:
        if candidate_strategy == "memory_descending":
            request_order = sorted(
                valid_requests,
                key=lambda request: (
                    -sum(piece.memory_bytes for piece in pieces_by_request[request.request_id]),
                    request.submission_seq,
                ),
            )
        else:
            request_order = list(valid_requests)
        grouped = _request_piece_groups(
            piece
            for request in valid_requests
            for piece in pieces_by_request[request.request_id]
        )
        reasons_by_request = {
            request.request_id: [
                adjustment
                for adjustment in adjustments
                if adjustment.get("request_id") == request.request_id
            ]
            for request in valid_requests
        }
        (
            planned_request_ids,
            unplanned_request_ids,
            candidate_errors,
            _memory_left,
            _compute_left,
        ) = _select_requests(
            request_order,
            grouped,
            node_memory_left=node_memory_left,
            node_compute_left=node_compute_left,
            reasons_by_request=reasons_by_request,
        )
        selected_pieces = [
            piece
            for request_id in planned_request_ids
            for piece in grouped[request_id]
        ]
        if candidate_strategy == "memory_descending":
            batches = _pack_memory_descending(
                selected_pieces,
                model_id=model_id,
                max_batch_samples=max_batch_samples,
                unit_sample_cost_millis=unit_sample_cost_millis,
            )
        else:
            batches = _pack_submission(
                selected_pieces,
                model_id=model_id,
                max_batch_samples=max_batch_samples,
                unit_sample_cost_millis=unit_sample_cost_millis,
            )
        _validate_batches(
            batches,
            max_batch_samples=max_batch_samples,
            unit_sample_cost_millis=unit_sample_cost_millis,
        )
        metrics = _planning_metrics(
            batches, planned_request_ids, unplanned_request_ids
        )
        candidates.append(
            PlanResult(
                strategy=candidate_strategy,
                batches=batches,
                planned_request_ids=planned_request_ids,
                unplanned_request_ids=unplanned_request_ids,
                adjustments=list(adjustments),
                errors=list(errors) + candidate_errors,
                metrics=metrics,
            )
        )

    def score(result: PlanResult) -> tuple[int, int, int, int]:
        return (
            int(result.metrics["unplanned_request_count"]),
            int(result.metrics["batch_count"]),
            int(result.metrics["total_estimated_latency_millis"]),
            0 if result.strategy == "greedy_submission" else 1,
        )

    selected = min(candidates, key=score)
    selected.metrics = dict(selected.metrics)
    selected.metrics["strategy_candidates"] = [
        {"strategy": candidate.strategy, **candidate.metrics}
        for candidate in candidates
    ]
    return selected

