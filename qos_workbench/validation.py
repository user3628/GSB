from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from .errors import (
    E_DEPENDENCY_CYCLE,
    E_FORMAT_UNSUPPORTED,
    E_PARENT_NOT_FOUND,
    E_POLICY_INVALID,
    E_RATE_EXCEEDED,
    BusinessError,
)
from .models import CURRENT_FORMAT_VERSION, Policy, PolicySnapshot


@dataclass(frozen=True, slots=True)
class DuplicateResolution:
    policies: tuple[Policy, ...]
    report: dict[str, Any]


def resolve_duplicate_queues(records: Sequence[Policy]) -> DuplicateResolution:
    """Resolve duplicate queue IDs deterministically: the last record wins."""
    winners: dict[str, tuple[int, Policy]] = {}
    occurrences: dict[str, list[int]] = {}
    for index, policy in enumerate(records):
        winners[policy.queue_id] = (index, policy)
        occurrences.setdefault(policy.queue_id, []).append(index)

    details: list[dict[str, Any]] = []
    for queue_id in sorted(occurrences):
        indexes = occurrences[queue_id]
        if len(indexes) > 1:
            winner_index = indexes[-1]
            details.append(
                {
                    "queue_id": queue_id,
                    "occurrences": indexes,
                    "winner_source_index": winner_index,
                    "discarded_source_indices": indexes[:-1],
                    "rule": "last_occurrence_wins",
                }
            )

    policies = tuple(winners[key][1] for key in sorted(winners))
    return DuplicateResolution(
        policies=policies,
        report={
            "duplicate_count": len(details),
            "resolution_rule": "last_occurrence_wins",
            "duplicates": details,
            "record_count_before": len(records),
            "record_count_after": len(policies),
        },
    )


def _validate_policy_shape(policy: Policy) -> None:
    if not policy.queue_id or len(policy.queue_id.encode("utf-8")) > 255:
        raise BusinessError(
            E_POLICY_INVALID,
            "queue_id must contain between 1 and 255 UTF-8 bytes",
            {"queue_id": policy.queue_id},
        )
    if policy.parent_queue_id is not None and (
        not policy.parent_queue_id
        or len(policy.parent_queue_id.encode("utf-8")) > 255
    ):
        raise BusinessError(
            E_POLICY_INVALID,
            "parent_queue_id must contain between 1 and 255 UTF-8 bytes",
            {"queue_id": policy.queue_id, "parent_queue_id": policy.parent_queue_id},
        )
    if len(policy.match_rule.encode("utf-8")) > 4096:
        raise BusinessError(
            E_POLICY_INVALID,
            "match_rule exceeds 4096 UTF-8 bytes",
            {"queue_id": policy.queue_id},
        )
    if not 0 <= policy.priority <= 7:
        raise BusinessError(
            E_POLICY_INVALID,
            "priority must be in the inclusive range 0..7",
            {"queue_id": policy.queue_id, "priority": policy.priority},
        )
    if policy.cir_bps < 0 or policy.pir_bps < 0:
        raise BusinessError(
            E_POLICY_INVALID,
            "rates must be non-negative",
            {"queue_id": policy.queue_id, "cir_bps": policy.cir_bps, "pir_bps": policy.pir_bps},
        )
    if policy.cir_bps > policy.pir_bps:
        raise BusinessError(
            E_POLICY_INVALID,
            "PIR must be greater than or equal to CIR",
            {"queue_id": policy.queue_id, "cir_bps": policy.cir_bps, "pir_bps": policy.pir_bps},
        )
    if policy.cbs_bytes <= 0 or policy.pbs_bytes <= 0:
        raise BusinessError(
            E_POLICY_INVALID,
            "burst sizes must be positive",
            {"queue_id": policy.queue_id, "cbs_bytes": policy.cbs_bytes, "pbs_bytes": policy.pbs_bytes},
        )
    if policy.cbs_bytes > policy.pbs_bytes:
        raise BusinessError(
            E_POLICY_INVALID,
            "PBS must be greater than or equal to CBS",
            {"queue_id": policy.queue_id, "cbs_bytes": policy.cbs_bytes, "pbs_bytes": policy.pbs_bytes},
        )
    if policy.timestamp_us < 0:
        raise BusinessError(
            E_POLICY_INVALID,
            "policy timestamp must not be negative",
            {"queue_id": policy.queue_id, "timestamp_us": policy.timestamp_us},
        )


def _find_cycle(by_queue: dict[str, Policy]) -> list[str] | None:
    state: dict[str, int] = {}
    path: list[str] = []

    def visit(queue_id: str) -> list[str] | None:
        state[queue_id] = 1
        path.append(queue_id)
        parent = by_queue[queue_id].parent_queue_id
        if parent is not None:
            if state.get(parent, 0) == 1:
                start = path.index(parent)
                return path[start:] + [parent]
            if state.get(parent, 0) == 0:
                found = visit(parent)
                if found is not None:
                    return found
        path.pop()
        state[queue_id] = 2
        return None

    for queue_id in sorted(by_queue):
        if state.get(queue_id, 0) == 0:
            found = visit(queue_id)
            if found is not None:
                return found
    return None


def validate_policies(policies: Iterable[Policy]) -> tuple[Policy, ...]:
    ordered = tuple(sorted(policies, key=lambda policy: policy.queue_id))
    by_queue = {policy.queue_id: policy for policy in ordered}
    if len(by_queue) != len(ordered):
        # Duplicates should have been resolved by the codec. This is a guard for
        # callers constructing snapshots directly through the HTTP API.
        raise BusinessError(
            E_POLICY_INVALID,
            "policy snapshot contains duplicate queue identifiers",
            {"duplicate_queue_ids": sorted({p.queue_id for p in ordered if ordered.count(p) > 1})},
        )

    for policy in ordered:
        _validate_policy_shape(policy)

    for policy in ordered:
        parent_id = policy.parent_queue_id
        if parent_id is None:
            continue
        parent = by_queue.get(parent_id)
        if parent is None:
            raise BusinessError(
                E_PARENT_NOT_FOUND,
                "a policy references a parent shaper that is not present in the snapshot",
                {"queue_id": policy.queue_id, "parent_queue_id": parent_id},
            )
        for metric in ("cir", "pir"):
            child_value = getattr(policy, f"{metric}_bps")
            parent_value = getattr(parent, f"{metric}_bps")
            if child_value > parent_value:
                raise BusinessError(
                    E_RATE_EXCEEDED,
                    f"child {metric.upper()} exceeds parent {metric.upper()}",
                    {
                        "queue_id": policy.queue_id,
                        "parent_queue_id": parent_id,
                        "metric": f"{metric}_bps",
                        "child_value": child_value,
                        "parent_value": parent_value,
                    },
                )

    cycle = _find_cycle(by_queue)
    if cycle is not None:
        raise BusinessError(
            E_DEPENDENCY_CYCLE,
            "policy parent dependencies contain a cycle",
            {"cycle": cycle, "cycle_queue_ids": sorted(set(cycle))},
        )

    return ordered


def validate_snapshot(snapshot: PolicySnapshot) -> PolicySnapshot:
    if snapshot.format_version <= 0:
        raise BusinessError(
            E_POLICY_INVALID,
            "snapshot format_version must be positive",
            {"format_version": snapshot.format_version},
        )
    if snapshot.format_version > CURRENT_FORMAT_VERSION:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "snapshot format_version is newer than the compatibility matrix",
            {
                "detected_version": snapshot.format_version,
                "current_version": CURRENT_FORMAT_VERSION,
                "decision": "reject",
                "partial_import": False,
            },
            http_status=422,
        )
    if snapshot.snapshot_no < 1:
        raise BusinessError(
            E_POLICY_INVALID,
            "snapshot_no must be a positive integer",
            {"snapshot_no": snapshot.snapshot_no},
        )
    if snapshot.generated_at_us < 0:
        raise BusinessError(
            E_POLICY_INVALID,
            "generated_at_us must not be negative",
            {"generated_at_us": snapshot.generated_at_us},
        )
    if not snapshot.policies:
        raise BusinessError(
            E_POLICY_INVALID,
            "policy snapshot must contain at least one policy",
            {"policy_count": 0},
        )
    policies = validate_policies(snapshot.policies)
    return PolicySnapshot(
        format_version=snapshot.format_version,
        snapshot_no=snapshot.snapshot_no,
        generated_at_us=snapshot.generated_at_us,
        policies=policies,
    )

