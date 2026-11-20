from __future__ import annotations

import sqlite3
from typing import Any

from .errors import ErrorCode, ServiceError
from .models import CheckpointState, canonical_json, utc_now


ALLOWED_TRANSITIONS: dict[CheckpointState, frozenset[CheckpointState]] = {
    CheckpointState.WRITING: frozenset({CheckpointState.COMMITTED, CheckpointState.INVALID}),
    CheckpointState.COMMITTED: frozenset({CheckpointState.INVALID, CheckpointState.PRUNED}),
    CheckpointState.INVALID: frozenset(),
    CheckpointState.PRUNED: frozenset(),
}


def validate_transition(current: CheckpointState, target: CheckpointState) -> None:
    if target not in ALLOWED_TRANSITIONS[current]:
        raise ServiceError(
            ErrorCode.INVALID_TRANSITION,
            f"illegal checkpoint transition {current.value} -> {target.value}",
            {
                "from_state": current.value,
                "to_state": target.value,
                "allowed": sorted(item.value for item in ALLOWED_TRANSITIONS[current]),
            },
        )


def apply_transition(
    conn: sqlite3.Connection,
    checkpoint_id: str,
    target: CheckpointState,
    reason: str,
    details: dict[str, Any] | None = None,
) -> CheckpointState:
    """Persist one validated transition inside the caller's transaction."""

    row = conn.execute(
        "SELECT state, request_id FROM checkpoints WHERE checkpoint_id = ?",
        (checkpoint_id,),
    ).fetchone()
    if row is None:
        raise ServiceError(
            ErrorCode.CHECKPOINT_NOT_FOUND,
            "checkpoint was not found",
            {"checkpoint_id": checkpoint_id},
        )
    current = CheckpointState(row["state"])
    validate_transition(current, target)
    now = utc_now()
    timestamp_column = {
        CheckpointState.COMMITTED: "committed_at",
        CheckpointState.INVALID: "invalidated_at",
        CheckpointState.PRUNED: "pruned_at",
    }.get(target)
    if timestamp_column is None:
        conn.execute(
            "UPDATE checkpoints SET state = ?, state_reason = ?, updated_at = ? WHERE checkpoint_id = ?",
            (target.value, reason, now, checkpoint_id),
        )
    else:
        conn.execute(
            f"UPDATE checkpoints SET state = ?, state_reason = ?, updated_at = ?, {timestamp_column} = ? WHERE checkpoint_id = ?",
            (target.value, reason, now, now, checkpoint_id),
        )
    conn.execute(
        "INSERT INTO transitions(checkpoint_id, request_id, from_state, to_state, reason, occurred_at, details_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            checkpoint_id,
            row["request_id"],
            current.value,
            target.value,
            reason,
            now,
            canonical_json(details or {}),
        ),
    )
    return target

