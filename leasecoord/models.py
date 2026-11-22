"""Domain records, state machines, and small validation helpers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable


class NodeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    SUSPECT = "SUSPECT"
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"


class LeaseState(str, Enum):
    FREE = "FREE"
    GRANTED = "GRANTED"
    EXPIRED = "EXPIRED"


LEGAL_NODE_TRANSITIONS: dict[NodeStatus, frozenset[NodeStatus]] = {
    NodeStatus.ACTIVE: frozenset(
        {NodeStatus.SUSPECT, NodeStatus.FOLLOWER, NodeStatus.LEADER}
    ),
    NodeStatus.SUSPECT: frozenset({NodeStatus.ACTIVE}),
    NodeStatus.LEADER: frozenset({NodeStatus.FOLLOWER, NodeStatus.SUSPECT}),
    NodeStatus.FOLLOWER: frozenset(
        {NodeStatus.ACTIVE, NodeStatus.SUSPECT, NodeStatus.LEADER}
    ),
}

LEGAL_LEASE_TRANSITIONS: dict[LeaseState, frozenset[LeaseState]] = {
    LeaseState.FREE: frozenset({LeaseState.GRANTED}),
    LeaseState.GRANTED: frozenset(
        {LeaseState.GRANTED, LeaseState.FREE, LeaseState.EXPIRED}
    ),
    LeaseState.EXPIRED: frozenset({LeaseState.GRANTED}),
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def validate_node_id(node_id: str) -> str:
    if not isinstance(node_id, str) or not node_id:
        raise ValueError("node_id must be a non-empty string")
    if len(node_id) > 128:
        raise ValueError("node_id must be at most 128 characters")
    if any(not (char.isalnum() or char in "._:-") for char in node_id):
        raise ValueError("node_id contains unsupported characters")
    return node_id


@dataclass(frozen=True, slots=True)
class NodeRecord:
    id: str
    priority: int
    lease_ttl_ms: int
    last_heartbeat_ms: int
    status: NodeStatus
    leader_lease_version: int
    registered_version: int
    updated_at_ms: int

    @classmethod
    def from_row(cls, row: Any) -> "NodeRecord":
        return cls(
            id=row["id"],
            priority=int(row["priority"]),
            lease_ttl_ms=int(row["lease_ttl_ms"]),
            last_heartbeat_ms=int(row["last_heartbeat_ms"]),
            status=NodeStatus(row["status"]),
            leader_lease_version=int(row["leader_lease_version"]),
            registered_version=int(row["registered_version"]),
            updated_at_ms=int(row["updated_at_ms"]),
        )

    def heartbeat_age_ms(self, now_ms: int) -> int:
        return max(0, now_ms - self.last_heartbeat_ms)

    def heartbeat_fresh(self, now_ms: int, timeout_ms: int) -> bool:
        return self.heartbeat_age_ms(now_ms) <= timeout_ms

    def effective_status(
        self,
        now_ms: int,
        timeout_ms: int,
        lease_holder_id: str | None,
        lease_granted: bool,
    ) -> NodeStatus:
        fresh = self.heartbeat_fresh(now_ms, timeout_ms)
        if not fresh:
            return NodeStatus.SUSPECT
        if lease_granted and lease_holder_id == self.id:
            return NodeStatus.LEADER
        if self.status is NodeStatus.LEADER:
            return NodeStatus.FOLLOWER
        return self.status


@dataclass(frozen=True, slots=True)
class LeaseRecord:
    holder_id: str | None
    version: int
    expires_at_ms: int
    term: int
    state: LeaseState
    updated_at_ms: int

    @classmethod
    def from_row(cls, row: Any) -> "LeaseRecord":
        return cls(
            holder_id=row["holder_id"],
            version=int(row["version"]),
            expires_at_ms=int(row["expires_at_ms"]),
            term=int(row["term"]),
            state=LeaseState(row["state"]),
            updated_at_ms=int(row["updated_at_ms"]),
        )


@dataclass(frozen=True, slots=True)
class EffectiveLease:
    record: LeaseRecord
    effective_state: LeaseState
    valid: bool
    expired_by_ttl: bool
    holder_heartbeat_fresh: bool
    holder_effective_status: NodeStatus | None
    invalidation_reason: str | None


def node_status_transition_allowed(
    current: NodeStatus,
    target: NodeStatus,
) -> bool:
    if current is target:
        return True
    return target in LEGAL_NODE_TRANSITIONS[current]


def lease_transition_allowed(current: LeaseState, target: LeaseState) -> bool:
    return target in LEGAL_LEASE_TRANSITIONS[current]


def candidate_snapshot_digest(nodes: Iterable[NodeRecord]) -> str:
    snapshot = [
        {
            "id": node.id,
            "priority": node.priority,
            "lease_ttl_ms": node.lease_ttl_ms,
            "last_heartbeat_ms": node.last_heartbeat_ms,
            "status": node.status.value,
        }
        for node in sorted(nodes, key=lambda item: item.id)
    ]
    return digest_json(snapshot)


def candidate_set_summary(
    nodes: Iterable[NodeRecord],
    eligible: Iterable[NodeRecord],
) -> dict[str, Any]:
    all_nodes = list(nodes)
    eligible_nodes = list(eligible)
    return {
        "candidate_count": len(all_nodes),
        "eligible_count": len(eligible_nodes),
        "candidate_set_digest": candidate_snapshot_digest(all_nodes),
        "eligible_set_digest": candidate_snapshot_digest(eligible_nodes),
        "top_priorities": sorted(
            {node.priority for node in eligible_nodes}, reverse=True
        )[:5],
    }
