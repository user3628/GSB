from __future__ import annotations

import hashlib

from .domain import Layout, ObjectHealth, ObjectLayout, ReplicaHealth, ShardHealth


REASON_ORDER = ("NODE_UNAVAILABLE", "DISK_FAULT", "SHARD_MISSING", "DIGEST_MISMATCH")


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def evaluate_object_health(layout: Layout, obj: ObjectLayout) -> ObjectHealth:
    readable_bitmap = 0
    shard_health: list[ShardHealth] = []
    for shard in obj.shards:
        replica_health: list[ReplicaHealth] = []
        selected = None
        selected_content = None
        all_reasons: list[str] = []
        for replica in shard.replicas:
            reason = None
            actual_digest = None
            if not layout.node_health.get(replica.node_id, True):
                reason = "NODE_UNAVAILABLE"
            elif layout.disk_status.get((replica.node_id, replica.disk_id), "online") != "online":
                reason = "DISK_FAULT"
            elif replica.present is False or replica.content is None:
                reason = "SHARD_MISSING"
            else:
                actual_digest = _digest(replica.content)
                if actual_digest != shard.digest:
                    reason = "DIGEST_MISMATCH"
            if reason:
                all_reasons.append(reason)
            else:
                selected = replica
                selected_content = replica.content
            replica_health.append(
                ReplicaHealth(
                    replica_id=replica.replica_id,
                    node_id=replica.node_id,
                    disk_id=replica.disk_id,
                    readable=reason is None,
                    reason_code=reason,
                    digest=actual_digest,
                )
            )
        ordered_reasons = tuple(code for code in REASON_ORDER if code in set(all_reasons))
        health = ShardHealth(
            shard_index=shard.index,
            role=shard.role,
            readable=selected is not None,
            selected_replica_id=selected.replica_id if selected else None,
            reason_codes=ordered_reasons,
            content=selected_content,
            replicas=tuple(replica_health),
        )
        if health.readable:
            readable_bitmap |= 1 << shard.index
        shard_health.append(health)
    return ObjectHealth(object_id=obj.object_id, shards=tuple(shard_health), readable_bitmap=readable_bitmap)


def bitmap_to_hex(bitmap: int) -> str:
    return f"0x{bitmap:x}"
