from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Replica:
    replica_id: str
    node_id: str
    disk_id: str
    present: bool | None
    content: bytes | None


@dataclass(frozen=True)
class ShardLayout:
    index: int
    role: str
    size_bytes: int
    digest: str
    replicas: tuple[Replica, ...]


@dataclass(frozen=True)
class ObjectLayout:
    object_id: str
    k: int
    m: int
    shard_size_bytes: int
    shards: tuple[ShardLayout, ...]

    @property
    def total_shards(self) -> int:
        return self.k + self.m

    def shard(self, index: int) -> ShardLayout:
        return self.shards[index]


@dataclass(frozen=True)
class EngineConfig:
    rebuild_concurrency: int = 2
    rebuild_queue_capacity: int = 4
    round_byte_budget: int = 2048
    round_time_budget_us: int = 20_000
    read_base_latency_us: int = 1200
    read_per_shard_latency_us: int = 80
    replica_read_latency_us: int = 350
    rebuild_setup_us: int = 700
    rebuild_per_shard_latency_us: int = 250
    rebuild_bandwidth_bytes_per_us: int = 2
    merge_per_shard_latency_us: int = 40

    def to_dict(self) -> dict[str, int]:
        return {
            "rebuild_concurrency": self.rebuild_concurrency,
            "rebuild_queue_capacity": self.rebuild_queue_capacity,
            "round_byte_budget": self.round_byte_budget,
            "round_time_budget_us": self.round_time_budget_us,
            "read_base_latency_us": self.read_base_latency_us,
            "read_per_shard_latency_us": self.read_per_shard_latency_us,
            "replica_read_latency_us": self.replica_read_latency_us,
            "rebuild_setup_us": self.rebuild_setup_us,
            "rebuild_per_shard_latency_us": self.rebuild_per_shard_latency_us,
            "rebuild_bandwidth_bytes_per_us": self.rebuild_bandwidth_bytes_per_us,
            "merge_per_shard_latency_us": self.merge_per_shard_latency_us,
        }


@dataclass(frozen=True)
class Layout:
    layout_version: str
    max_k_plus_m: int
    matrix_id: str
    objects: dict[str, ObjectLayout]
    node_health: dict[str, bool]
    disk_status: dict[tuple[str, str], str]
    engine: EngineConfig
    raw_document: dict[str, Any] = field(repr=False)
    document_hash: str = ""


@dataclass(frozen=True)
class TraceRequest:
    seq: int
    request_id: str
    arrival_ms: int
    object_id: str
    shard_bitmap: int
    requested_shards: tuple[int, ...]
    priority: int
    tolerable_latency_us: int
    content_hash: str = ""

    def canonical(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "arrival_ms": self.arrival_ms,
            "object_id": self.object_id,
            "shard_bitmap": f"0x{self.shard_bitmap:x}",
            "priority": self.priority,
            "tolerable_latency_us": self.tolerable_latency_us,
        }


@dataclass(frozen=True)
class Trace:
    trace_id: str
    layout_version: str
    requests: tuple[TraceRequest, ...]
    options: EngineConfig
    trace_hash: str


@dataclass(frozen=True)
class ReplicaHealth:
    replica_id: str
    node_id: str
    disk_id: str
    readable: bool
    reason_code: str | None
    digest: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "replica_id": self.replica_id,
            "node_id": self.node_id,
            "disk_id": self.disk_id,
            "readable": self.readable,
            "reason_code": self.reason_code,
            "digest": self.digest,
        }


@dataclass(frozen=True)
class ShardHealth:
    shard_index: int
    role: str
    readable: bool
    selected_replica_id: str | None
    reason_codes: tuple[str, ...]
    content: bytes | None = field(repr=False)
    replicas: tuple[ReplicaHealth, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_index": self.shard_index,
            "role": self.role,
            "readable": self.readable,
            "selected_replica_id": self.selected_replica_id,
            "reason_codes": list(self.reason_codes),
            "replicas": [replica.to_dict() for replica in self.replicas],
        }


@dataclass(frozen=True)
class ObjectHealth:
    object_id: str
    shards: tuple[ShardHealth, ...]
    readable_bitmap: int

    @property
    def readable_count(self) -> int:
        return sum(1 for shard in self.shards if shard.readable)

    def shard(self, index: int) -> ShardHealth:
        return self.shards[index]
