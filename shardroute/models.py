from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RangePart:
    lower: Any
    upper: Any
    lower_inclusive: bool
    upper_inclusive: bool
    shard: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "lower": self.lower,
            "upper": self.upper,
            "lower_inclusive": self.lower_inclusive,
            "upper_inclusive": self.upper_inclusive,
            "shard": self.shard,
        }


@dataclass(frozen=True)
class ShardAlgorithm:
    kind: str
    modulus: int | None = None
    seed: int = 0
    ranges: tuple[RangePart, ...] = ()
    declared_name: str | None = None

    def as_cache_dict(self) -> dict[str, Any]:
        if self.kind in {"mod", "hash"}:
            return {"type": self.kind, "modulus": self.modulus, "seed": self.seed}
        return {"type": "range", "ranges": [part.as_dict() for part in self.ranges]}

    @property
    def shard_ids(self) -> tuple[int, ...]:
        if self.kind == "range":
            return tuple(sorted({part.shard for part in self.ranges}))
        assert self.modulus is not None
        return tuple(range(self.modulus))


@dataclass(frozen=True)
class PhysicalTable:
    logical_db: str
    table: str
    shard: int
    primary_key: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "logical_db": self.logical_db,
            "table": self.table,
            "shard": self.shard,
            "primary_key": self.primary_key,
        }

    @property
    def order_key(self) -> tuple[int, str, str]:
        return (self.shard, self.logical_db, self.table)


@dataclass(frozen=True)
class TableSpec:
    logical_table: str
    logical_db: str
    physical_tables: tuple[PhysicalTable, ...]
    shard_key: str
    shard_key_type: str
    algorithm: ShardAlgorithm
    conflict_rule: str

    def physical_by_shard(self) -> dict[int, PhysicalTable]:
        return {physical.shard: physical for physical in self.physical_tables}


@dataclass(frozen=True)
class Snapshot:
    version: int
    checksum: str
    generated_at: str
    shard_key: str
    shard_key_type: str
    default_algorithm: ShardAlgorithm
    conflict_rule: str
    tables: dict[str, TableSpec]
    document: dict[str, Any]


@dataclass(frozen=True)
class TableCache:
    logical_table: str
    effective_version: int
    source_version: int
    rebuilt: bool
    physical_tables: tuple[PhysicalTable, ...]
    boundaries: tuple[RangePart, ...]
    algorithm_signature: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "logical_table": self.logical_table,
            "cache_version": self.effective_version,
            "source_version": self.source_version,
            "rebuilt": self.rebuilt,
            "physical_tables": [item.as_dict() for item in self.physical_tables],
            "boundaries": [item.as_dict() for item in self.boundaries],
            "algorithm_signature": self.algorithm_signature,
        }


@dataclass(frozen=True)
class RouteState:
    version: int
    snapshot: Snapshot
    caches: dict[str, TableCache]
    committed_at: str
    affected_tables: tuple[str, ...]

