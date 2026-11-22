"""资源、快照、增量片段的领域模型。

模型层只表达结构与确定性序列化，不做引用完整性等跨资源校验
（那是 :mod:`graph_bench.validation` 的职责）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .canonical import snapshot_digest


@dataclass(frozen=True)
class Resource:
    """一个服务资源的声明式规格。

    - ``replicas``：副本预算，非负整数；
    - ``parents``：父资源标识（父先于子编排），按字典序排序存储；
    - ``weight_milli``：千分比整数权重，合法区间 [1, 1000]；
    - ``desired_version``：期望版本，按不透明字符串处理。
    """

    id: str
    replicas: int
    parents: tuple[str, ...]
    weight_milli: int
    desired_version: str

    @classmethod
    def from_dict(cls, raw: Any) -> "Resource":
        if not isinstance(raw, dict):
            raise TypeError("resource 必须是 JSON 对象")
        resource_id = raw.get("id")
        replicas = raw.get("replicas")
        parents = raw.get("parents", [])
        weight = raw.get("weight_milli")
        desired = raw.get("desired_version")
        if not isinstance(resource_id, str) or not resource_id:
            raise TypeError("resource.id 必须是非空字符串")
        if not isinstance(replicas, int) or isinstance(replicas, bool) or replicas < 0:
            raise TypeError(f"resource {resource_id!r} 的 replicas 必须是非负整数")
        if not isinstance(parents, list) or not all(isinstance(p, str) and p for p in parents):
            raise TypeError(f"resource {resource_id!r} 的 parents 必须是字符串数组")
        if not isinstance(weight, int) or isinstance(weight, bool):
            raise TypeError(f"resource {resource_id!r} 的 weight_milli 必须是整数")
        if not isinstance(desired, str) or not desired:
            raise TypeError(f"resource {resource_id!r} 的 desired_version 必须是非空字符串")
        return cls(
            id=resource_id,
            replicas=replicas,
            parents=tuple(sorted(parents)),
            weight_milli=weight,
            desired_version=desired,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "replicas": self.replicas,
            "parents": list(self.parents),
            "weight_milli": self.weight_milli,
            "desired_version": self.desired_version,
        }


@dataclass(frozen=True)
class Snapshot:
    """整份编排快照。"""

    version: int
    generated_at: str
    resources: tuple[Resource, ...]
    checksum: str
    supplied_checksum: str | None = field(default=None)

    @property
    def resource_map(self) -> dict[str, Resource]:
        return {item.id: item for item in self.resources}

    @classmethod
    def from_dict(cls, raw: Any) -> "Snapshot":
        if not isinstance(raw, dict):
            raise TypeError("快照必须是 JSON 对象")
        version = raw.get("version")
        generated_at = raw.get("generated_at")
        raw_resources = raw.get("resources")
        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            raise TypeError("snapshot.version 必须是非负整数")
        if not isinstance(generated_at, str) or not generated_at:
            raise TypeError("snapshot.generated_at 必须是非空字符串")
        if not isinstance(raw_resources, list):
            raise TypeError("snapshot.resources 必须是数组")
        resources = tuple(Resource.from_dict(item) for item in raw_resources)
        checksum = snapshot_digest(
            version,
            generated_at,
            [item.to_dict() for item in resources],
        )
        supplied = raw.get("checksum")
        if supplied is not None and not isinstance(supplied, str):
            raise TypeError("snapshot.checksum 必须是字符串")
        return cls(
            version=version,
            generated_at=generated_at,
            resources=resources,
            checksum=checksum,
            supplied_checksum=supplied,
        )


@dataclass(frozen=True)
class Increment:
    """局部增量片段。

    - ``base_version``：片段所基于的已提交版本；
    - ``upserts``：需要新增或整体替换的资源；
    - ``removals``：需要删除的资源标识。
    """

    base_version: int
    upserts: tuple[Resource, ...]
    removals: tuple[str, ...]
    generated_at: str | None = None

    @classmethod
    def from_dict(cls, raw: Any) -> "Increment":
        if not isinstance(raw, dict):
            raise TypeError("增量片段必须是 JSON 对象")
        base = raw.get("base_version")
        if not isinstance(base, int) or isinstance(base, bool) or base < 0:
            raise TypeError("increment.base_version 必须是非负整数")
        raw_upserts = raw.get("upserts", [])
        if not isinstance(raw_upserts, list):
            raise TypeError("increment.upserts 必须是数组")
        upserts = tuple(Resource.from_dict(item) for item in raw_upserts)
        raw_removals = raw.get("removals", [])
        if not isinstance(raw_removals, list) or not all(isinstance(r, str) and r for r in raw_removals):
            raise TypeError("increment.removals 必须是字符串数组")
        generated_at = raw.get("generated_at")
        if generated_at is not None and not isinstance(generated_at, str):
            raise TypeError("increment.generated_at 必须是字符串")
        return cls(
            base_version=base,
            upserts=upserts,
            removals=tuple(sorted(raw_removals)),
            generated_at=generated_at,
        )

    def touched_ids(self) -> set[str]:
        ids = {item.id for item in self.upserts}
        ids.update(self.removals)
        return ids
