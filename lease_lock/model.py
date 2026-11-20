from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Iterable, Mapping


class DependencyMode(str, Enum):
    STRONG = "strong"
    WEAK = "weak"


class LeaseState(str, Enum):
    ACTIVE = "active"
    RELEASED = "released"


RELEASE_LEASE_EXPIRED = "LEASE_EXPIRED"
RELEASE_EXPLICIT = "EXPLICIT_RELEASE"
RELEASE_DEPENDENCY_EXPIRED = "DEPENDENCY_EXPIRED"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def checksum_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class Dependency:
    parent_key: str
    mode: DependencyMode

    def to_dict(self) -> dict[str, str]:
        return {"lock_key": self.parent_key, "mode": self.mode.value}


@dataclass(frozen=True, slots=True)
class ChildLink:
    child_key: str
    mode: DependencyMode

    def to_dict(self) -> dict[str, str]:
        return {"lock_key": self.child_key, "mode": self.mode.value}


@dataclass(frozen=True, slots=True)
class Lease:
    lock_key: str
    holder: str
    lease_version: int
    acquired_at_ms: int
    ttl_ms: int
    expires_at_ms: int
    state: LeaseState = LeaseState.ACTIVE
    released_at_ms: int | None = None
    release_reason: str | None = None
    dependency_expired: bool = False
    dependencies: tuple[Dependency, ...] = ()

    @property
    def active(self) -> bool:
        return self.state == LeaseState.ACTIVE

    def to_dict(self) -> dict[str, Any]:
        return {
            "lock_key": self.lock_key,
            "holder": self.holder,
            "lease_version": self.lease_version,
            "acquired_at_ms": self.acquired_at_ms,
            "ttl_ms": self.ttl_ms,
            "expires_at_ms": self.expires_at_ms,
            "state": self.state.value,
            "released_at_ms": self.released_at_ms,
            "release_reason": self.release_reason,
            "dependency_expired": self.dependency_expired,
            "dependencies": [d.to_dict() for d in self.dependencies],
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Lease":
        return cls(
            lock_key=str(raw["lock_key"]),
            holder=str(raw["holder"]),
            lease_version=int(raw["lease_version"]),
            acquired_at_ms=int(raw["acquired_at_ms"]),
            ttl_ms=int(raw["ttl_ms"]),
            expires_at_ms=int(raw["expires_at_ms"]),
            state=LeaseState(str(raw.get("state", "active"))),
            released_at_ms=(
                int(raw["released_at_ms"]) if raw.get("released_at_ms") is not None else None
            ),
            release_reason=raw.get("release_reason"),
            dependency_expired=bool(raw.get("dependency_expired", False)),
            dependencies=normalize_dependencies(raw.get("dependencies", [])),
        )


def normalize_dependencies(raw_dependencies: Iterable[Any] | None) -> tuple[Dependency, ...]:
    if raw_dependencies is None:
        return ()
    result: dict[str, DependencyMode] = {}
    for raw in raw_dependencies:
        if isinstance(raw, str):
            key, mode_text = raw, "strong"
        elif isinstance(raw, Mapping):
            key = str(raw.get("lock_key") or raw.get("key") or raw.get("parent_key") or "")
            mode_text = str(raw.get("mode", "strong"))
        else:
            raise ValueError("dependency must be a string or an object")
        if not key:
            raise ValueError("dependency lock_key must not be empty")
        try:
            mode = DependencyMode(mode_text.lower())
        except ValueError as exc:
            raise ValueError(f"invalid dependency mode: {mode_text}") from exc
        if key in result and result[key] != mode:
            raise ValueError(f"conflicting modes for dependency {key}")
        result[key] = mode
    return tuple(Dependency(key, result[key]) for key in sorted(result))


@dataclass(frozen=True, slots=True)
class Snapshot:
    version: int
    parent_version: int
    kind: str
    created_at_ms: int
    leases: Mapping[str, Lease]
    parents: Mapping[str, tuple[Dependency, ...]]
    children: Mapping[str, tuple[ChildLink, ...]]
    ancestors: Mapping[str, tuple[str, ...]]
    derived_version: Mapping[str, int]
    affected: tuple[str, ...]

    @classmethod
    def empty(cls, version: int = 0) -> "Snapshot":
        return cls(
            version=version,
            parent_version=0,
            kind="bootstrap",
            created_at_ms=0,
            leases={},
            parents={},
            children={},
            ancestors={},
            derived_version={},
            affected=(),
        )

    @classmethod
    def build(
        cls,
        *,
        version: int,
        parent_version: int,
        kind: str,
        created_at_ms: int,
        leases: Mapping[str, Lease],
        affected: Iterable[str] | None = None,
    ) -> "Snapshot":
        normalized_leases = {
            key: replace(lease, dependencies=tuple(sorted(lease.dependencies, key=lambda d: d.parent_key)))
            for key, lease in leases.items()
        }
        parents = {key: lease.dependencies for key, lease in normalized_leases.items()}
        children_mut: dict[str, list[ChildLink]] = {}
        for child_key, lease in normalized_leases.items():
            for dependency in lease.dependencies:
                children_mut.setdefault(dependency.parent_key, []).append(
                    ChildLink(child_key, dependency.mode)
                )
        children = {
            key: tuple(sorted(links, key=lambda link: link.child_key))
            for key, links in children_mut.items()
        }
        ancestors = {
            key: _ancestor_closure(key, parents) for key in normalized_leases
        }
        affected_tuple = tuple(
            sorted(set(normalized_leases) if affected is None else affected)
        )
        derived_version = {key: version for key in normalized_leases}
        return cls(
            version=version,
            parent_version=parent_version,
            kind=kind,
            created_at_ms=created_at_ms,
            leases=normalized_leases,
            parents=parents,
            children=children,
            ancestors=ancestors,
            derived_version=derived_version,
            affected=affected_tuple,
        )

    def evolve(
        self,
        *,
        version: int,
        patches: Mapping[str, Lease],
        kind: str,
        created_at_ms: int,
        seed_keys: Iterable[str] | None = None,
    ) -> "Snapshot":
        """Apply only the supplied entries and rebuild only their dependency cone."""

        seeds = set(patches if seed_keys is None else seed_keys)
        old_descendants = self.descendant_keys(seeds)
        leases = dict(self.leases)
        parents = dict(self.parents)
        children = dict(self.children)
        touched_parents: set[str] = set()

        for key, lease in patches.items():
            old_lease = leases.get(key)
            old_deps = old_lease.dependencies if old_lease is not None else ()
            new_deps = tuple(sorted(lease.dependencies, key=lambda d: d.parent_key))
            leases[key] = replace(lease, dependencies=new_deps)
            if old_deps != new_deps or old_lease is None:
                parents[key] = new_deps
                touched_parents.update(d.parent_key for d in old_deps)
                touched_parents.update(d.parent_key for d in new_deps)

        for parent_key in touched_parents:
            links = list(children.get(parent_key, ()))
            links = [link for link in links if link.child_key not in patches]
            for key in patches:
                lease = leases[key]
                for dependency in lease.dependencies:
                    if dependency.parent_key == parent_key:
                        links.append(ChildLink(key, dependency.mode))
            if links:
                children[parent_key] = tuple(sorted(links, key=lambda link: link.child_key))
            else:
                children.pop(parent_key, None)

        new_descendants = _descendant_keys_from(children, seeds)
        affected = set(seeds) | old_descendants | new_descendants

        ancestors = dict(self.ancestors)
        for key in affected:
            ancestors[key] = _ancestor_closure(key, parents)

        derived_version = dict(self.derived_version)
        for key in affected:
            derived_version[key] = version

        return Snapshot(
            version=version,
            parent_version=self.version,
            kind=kind,
            created_at_ms=created_at_ms,
            leases=leases,
            parents=parents,
            children=children,
            ancestors=ancestors,
            derived_version=derived_version,
            affected=tuple(sorted(affected)),
        )

    def replace_leases(self, updates: Mapping[str, Lease]) -> "Snapshot":
        if not updates:
            return self
        leases = dict(self.leases)
        leases.update(updates)
        return replace(self, leases=leases)

    def descendant_keys(self, seeds: Iterable[str]) -> set[str]:
        return _descendant_keys_from(self.children, set(seeds))

    def direct_children(self, key: str) -> tuple[ChildLink, ...]:
        return self.children.get(key, ())

    def topological_affected(self) -> list[str]:
        affected = set(self.affected)
        indegree: dict[str, int] = {key: 0 for key in affected}
        outgoing: dict[str, list[str]] = {key: [] for key in affected}
        for key in affected:
            for dependency in self.parents.get(key, ()):
                if dependency.parent_key in affected:
                    indegree[key] += 1
                    outgoing.setdefault(dependency.parent_key, []).append(key)
        ready = sorted(key for key, degree in indegree.items() if degree == 0)
        order: list[str] = []
        while ready:
            key = ready.pop(0)
            order.append(key)
            for child in sorted(outgoing.get(key, ())):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
                    ready.sort()
        if len(order) != len(affected):
            raise ValueError("dependency cycle detected while ordering snapshot")
        return order

    def to_payload(self) -> dict[str, Any]:
        body = self.state_payload()
        body["affected"] = list(self.affected)
        body["checksum"] = checksum_json(self.state_payload())
        return body

    def state_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "parent_version": self.parent_version,
            "kind": self.kind,
            "created_at_ms": self.created_at_ms,
            "leases": [
                self.leases[key].to_dict() for key in sorted(self.leases)
            ],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any], *, force_version: int | None = None) -> "Snapshot":
        version = int(force_version if force_version is not None else payload.get("version", 0))
        lease_map = {
            str(raw["lock_key"]): Lease.from_dict(raw)
            for raw in payload.get("leases", [])
        }
        return cls.build(
            version=version,
            parent_version=int(payload.get("parent_version", 0)),
            kind=str(payload.get("kind", "checkpoint")),
            created_at_ms=int(payload.get("created_at_ms", 0)),
            leases=lease_map,
            affected=payload.get("affected", list(lease_map)),
        )


def _ancestor_closure(key: str, parents: Mapping[str, tuple[Dependency, ...]]) -> tuple[str, ...]:
    found: set[str] = set()
    stack = [dependency.parent_key for dependency in parents.get(key, ())]
    while stack:
        current = stack.pop()
        if current in found:
            continue
        found.add(current)
        for dependency in parents.get(current, ()):
            stack.append(dependency.parent_key)
    return tuple(sorted(found))


def _descendant_keys_from(
    children: Mapping[str, tuple[ChildLink, ...]], seeds: set[str]
) -> set[str]:
    found: set[str] = set()
    stack = list(seeds)
    while stack:
        current = stack.pop()
        for link in children.get(current, ()):
            if link.child_key not in found:
                found.add(link.child_key)
                stack.append(link.child_key)
    return found


def find_dependency_cycle(leases: Mapping[str, Lease]) -> list[str] | None:
    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(key: str) -> list[str] | None:
        if key in visiting:
            try:
                start = stack.index(key)
            except ValueError:
                start = 0
            return stack[start:] + [key]
        if key in visited:
            return None
        visiting.add(key)
        stack.append(key)
        lease = leases.get(key)
        dependencies = lease.dependencies if lease is not None else ()
        for dependency in dependencies:
            cycle = visit(dependency.parent_key)
            if cycle:
                return cycle
        stack.pop()
        visiting.remove(key)
        visited.add(key)
        return None

    for key in sorted(leases):
        cycle = visit(key)
        if cycle:
            return cycle
    return None

