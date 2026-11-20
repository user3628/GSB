"""Compile routes into an immutable state and report cascade invalidation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .models import RouteConfig, changed_fields, content_hash, snapshot_payload


ROUTE_ARTIFACT_KINDS = ("match", "pool_group", "health_plan", "forward_rules")
AGGREGATE_ARTIFACT_IDS = (
    "index:artifact-manifest",
    "index:reverse-match",
    "index:forward-match",
    "index:upstream-groups",
    "index:health-plans",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class Artifact:
    artifact_id: str
    kind: str
    route_id: str | None
    content_hash: str
    content: dict[str, Any]
    depends_on: tuple[str, ...]
    built_version: int

    def public(self, *, status: str = "fresh") -> dict[str, Any]:
        return {
            "id": self.artifact_id,
            "kind": self.kind,
            "route_id": self.route_id,
            "content_hash": self.content_hash,
            "depends_on": list(self.depends_on),
            "built_version": self.built_version,
            "status": status,
        }


@dataclass(frozen=True)
class CompiledRoute:
    route: RouteConfig
    fingerprint: str
    artifacts: dict[str, Artifact]

    def artifact(self, kind: str) -> Artifact:
        return self.artifacts[kind]


@dataclass(frozen=True)
class ReverseIndexEntry:
    route_id: str
    methods: tuple[str, ...]
    path_prefix: str
    fingerprint: str
    match_artifact_id: str
    pool_group_artifact_id: str

    def allows(self, method: str) -> bool:
        return "*" in self.methods or method in self.methods


@dataclass(frozen=True)
class ForwardIndexEntry:
    route_id: str
    methods: tuple[str, ...]
    path_prefix: str
    target_host: str
    target_port: int
    fingerprint: str
    match_artifact_id: str
    pool_group_artifact_id: str

    def allows(self, method: str) -> bool:
        return "*" in self.methods or method in self.methods


@dataclass(frozen=True)
class CompiledState:
    version: int
    config_hash: str
    generation: int
    compiled_at: str
    routes: dict[str, CompiledRoute]
    reverse_index: tuple[ReverseIndexEntry, ...]
    forward_index: tuple[ForwardIndexEntry, ...]
    artifacts: dict[str, Artifact]
    invalidation: dict[str, Any]

    @property
    def route_count(self) -> int:
        return len(self.routes)

    def route(self, route_id: str) -> CompiledRoute | None:
        return self.routes.get(route_id)

    def match_reverse(self, path: str, method: str) -> CompiledRoute | None:
        method = method.upper()
        for entry in self.reverse_index:
            if entry.allows(method) and path.startswith(entry.path_prefix):
                return self.routes[entry.route_id]
        return None

    def match_forward(self, host: str, port: int, path: str, method: str) -> CompiledRoute | None:
        method = method.upper()
        normalized_host = host.strip().strip("[]").lower()
        for entry in self.forward_index:
            if (
                entry.target_host == normalized_host
                and entry.target_port == port
                and entry.allows(method)
                and path.startswith(entry.path_prefix)
            ):
                return self.routes[entry.route_id]
        return None

    def assert_atomic(self) -> None:
        for route_id, compiled in self.routes.items():
            for kind in ROUTE_ARTIFACT_KINDS:
                artifact = compiled.artifacts.get(kind)
                if artifact is None or self.artifacts.get(artifact.artifact_id) is not artifact:
                    raise AssertionError(f"route {route_id} missing {kind} artifact in frozen state")
                if artifact.route_id != route_id:
                    raise AssertionError(f"route {route_id} artifact {kind} points at {artifact.route_id}")
        for entry in self.reverse_index:
            route = self.routes.get(entry.route_id)
            if route is None or route.fingerprint != entry.fingerprint:
                raise AssertionError(f"reverse index entry is stale for {entry.route_id}")
            if entry.match_artifact_id not in self.artifacts:
                raise AssertionError("reverse index references missing match artifact")
        for entry in self.forward_index:
            route = self.routes.get(entry.route_id)
            if route is None or route.fingerprint != entry.fingerprint:
                raise AssertionError(f"forward index entry is stale for {entry.route_id}")
            if entry.match_artifact_id not in self.artifacts:
                raise AssertionError("forward index references missing match artifact")
        for artifact_id in AGGREGATE_ARTIFACT_IDS:
            if artifact_id not in self.artifacts:
                raise AssertionError(f"missing aggregate artifact {artifact_id}")


def _make_artifact(
    *,
    artifact_id: str,
    kind: str,
    route_id: str | None,
    content: dict[str, Any],
    depends_on: tuple[str, ...],
    version: int,
    old_artifacts: dict[str, Artifact],
) -> Artifact:
    digest = content_hash(content)
    previous = old_artifacts.get(artifact_id)
    built_version = previous.built_version if previous and previous.content_hash == digest else version
    return Artifact(
        artifact_id=artifact_id,
        kind=kind,
        route_id=route_id,
        content_hash=digest,
        content=content,
        depends_on=depends_on,
        built_version=built_version,
    )


def _route_artifacts(route: RouteConfig, version: int, old_artifacts: dict[str, Artifact]) -> dict[str, Artifact]:
    fingerprint = route.fingerprint
    match_content = {
        "type": "matching-table-entry",
        "route_id": route.id,
        "route_kind": route.kind,
        "fingerprint": fingerprint,
        "match": route.match.public(),
    }
    pool_content = {
        "type": "connection-pool-group",
        "route_id": route.id,
        "group": route.failure_policy.group,
        "primary_upstream": route.upstream,
        "candidates": list(route.upstream_candidates),
        "connect_timeout": route.timeout.connect,
        "read_timeout": route.timeout.read,
        "max_connections": 32,
        "fingerprint": fingerprint,
    }
    health_content = {
        "type": "health-probe-plan",
        "route_id": route.id,
        "targets": list(route.upstream_candidates),
        "path": route.health.path,
        "interval": route.health.interval,
        "timeout": route.health.timeout,
        "fail_threshold": route.health.fail_threshold,
        "success_threshold": route.health.success_threshold,
        "enabled": route.health.enabled,
        "fingerprint": fingerprint,
    }
    rule_content = {
        "type": "forward-rule-cache",
        "route_id": route.id,
        "route_kind": route.kind,
        "match": route.match.public(),
        "upstream": route.upstream,
        "candidates": list(route.upstream_candidates),
        "headers": {name: value for name, value in route.headers},
        "retries": route.retries.public(),
        "failure_policy": route.failure_policy.public(),
        "in_flight_policy": route.in_flight_policy,
        "fingerprint": fingerprint,
    }
    definitions = {
        "match": (f"match:{route.id}", "matching_table", match_content),
        "pool_group": (f"pool:{route.failure_policy.group}:{route.id}", "connection_pool_group", pool_content),
        "health_plan": (f"health:{route.id}", "health_probe_plan", health_content),
        "forward_rules": (f"rules:{route.id}", "forward_rule_cache", rule_content),
    }
    result: dict[str, Artifact] = {}
    for kind, (artifact_id, artifact_kind, content) in definitions.items():
        result[kind] = _make_artifact(
            artifact_id=artifact_id,
            kind=artifact_kind,
            route_id=route.id,
            content=content,
            depends_on=(),
            version=version,
            old_artifacts=old_artifacts,
        )
    return result


def compile_state(
    *,
    version: int,
    routes: Iterable[RouteConfig],
    old_state: CompiledState | None,
    generation: int | None = None,
) -> CompiledState:
    route_list = tuple(sorted(routes, key=lambda route: route.id))
    old_artifacts = old_state.artifacts if old_state else {}
    old_routes = old_state.routes if old_state else {}
    compiled_routes: dict[str, CompiledRoute] = {}
    for route in route_list:
        artifacts = _route_artifacts(route, version, old_artifacts)
        compiled_routes[route.id] = CompiledRoute(
            route=route,
            fingerprint=route.fingerprint,
            artifacts=artifacts,
        )

    reverse_entries = tuple(
        sorted(
            (
                ReverseIndexEntry(
                    route_id=compiled.route.id,
                    methods=compiled.route.match.methods,
                    path_prefix=compiled.route.match.path_prefix,
                    fingerprint=compiled.fingerprint,
                    match_artifact_id=compiled.artifact("match").artifact_id,
                    pool_group_artifact_id=compiled.artifact("pool_group").artifact_id,
                )
                for compiled in compiled_routes.values()
                if compiled.route.kind == "reverse"
            ),
            key=lambda item: (-len(item.path_prefix), item.route_id),
        )
    )
    forward_entries = tuple(
        sorted(
            (
                ForwardIndexEntry(
                    route_id=compiled.route.id,
                    methods=compiled.route.match.methods,
                    path_prefix=compiled.route.match.path_prefix,
                    target_host=compiled.route.match.target_host or "",
                    target_port=compiled.route.match.target_port or 0,
                    fingerprint=compiled.fingerprint,
                    match_artifact_id=compiled.artifact("match").artifact_id,
                    pool_group_artifact_id=compiled.artifact("pool_group").artifact_id,
                )
                for compiled in compiled_routes.values()
                if compiled.route.kind == "forward"
            ),
            key=lambda item: (
                item.target_host,
                item.target_port,
                -len(item.path_prefix),
                item.route_id,
            ),
        )
    )

    all_artifacts: dict[str, Artifact] = {}
    for compiled in compiled_routes.values():
        for artifact in compiled.artifacts.values():
            all_artifacts[artifact.artifact_id] = artifact

    # Aggregates intentionally reference route fingerprints and route artifact hashes.
    # A changing route therefore forces each dependent aggregate through invalidation.
    aggregate_specs = {
        "index:artifact-manifest": (
            "artifact_manifest",
            {
                "type": "artifact-manifest",
                "route_fingerprints": {route_id: item.fingerprint for route_id, item in compiled_routes.items()},
                "artifacts": {
                    artifact_id: artifact.content_hash
                    for artifact_id, artifact in sorted(all_artifacts.items())
                },
            },
        ),
        "index:reverse-match": (
            "reverse_match_index",
            {
                "type": "reverse-match-index",
                "entries": [
                    {
                        "route_id": entry.route_id,
                        "methods": list(entry.methods),
                        "path_prefix": entry.path_prefix,
                        "fingerprint": entry.fingerprint,
                        "match_artifact": entry.match_artifact_id,
                    }
                    for entry in reverse_entries
                ],
            },
        ),
        "index:forward-match": (
            "forward_match_index",
            {
                "type": "forward-match-index",
                "entries": [
                    {
                        "route_id": entry.route_id,
                        "methods": list(entry.methods),
                        "path_prefix": entry.path_prefix,
                        "target_host": entry.target_host,
                        "target_port": entry.target_port,
                        "fingerprint": entry.fingerprint,
                        "match_artifact": entry.match_artifact_id,
                    }
                    for entry in forward_entries
                ],
            },
        ),
        "index:upstream-groups": (
            "upstream_pool_index",
            {
                "type": "upstream-pool-index",
                "groups": {
                    group: {
                        "routes": [
                            {
                                "route_id": compiled.route.id,
                                "primary": compiled.route.upstream,
                                "candidates": list(compiled.route.upstream_candidates),
                                "pool_artifact": compiled.artifact("pool_group").artifact_id,
                            }
                            for compiled in compiled_routes.values()
                            if compiled.route.failure_policy.group == group
                        ]
                    }
                    for group in sorted(
                        {compiled.route.failure_policy.group for compiled in compiled_routes.values()}
                    )
                },
            },
        ),
        "index:health-plans": (
            "health_plan_index",
            {
                "type": "health-plan-index",
                "routes": {
                    compiled.route.id: {
                        "plan_artifact": compiled.artifact("health_plan").artifact_id,
                        "targets": list(compiled.route.upstream_candidates),
                    }
                    for compiled in compiled_routes.values()
                },
            },
        ),
    }
    dependency_ids = tuple(compiled.artifact(kind).artifact_id for compiled in compiled_routes.values() for kind in ROUTE_ARTIFACT_KINDS)
    for artifact_id, (kind, content) in aggregate_specs.items():
        all_artifacts[artifact_id] = _make_artifact(
            artifact_id=artifact_id,
            kind=kind,
            route_id=None,
            content=content,
            depends_on=dependency_ids,
            version=version,
            old_artifacts=old_artifacts,
        )

    if old_state is None:
        changed_route_ids = [route.id for route in route_list]
        added = changed_route_ids
        removed: list[str] = []
        field_changes = {route.id: ["<initial>"] for route in route_list}
        invalidated: list[str] = []
        rebuilt = sorted(all_artifacts)
        reused: list[str] = []
    else:
        old_ids = set(old_routes)
        new_ids = set(compiled_routes)
        added = sorted(new_ids - old_ids)
        removed = sorted(old_ids - new_ids)
        changed_route_ids = sorted(
            route_id
            for route_id in old_ids & new_ids
            if old_routes[route_id].fingerprint != compiled_routes[route_id].fingerprint
        )
        field_changes = {
            route_id: changed_fields(
                old_routes[route_id].route if route_id in old_routes else None,
                compiled_routes[route_id].route if route_id in compiled_routes else None,
            )
            for route_id in sorted(set(old_ids) | set(new_ids))
            if route_id in added or route_id in removed or route_id in changed_route_ids
        }
        impacted = set(changed_route_ids) | set(added) | set(removed)
        invalidated = []
        for route_id in sorted(impacted):
            previous = old_routes.get(route_id)
            if previous:
                invalidated.extend(artifact.artifact_id for artifact in previous.artifacts.values())
        if impacted:
            invalidated.extend(AGGREGATE_ARTIFACT_IDS)
        rebuilt = []
        for route_id in sorted(impacted):
            current = compiled_routes.get(route_id)
            if current:
                rebuilt.extend(artifact.artifact_id for artifact in current.artifacts.values())
        if impacted:
            rebuilt.extend(AGGREGATE_ARTIFACT_IDS)
        rebuilt = sorted(set(rebuilt))
        reused = sorted(
            artifact_id
            for artifact_id, artifact in all_artifacts.items()
            if artifact_id not in set(rebuilt)
            and artifact_id in old_artifacts
            and old_artifacts[artifact_id].content_hash == artifact.content_hash
        )

    state_generation = generation if generation is not None else ((old_state.generation + 1) if old_state else 1)
    invalidation = {
        "from_version": old_state.version if old_state else None,
        "to_version": version,
        "generation": state_generation,
        "changed_route_ids": sorted(set(changed_route_ids) | set(added) | set(removed)),
        "added_route_ids": added,
        "removed_route_ids": removed,
        "changed_fields": field_changes,
        "invalidated_artifacts": sorted(set(invalidated)),
        "rebuilt_artifacts": sorted(set(rebuilt)),
        "reused_artifacts": reused,
        "atomic": True,
        "compiled_at": utc_now(),
    }
    state_payload = snapshot_payload(version, route_list)
    state = CompiledState(
        version=version,
        config_hash=content_hash(state_payload),
        generation=state_generation,
        compiled_at=invalidation["compiled_at"],
        routes=compiled_routes,
        reverse_index=reverse_entries,
        forward_index=forward_entries,
        artifacts=all_artifacts,
        invalidation=invalidation,
    )
    state.assert_atomic()
    return state


def state_summary(state: CompiledState) -> dict[str, Any]:
    return {
        "version": state.version,
        "config_hash": state.config_hash,
        "generation": state.generation,
        "route_count": state.route_count,
        "compiled_at": state.compiled_at,
        "artifact_count": len(state.artifacts),
        "invalidation": state.invalidation,
    }

