from __future__ import annotations

from .models import RouteState, Snapshot, TableCache, TableSpec
from .util import canonical_json, sha256_text, utc_now


def build_table_cache(
    table: TableSpec,
    *,
    effective_version: int,
    source_version: int,
    rebuilt: bool,
) -> TableCache:
    algorithm_signature = sha256_text(canonical_json(table.algorithm.as_cache_dict()))
    return TableCache(
        logical_table=table.logical_table,
        effective_version=effective_version,
        source_version=source_version,
        rebuilt=rebuilt,
        physical_tables=table.physical_tables,
        boundaries=table.algorithm.ranges,
        algorithm_signature=algorithm_signature,
    )


def build_route_state(
    snapshot: Snapshot,
    *,
    affected_tables: tuple[str, ...] | None = None,
    previous: RouteState | None = None,
    committed_at: str | None = None,
) -> RouteState:
    affected = set(affected_tables if affected_tables is not None else tuple(snapshot.tables))
    caches: dict[str, TableCache] = {}
    for name, table in snapshot.tables.items():
        if previous is None or name in affected:
            source_version = snapshot.version
            rebuilt = True
        else:
            prior = previous.caches[name]
            source_version = prior.source_version
            rebuilt = False
        caches[name] = build_table_cache(
            table,
            effective_version=snapshot.version,
            source_version=source_version,
            rebuilt=rebuilt,
        )
    return RouteState(
        version=snapshot.version,
        snapshot=snapshot,
        caches=caches,
        committed_at=committed_at or utc_now(),
        affected_tables=tuple(sorted(affected)),
    )


def cache_manifest(route_state: RouteState) -> list[dict]:
    return [route_state.caches[name].as_dict() for name in sorted(route_state.caches)]

