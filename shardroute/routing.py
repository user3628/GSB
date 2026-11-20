from __future__ import annotations

import hashlib
from typing import Any

from .errors import error
from .models import PhysicalTable, RangePart, RouteState, ShardAlgorithm, TableSpec
from .snapshot import range_contains, range_is_empty, ranges_overlap
from .util import (
    canonical_json,
    normalize_shard_value,
    sha256_text,
    stable_value_key,
)


QUERY_TYPES = {"eq", "range", "in", "scan"}


def _query_context(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_id": request.get("request_id"),
        "logical_table": request.get("logical_table"),
        "query_type": request.get("query_type"),
        "routing_version": request.get("routing_version"),
    }


def _validate_query_type(query_type: Any, request: dict[str, Any]) -> str:
    if query_type not in QUERY_TYPES:
        raise error(
            "E_QUERY_NOT_SUPPORTED",
            "query_type must be one of eq, range, in, scan",
            supported=sorted(QUERY_TYPES),
            **_query_context(request),
        )
    return query_type


def _hash_shard(value: Any, shard_key_type: str, algorithm: ShardAlgorithm) -> int:
    assert algorithm.modulus is not None
    payload = f"{algorithm.seed}\x00{shard_key_type}\x00{value}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    number = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return number % algorithm.modulus


def _mod_shard(value: int, algorithm: ShardAlgorithm) -> int:
    assert algorithm.modulus is not None
    return value % algorithm.modulus


def _point_shard(value: Any, table: TableSpec, request: dict[str, Any]) -> int:
    algorithm = table.algorithm
    if algorithm.kind == "mod":
        if not isinstance(value, int):
            raise error(
                "E_SHARD_KEY_INVALID",
                "mod algorithm requires an integer shard key",
                reason="algorithm_type_mismatch",
                **_query_context(request),
            )
        return _mod_shard(value, algorithm)
    if algorithm.kind == "hash":
        return _hash_shard(value, table.shard_key_type, algorithm)
    matches = [part.shard for part in algorithm.ranges if range_contains(part, value)]
    if not matches:
        raise error(
            "E_SHARD_KEY_INVALID",
            "shard key is outside all configured ranges",
            reason="outside_range",
            value=value,
            **_query_context(request),
        )
    return matches[0]


def _normalized_value(value: Any, table: TableSpec, request: dict[str, Any]) -> Any:
    try:
        return normalize_shard_value(value, table.shard_key_type)
    except Exception as exc:
        if hasattr(exc, "context") and isinstance(exc.context, dict):
            exc.context = {**exc.context, **_query_context(request)}
        raise


def _query_range(request: dict[str, Any], table: TableSpec) -> RangePart:
    raw_range = request.get("shard_key_range")
    if not isinstance(raw_range, dict):
        raise error(
            "E_SHARD_KEY_INVALID",
            "range query requires shard_key_range object",
            reason="missing_range",
            **_query_context(request),
        )
    lower = raw_range.get("lower")
    upper = raw_range.get("upper")
    lower = None if lower is None else _normalized_value(lower, table, request)
    upper = None if upper is None else _normalized_value(upper, table, request)
    lower_inclusive = raw_range.get("lower_inclusive", True)
    upper_inclusive = raw_range.get("upper_inclusive", False)
    if not isinstance(lower_inclusive, bool) or not isinstance(upper_inclusive, bool):
        raise error(
            "E_SHARD_KEY_INVALID",
            "range inclusivity flags must be booleans",
            reason="invalid_range_inclusivity",
            **_query_context(request),
        )
    return RangePart(
        lower=lower,
        upper=upper,
        lower_inclusive=lower_inclusive,
        upper_inclusive=upper_inclusive,
        shard=-1,
    )


def _targets_for_range(
    request: dict[str, Any], table: TableSpec
) -> tuple[list[PhysicalTable], dict[str, Any]]:
    if table.algorithm.kind != "range":
        raise error(
            "E_QUERY_NOT_SUPPORTED",
            "range queries are supported only by range shard algorithms",
            algorithm=table.algorithm.kind,
            **_query_context(request),
        )
    query_range = _query_range(request, table)
    if range_is_empty(query_range):
        return [], {"range_empty": True, "query_range": query_range.as_dict()}
    shards = sorted(
        {
            part.shard
            for part in table.algorithm.ranges
            if ranges_overlap(part, query_range)
        }
    )
    by_shard = table.physical_by_shard()
    return [by_shard[shard] for shard in shards], {
        "matched_shards": shards,
        "query_range": query_range.as_dict(),
    }


def build_routing_plan(state: RouteState, request: dict[str, Any]) -> dict[str, Any]:
    request_id = request.get("request_id")
    logical_table = request.get("logical_table")
    if not isinstance(logical_table, str) or not logical_table:
        raise error("E_BAD_REQUEST", "logical_table is required", **_query_context(request))
    table = state.snapshot.tables.get(logical_table)
    if table is None:
        raise error(
            "E_NOT_FOUND",
            "logical table is not present in the bound routing snapshot",
            available_tables=sorted(state.snapshot.tables),
            **_query_context(request),
        )
    query_type = _validate_query_type(request.get("query_type"), request)
    cache = state.caches[logical_table]
    trace: list[dict[str, Any]] = [
        {
            "step": "bind_snapshot",
            "routing_version": state.version,
            "snapshot_checksum": state.snapshot.checksum,
        },
        {
            "step": "cache_lookup",
            "logical_table": logical_table,
            "cache_version": cache.effective_version,
            "cache_source_version": cache.source_version,
            "cache_rebuilt": cache.rebuilt,
        },
    ]
    details: dict[str, Any] = {}
    targets: list[PhysicalTable]
    if query_type == "scan":
        targets = list(table.physical_tables)
        trace.append({"step": "full_scan", "matched_shards": [item.shard for item in targets]})
    elif query_type == "eq":
        if "shard_key_value" not in request:
            raise error(
                "E_SHARD_KEY_INVALID",
                "eq query requires shard_key_value",
                reason="missing_shard_key_value",
                **_query_context(request),
            )
        value = _normalized_value(request["shard_key_value"], table, request)
        shard = _point_shard(value, table, request)
        target = table.physical_by_shard()[shard]
        targets = [target]
        details = {"normalized_shard_key": value, "matched_shards": [shard]}
        trace.append(
            {
                "step": "point_route",
                "algorithm": table.algorithm.kind,
                "value": value,
                "shard": shard,
                "physical_table": target.table,
            }
        )
    elif query_type == "in":
        raw_values = request.get("shard_key_values")
        if not isinstance(raw_values, list):
            raise error(
                "E_SHARD_KEY_INVALID",
                "in query requires shard_key_values array",
                reason="missing_shard_key_values",
                **_query_context(request),
            )
        normalized_values = [
            _normalized_value(value, table, request) for value in raw_values
        ]
        shards = sorted({_point_shard(value, table, request) for value in normalized_values})
        by_shard = table.physical_by_shard()
        targets = [by_shard[shard] for shard in shards]
        details = {"normalized_shard_keys": normalized_values, "matched_shards": shards}
        trace.append(
            {
                "step": "in_route",
                "algorithm": table.algorithm.kind,
                "values": normalized_values,
                "matched_shards": shards,
            }
        )
    else:
        targets, range_details = _targets_for_range(request, table)
        details = range_details
        trace.append({"step": "range_route", "algorithm": "range", **range_details})

    targets = sorted(dict.fromkeys(targets), key=lambda item: item.order_key)
    trace.append(
        {
            "step": "targets_ordered",
            "physical_tables": [item.as_dict() for item in targets],
        }
    )
    physical_descriptors = [
        {
            **target.as_dict(),
            "order": index,
        }
        for index, target in enumerate(targets)
    ]
    plan_body = {
        "request_id": request_id,
        "routing_version": state.version,
        "logical_table": logical_table,
        "query_type": query_type,
        "physical_tables": physical_descriptors,
        "matched_shards": details.get("matched_shards", []),
        "query_predicate": details,
        "cache": cache.as_dict(),
        "trace": trace,
    }
    plan_body["plan_checksum"] = sha256_text(canonical_json(plan_body))
    return plan_body


def routing_sort_key(row: dict[str, Any], shard_key: str, primary_key: str) -> tuple:
    return (
        stable_value_key(row.get(shard_key)),
        stable_value_key(row.get(primary_key)),
    )

