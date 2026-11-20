from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

from .errors import BusinessError, error
from .models import PhysicalTable, RangePart, ShardAlgorithm, Snapshot, TableSpec
from .util import (
    SAFE_IDENTIFIER,
    normalize_integer,
    read_json,
    require_plain_int,
    snapshot_checksum,
    validate_identifier,
)


SUPPORTED_SCHEMA_VERSION = 1
SUPPORTED_SHARD_KEY_TYPES = {"integer", "string"}
SUPPORTED_CONFLICT_RULES = {
    "physical_order_first",
    "physical_order_last",
    "max_updated_at_physical_order",
}
CHECKSUM_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _snapshot_invalid(message: str, **context: Any) -> BusinessError:
    return error("E_SNAPSHOT_INVALID", message, **context)


def _plain_bool(value: Any, field_name: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise _snapshot_invalid(f"{field_name} must be a boolean", field=field_name, value=value)
    return value


def _validate_bound(value: Any, value_type: str, field_name: str) -> Any:
    if value is None:
        return None
    if value_type == "integer":
        normalized = normalize_integer(value)
        if normalized is None:
            raise _snapshot_invalid(
                f"{field_name} must be an integer or null",
                field=field_name,
                value=value,
            )
        return normalized
    if value_type == "string":
        if not isinstance(value, str):
            raise _snapshot_invalid(
                f"{field_name} must be a string or null",
                field=field_name,
                value=value,
            )
        return value
    raise _snapshot_invalid("unsupported shard key type", shard_key_type=value_type)


def _compare_values(left: Any, right: Any) -> int:
    if left == right:
        return 0
    return -1 if left < right else 1


def range_is_empty(part: RangePart) -> bool:
    if part.lower is None or part.upper is None:
        return False
    compared = _compare_values(part.lower, part.upper)
    if compared > 0:
        return True
    if compared == 0 and not (part.lower_inclusive and part.upper_inclusive):
        return True
    return False


def range_contains(part: RangePart, value: Any) -> bool:
    if part.lower is not None:
        compared = _compare_values(value, part.lower)
        if compared < 0 or (compared == 0 and not part.lower_inclusive):
            return False
    if part.upper is not None:
        compared = _compare_values(value, part.upper)
        if compared > 0 or (compared == 0 and not part.upper_inclusive):
            return False
    return True


def ranges_overlap(left: RangePart, right: RangePart) -> bool:
    if range_is_empty(left) or range_is_empty(right):
        return False
    if left.upper is not None and right.lower is not None:
        compared = _compare_values(left.upper, right.lower)
        if compared < 0:
            return False
        if compared == 0 and not (left.upper_inclusive and right.lower_inclusive):
            return False
    if right.upper is not None and left.lower is not None:
        compared = _compare_values(right.upper, left.lower)
        if compared < 0:
            return False
        if compared == 0 and not (right.upper_inclusive and left.lower_inclusive):
            return False
    return True


class _AlgorithmResolver:
    def __init__(self, raw_algorithms: dict[str, Any], shard_key_type: str):
        self.raw_algorithms = raw_algorithms
        self.shard_key_type = shard_key_type
        self.cache: dict[str, ShardAlgorithm] = {}

    def resolve_named(self, name: str, stack: tuple[str, ...] = ()) -> ShardAlgorithm:
        if not isinstance(name, str) or not name:
            raise _snapshot_invalid("algorithm reference must be a non-empty string", value=name)
        if name in self.cache:
            return self.cache[name]
        if name in stack:
            path = list(stack[stack.index(name) :]) + [name]
            raise error(
                "E_ROUTE_CYCLE",
                "algorithm reference cycle detected",
                cycle=path,
                reference_kind="algorithm",
            )
        if name not in self.raw_algorithms:
            raise _snapshot_invalid("unknown algorithm reference", reference=name)
        resolved = self.resolve_definition(
            self.raw_algorithms[name], stack + (name,), declared_name=name
        )
        self.cache[name] = resolved
        return resolved

    def resolve_definition(
        self,
        raw: Any,
        stack: tuple[str, ...] = (),
        declared_name: str | None = None,
    ) -> ShardAlgorithm:
        if isinstance(raw, str):
            return self.resolve_named(raw, stack)
        if not isinstance(raw, dict):
            raise _snapshot_invalid("algorithm must be an object or named reference")
        kind = raw.get("type")
        if kind == "ref":
            target = raw.get("target")
            return self.resolve_named(target, stack)
        return self._parse_direct(raw, declared_name=declared_name)

    def _parse_direct(self, raw: dict[str, Any], declared_name: str | None) -> ShardAlgorithm:
        kind = raw.get("type")
        if kind in {"mod", "hash"}:
            modulus = require_plain_int(raw.get("modulus"), "algorithm.modulus")
            if modulus <= 0:
                raise _snapshot_invalid(
                    "algorithm.modulus must be positive", modulus=modulus, algorithm=kind
                )
            seed = raw.get("seed", 0)
            seed = require_plain_int(seed, "algorithm.seed")
            return ShardAlgorithm(
                kind=kind, modulus=modulus, seed=seed, declared_name=declared_name
            )
        if kind == "range":
            raw_ranges = raw.get("ranges")
            if not isinstance(raw_ranges, list) or not raw_ranges:
                raise _snapshot_invalid("range algorithm requires a non-empty ranges list")
            parts: list[RangePart] = []
            for index, item in enumerate(raw_ranges):
                if not isinstance(item, dict):
                    raise _snapshot_invalid(
                        "range entry must be an object", range_index=index
                    )
                shard = require_plain_int(item.get("shard"), f"ranges[{index}].shard")
                if shard < 0:
                    raise _snapshot_invalid(
                        "range shard must be non-negative", range_index=index, shard=shard
                    )
                lower = _validate_bound(
                    item.get("lower"), self.shard_key_type, f"ranges[{index}].lower"
                )
                upper = _validate_bound(
                    item.get("upper"), self.shard_key_type, f"ranges[{index}].upper"
                )
                lower_inclusive = _plain_bool(
                    item.get("lower_inclusive"), f"ranges[{index}].lower_inclusive", True
                )
                upper_inclusive = _plain_bool(
                    item.get("upper_inclusive"), f"ranges[{index}].upper_inclusive", False
                )
                part = RangePart(
                    lower=lower,
                    upper=upper,
                    lower_inclusive=lower_inclusive,
                    upper_inclusive=upper_inclusive,
                    shard=shard,
                )
                if range_is_empty(part):
                    raise _snapshot_invalid(
                        "range entry is empty", range_index=index, range=part.as_dict()
                    )
                parts.append(part)
            for left_index, left in enumerate(parts):
                for right_index in range(left_index + 1, len(parts)):
                    right = parts[right_index]
                    if ranges_overlap(left, right):
                        raise error(
                            "E_ROUTE_CONFLICT",
                            "shard ranges overlap for the same shard key",
                            conflict_kind="range_overlap",
                            left_range=left.as_dict(),
                            right_range=right.as_dict(),
                            left_index=left_index,
                            right_index=right_index,
                        )
            return ShardAlgorithm(
                kind="range", ranges=tuple(parts), declared_name=declared_name
            )
        raise _snapshot_invalid("unsupported shard algorithm", algorithm=raw)


def _resolve_tables(
    raw_tables: dict[str, Any], algorithm_resolver: _AlgorithmResolver, snapshot_values: dict[str, Any]
) -> dict[str, TableSpec]:
    resolving: list[str] = []
    resolved_raw: dict[str, dict[str, Any]] = {}

    def resolve_raw(name: str) -> dict[str, Any]:
        if name in resolved_raw:
            return resolved_raw[name]
        if name in resolving:
            path = resolving[resolving.index(name) :] + [name]
            raise error(
                "E_ROUTE_CYCLE",
                "logical table mapping reference cycle detected",
                cycle=path,
                reference_kind="logical_table",
            )
        raw = raw_tables.get(name)
        if not isinstance(raw, dict):
            raise _snapshot_invalid("logical table definition must be an object", table=name)
        resolving.append(name)
        try:
            route_ref = raw.get("route_ref")
            if route_ref is None:
                merged = copy.deepcopy(raw)
            else:
                if not isinstance(route_ref, str) or not route_ref:
                    raise _snapshot_invalid(
                        "route_ref must be a non-empty string", table=name, route_ref=route_ref
                    )
                if route_ref not in raw_tables:
                    raise _snapshot_invalid(
                        "unknown route_ref target", table=name, route_ref=route_ref
                    )
                inherited = resolve_raw(route_ref)
                merged = copy.deepcopy(inherited)
                for key in (
                    "logical_db",
                    "physical_tables",
                    "shard_key",
                    "shard_key_type",
                    "algorithm",
                    "conflict_rule",
                ):
                    if key in raw:
                        merged[key] = copy.deepcopy(raw[key])
            resolved_raw[name] = merged
            return merged
        finally:
            resolving.pop()

    tables: dict[str, TableSpec] = {}
    for logical_table in raw_tables:
        if not isinstance(logical_table, str) or not SAFE_IDENTIFIER.fullmatch(logical_table):
            raise _snapshot_invalid(
                "logical table name is not a safe identifier", logical_table=logical_table
            )
        raw = resolve_raw(logical_table)
        logical_db = validate_identifier(raw.get("logical_db"), "logical_db")

        raw_physical = raw.get("physical_tables")
        if not isinstance(raw_physical, list) or not raw_physical:
            raise error(
                "E_EMPTY_PHYSICAL_MAPPING",
                "logical table must map to at least one physical table",
                logical_table=logical_table,
            )
        physical_tables: list[PhysicalTable] = []
        seen_physical: set[tuple[str, str]] = set()
        seen_shards: set[int] = set()
        for index, raw_physical_table in enumerate(raw_physical):
            if not isinstance(raw_physical_table, dict):
                raise _snapshot_invalid(
                    "physical table mapping must be an object",
                    logical_table=logical_table,
                    mapping_index=index,
                )
            database = validate_identifier(
                raw_physical_table.get("logical_db", logical_db),
                f"tables.{logical_table}.physical_tables[{index}].logical_db",
            )
            table_name = validate_identifier(
                raw_physical_table.get("table"),
                f"tables.{logical_table}.physical_tables[{index}].table",
            )
            primary_key = validate_identifier(
                raw_physical_table.get("primary_key", "id"),
                f"tables.{logical_table}.physical_tables[{index}].primary_key",
            )
            shard = require_plain_int(
                raw_physical_table.get("shard"),
                f"tables.{logical_table}.physical_tables[{index}].shard",
            )
            if shard < 0:
                raise _snapshot_invalid(
                    "physical shard must be non-negative", logical_table=logical_table, shard=shard
                )
            physical_key = (database, table_name)
            if physical_key in seen_physical:
                raise error(
                    "E_ROUTE_CONFLICT",
                    "logical table maps to the same physical table more than once",
                    conflict_kind="duplicate_physical_table",
                    logical_table=logical_table,
                    logical_db=database,
                    physical_table=table_name,
                )
            if shard in seen_shards:
                raise error(
                    "E_ROUTE_CONFLICT",
                    "logical table maps more than one physical table to the same shard",
                    conflict_kind="duplicate_shard_mapping",
                    logical_table=logical_table,
                    shard=shard,
                )
            seen_physical.add(physical_key)
            seen_shards.add(shard)
            physical_tables.append(
                PhysicalTable(
                    logical_db=database,
                    table=table_name,
                    shard=shard,
                    primary_key=primary_key,
                )
            )
        physical_tables.sort(key=lambda item: item.order_key)

        shard_key = raw.get("shard_key", snapshot_values["shard_key"])
        shard_key = validate_identifier(shard_key, f"tables.{logical_table}.shard_key")
        shard_key_type = raw.get("shard_key_type", snapshot_values["shard_key_type"])
        if shard_key_type not in SUPPORTED_SHARD_KEY_TYPES:
            raise _snapshot_invalid(
                "unsupported table shard key type",
                logical_table=logical_table,
                shard_key_type=shard_key_type,
            )
        algorithm_raw = raw.get("algorithm", snapshot_values["algorithm"])
        algorithm = algorithm_resolver.resolve_definition(algorithm_raw)
        conflict_rule = raw.get("conflict_rule", snapshot_values["conflict_rule"])
        if conflict_rule not in SUPPORTED_CONFLICT_RULES:
            raise _snapshot_invalid(
                "unsupported conflict rule",
                logical_table=logical_table,
                conflict_rule=conflict_rule,
            )

        expected_shards = set(algorithm.shard_ids)
        actual_shards = {item.shard for item in physical_tables}
        if expected_shards != actual_shards:
            raise error(
                "E_ROUTE_CONFLICT",
                "physical table shard set does not match the shard algorithm",
                conflict_kind="shard_mapping_mismatch",
                logical_table=logical_table,
                expected_shards=sorted(expected_shards),
                actual_shards=sorted(actual_shards),
                missing_shards=sorted(expected_shards - actual_shards),
                extra_shards=sorted(actual_shards - expected_shards),
            )

        tables[logical_table] = TableSpec(
            logical_table=logical_table,
            logical_db=logical_db,
            physical_tables=tuple(physical_tables),
            shard_key=shard_key,
            shard_key_type=shard_key_type,
            algorithm=algorithm,
            conflict_rule=conflict_rule,
        )
    if not tables:
        raise _snapshot_invalid("snapshot must define at least one logical table")
    return tables


def _parse_snapshot_document_raw(document: Any) -> Snapshot:
    if not isinstance(document, dict):
        raise _snapshot_invalid("snapshot must be a JSON object")
    checksum = document.get("checksum")
    if not isinstance(checksum, str) or not CHECKSUM_PATTERN.fullmatch(checksum):
        raise _snapshot_invalid("snapshot checksum is missing or malformed")
    computed_checksum = snapshot_checksum(document)
    if checksum != computed_checksum:
        raise _snapshot_invalid(
            "snapshot checksum mismatch", expected=computed_checksum, actual=checksum
        )
    if document.get("schema_version") != SUPPORTED_SCHEMA_VERSION:
        raise _snapshot_invalid(
            "unsupported snapshot schema version", schema_version=document.get("schema_version")
        )
    version = require_plain_int(document.get("snapshot_version"), "snapshot_version")
    if version <= 0:
        raise _snapshot_invalid("snapshot_version must be positive", snapshot_version=version)
    generated_at = document.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at:
        raise _snapshot_invalid("generated_at must be a non-empty string")
    shard_key = validate_identifier(document.get("shard_key"), "shard_key")
    shard_key_type = document.get("shard_key_type", "integer")
    if shard_key_type not in SUPPORTED_SHARD_KEY_TYPES:
        raise _snapshot_invalid("unsupported shard key type", shard_key_type=shard_key_type)
    conflict_rule = document.get("conflict_rule", "physical_order_first")
    if conflict_rule not in SUPPORTED_CONFLICT_RULES:
        raise _snapshot_invalid("unsupported conflict rule", conflict_rule=conflict_rule)
    raw_algorithms = document.get("algorithms", {})
    if not isinstance(raw_algorithms, dict):
        raise _snapshot_invalid("algorithms must be an object")
    for name, value in raw_algorithms.items():
        if not isinstance(name, str) or not name:
            raise _snapshot_invalid("algorithm name must be a non-empty string", name=name)
        if not isinstance(value, (dict, str)):
            raise _snapshot_invalid("algorithm definition must be an object or reference", name=name)

    resolver = _AlgorithmResolver(raw_algorithms, shard_key_type)
    for algorithm_name in raw_algorithms:
        resolver.resolve_named(algorithm_name)
    if "algorithm" not in document:
        raise _snapshot_invalid("snapshot.algorithm is required")
    default_algorithm = resolver.resolve_definition(document["algorithm"])
    raw_tables = document.get("tables")
    if not isinstance(raw_tables, dict) or not raw_tables:
        raise _snapshot_invalid("tables must be a non-empty object")
    tables = _resolve_tables(
        raw_tables,
        resolver,
        {
            "shard_key": shard_key,
            "shard_key_type": shard_key_type,
            "algorithm": default_algorithm,
            "conflict_rule": conflict_rule,
        },
    )
    return Snapshot(
        version=version,
        checksum=checksum,
        generated_at=generated_at,
        shard_key=shard_key,
        shard_key_type=shard_key_type,
        default_algorithm=default_algorithm,
        conflict_rule=conflict_rule,
        tables=tables,
        document=copy.deepcopy(document),
    )


def load_snapshot_document(document: Any) -> Snapshot:
    try:
        return _parse_snapshot_document_raw(document)
    except BusinessError as exc:
        if exc.code in {
            "E_SNAPSHOT_INVALID",
            "E_ROUTE_CYCLE",
            "E_ROUTE_CONFLICT",
            "E_EMPTY_PHYSICAL_MAPPING",
        }:
            raise
        raise _snapshot_invalid(
            "snapshot validation failed",
            cause_code=exc.code,
            cause_message=exc.message,
            cause_context=exc.context,
        ) from exc
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise _snapshot_invalid(
            "snapshot structure could not be validated",
            cause=repr(exc),
        ) from exc


def load_snapshot_file(path: str | Path) -> Snapshot:
    return load_snapshot_document(read_json(path))


def finalize_snapshot_document(document: dict[str, Any]) -> dict[str, Any]:
    finalized = copy.deepcopy(document)
    finalized.pop("checksum", None)
    finalized["checksum"] = snapshot_checksum(finalized)
    return finalized


def validate_patch_checksum(patch: dict[str, Any]) -> None:
    checksum = patch.get("checksum")
    if checksum is None:
        return
    if not isinstance(checksum, str) or checksum != snapshot_checksum(patch):
        raise _snapshot_invalid(
            "patch checksum mismatch", expected=snapshot_checksum(patch), actual=checksum
        )

