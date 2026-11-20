from __future__ import annotations

import copy

import pytest

from shardroute.errors import BusinessError

from conftest import finalized


def expect_code(code: str, function, *args, **kwargs) -> BusinessError:
    with pytest.raises(BusinessError) as caught:
        function(*args, **kwargs)
    assert caught.value.code == code, caught.value.to_error()
    return caught.value


def test_checksum_version_rollback_and_unchanged_state(service, snapshot_document):
    active_before = service.current_state()
    tampered = copy.deepcopy(snapshot_document)
    tampered["generated_at"] = "2026-11-21T01:00:00Z"
    expect_code("E_SNAPSHOT_INVALID", service.load_snapshot, tampered)
    assert service.current_state().version == active_before.version
    assert service.current_state().snapshot.checksum == active_before.snapshot.checksum

    rollback = copy.deepcopy(snapshot_document)
    rollback["generated_at"] = "2026-11-21T01:00:00Z"
    rollback = finalized(rollback)
    expect_code("E_VERSION_ROLLBACK", service.load_snapshot, rollback)
    assert service.current_state().version == 1
    assert service.current_state().snapshot.checksum == active_before.snapshot.checksum


def test_full_replacement_uses_only_new_mappings(service, snapshot_document):
    replacement = copy.deepcopy(snapshot_document)
    replacement["snapshot_version"] = 2
    replacement["generated_at"] = "2026-11-21T02:00:00Z"
    replacement["tables"]["orders"]["algorithm"] = {
        "type": "hash",
        "modulus": 4,
        "seed": 20261121,
    }
    replacement = finalized(replacement)
    result = service.load_snapshot(replacement)
    assert result["active_version"] == 2
    assert service.current_state().version == 2
    assert all(cache.effective_version == 2 for cache in service.current_state().caches.values())
    expect_code(
        "E_VERSION_RETIRED",
        service.execute_query,
        {
            "request_id": "retired-v1",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
            "routing_version": 1,
        },
    )
    current = service.execute_query(
        {
            "request_id": "full-v2",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
            "routing_version": 2,
        }
    )
    assert [(item["table"], item["shard"]) for item in current["physical_tables"]] == [
        ("orders_0002", 2)
    ]
    assert [row["id"] for row in current["rows"]] == [1002]


def test_modulo_in_and_scan_are_deterministically_ordered(service):
    first = service.execute_query(
        {
            "request_id": "mod-a",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
        }
    )
    second = service.execute_query(
        {
            "request_id": "mod-b",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
        }
    )
    assert first["physical_tables"] == second["physical_tables"]
    assert [row["id"] for row in first["rows"]] == [row["id"] for row in second["rows"]]
    assert [(item["table"], item["shard"]) for item in first["physical_tables"]] == [
        ("orders_0003", 3)
    ]

    in_result = service.execute_query(
        {
            "request_id": "in-all-shards",
            "logical_table": "orders",
            "query_type": "in",
            "shard_key_values": [3, 1, 2, 0],
        }
    )
    assert [(item["shard"], item["table"]) for item in in_result["physical_tables"]] == [
        (0, "orders_0000"),
        (1, "orders_0001"),
        (2, "orders_0002"),
        (3, "orders_0003"),
    ]
    scan = service.execute_query(
        {
            "request_id": "scan-all",
            "logical_table": "orders",
            "query_type": "scan",
        }
    )
    assert scan["physical_tables"] == in_result["physical_tables"]


def test_hash_uses_fixed_seed(service):
    expected = {
        "tenant-0": 0,
        "tenant-2": 1,
        "tenant-6": 2,
        "tenant-5": 3,
    }
    for index, (tenant, shard) in enumerate(expected.items()):
        result = service.execute_query(
            {
                "request_id": f"hash-{index}",
                "logical_table": "events",
                "query_type": "eq",
                "shard_key_value": tenant,
            }
        )
        assert result["matched_shards"] == [shard]
        assert result["physical_tables"][0]["table"] == f"events_{shard:04d}"


def test_range_boundaries_and_empty_interval(service):
    cases = [
        (10, 0, "catalog_0000"),
        (99, 0, "catalog_0000"),
        (100, 1, "catalog_0001"),
        (199, 1, "catalog_0001"),
        (200, 2, "catalog_0002"),
        (1000, 3, "catalog_0003"),
    ]
    for index, (value, shard, table) in enumerate(cases):
        result = service.execute_query(
            {
                "request_id": f"range-boundary-{index}",
                "logical_table": "catalog",
                "query_type": "eq",
                "shard_key_value": value,
            }
        )
        assert result["matched_shards"] == [shard]
        assert result["physical_tables"][0]["table"] == table

    empty = service.execute_query(
        {
            "request_id": "range-empty",
            "logical_table": "catalog",
            "query_type": "range",
            "shard_key_range": {"lower": 100, "upper": 100},
        }
    )
    assert empty["physical_tables"] == []
    assert empty["rows"] == []

    crossing = service.execute_query(
        {
            "request_id": "range-crossing",
            "logical_table": "catalog",
            "query_type": "range",
            "shard_key_range": {"lower": 99, "upper": 101},
        }
    )
    assert [item["shard"] for item in crossing["physical_tables"]] == [0, 1]

    tail = service.execute_query(
        {
            "request_id": "range-tail",
            "logical_table": "catalog",
            "query_type": "range",
            "shard_key_range": {"lower": 1000, "upper": None},
        }
    )
    assert [item["shard"] for item in tail["physical_tables"]] == [3]
    assert [row["id"] for row in tail["rows"]] == [4003]


def test_invalid_shard_keys_are_rejected(service):
    expect_code(
        "E_SHARD_KEY_INVALID",
        service.execute_query,
        {
            "request_id": "bad-int",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": "not-an-integer",
        },
    )
    expect_code(
        "E_SHARD_KEY_INVALID",
        service.execute_query,
        {
            "request_id": "bad-string",
            "logical_table": "events",
            "query_type": "eq",
            "shard_key_value": 123,
        },
    )
    expect_code(
        "E_SHARD_KEY_INVALID",
        service.execute_query,
        {
            "request_id": "bad-in-member",
            "logical_table": "orders",
            "query_type": "in",
            "shard_key_values": [7, "bad"],
        },
    )


def test_table_and_algorithm_cycles_are_distinct_errors(service, snapshot_document):
    table_cycle = copy.deepcopy(snapshot_document)
    table_cycle["tables"]["alpha"] = {"route_ref": "beta"}
    table_cycle["tables"]["beta"] = {"route_ref": "alpha"}
    table_cycle = finalized(table_cycle)
    table_error = expect_code("E_ROUTE_CYCLE", service.load_snapshot, table_cycle)
    assert table_error.context["reference_kind"] == "logical_table"
    assert service.current_state().version == 1

    algorithm_cycle = copy.deepcopy(snapshot_document)
    algorithm_cycle["algorithms"]["cycle_a"] = {"type": "ref", "target": "cycle_b"}
    algorithm_cycle["algorithms"]["cycle_b"] = {"type": "ref", "target": "cycle_a"}
    algorithm_cycle = finalized(algorithm_cycle)
    algorithm_error = expect_code("E_ROUTE_CYCLE", service.load_snapshot, algorithm_cycle)
    assert algorithm_error.context["reference_kind"] == "algorithm"
    assert service.current_state().version == 1


def test_duplicate_mapping_overlap_and_empty_mapping_conflicts(service, snapshot_document):
    duplicate = copy.deepcopy(snapshot_document)
    duplicate["tables"]["orders"]["physical_tables"][1]["table"] = "orders_0000"
    duplicate = finalized(duplicate)
    duplicate_error = expect_code("E_ROUTE_CONFLICT", service.load_snapshot, duplicate)
    assert duplicate_error.context["conflict_kind"] == "duplicate_physical_table"

    overlap = copy.deepcopy(snapshot_document)
    overlap["algorithms"]["catalog_ranges"]["ranges"][1]["lower"] = 50
    overlap = finalized(overlap)
    overlap_error = expect_code("E_ROUTE_CONFLICT", service.load_snapshot, overlap)
    assert overlap_error.context["conflict_kind"] == "range_overlap"

    empty_mapping = copy.deepcopy(snapshot_document)
    empty_mapping["tables"]["orders"]["physical_tables"] = []
    empty_mapping = finalized(empty_mapping)
    expect_code("E_EMPTY_PHYSICAL_MAPPING", service.load_snapshot, empty_mapping)
    assert service.current_state().version == 1

