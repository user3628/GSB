from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from shardroute.engine import ShardRouteService
from shardroute.errors import BusinessError


def expect_code(code: str, function, *args, **kwargs) -> BusinessError:
    with pytest.raises(BusinessError) as caught:
        function(*args, **kwargs)
    assert caught.value.code == code, caught.value.to_error()
    return caught.value


def test_stale_patch_does_not_change_snapshot_or_cache(service, patch_document):
    before = service.state_summary()
    stale = copy.deepcopy(patch_document)
    stale["base_version"] = 0
    stale["target_version"] = 3
    stale.pop("checksum", None)
    expect_code("E_STALE_BASE", service.apply_patch, stale)
    after = service.state_summary()
    assert after["active_version"] == before["active_version"] == 1
    assert after["caches"] == before["caches"]
    assert after["snapshots"] == before["snapshots"]


def test_single_table_patch_rebuilds_affected_cache_atomically(service, patch_document):
    result = service.apply_patch(patch_document)
    assert result["base_version"] == 1
    assert result["active_version"] == 2
    assert result["rebuilt_tables"] == ["orders", "orders_view"]
    assert result["reused_tables"] == ["catalog", "events", "order_items"]
    caches = result["cache"]["tables"]
    assert {entry["cache_version"] for entry in caches.values()} == {2}
    assert caches["orders"]["rebuilt"] is True
    assert caches["orders"]["source_version"] == 2
    assert caches["orders_view"]["rebuilt"] is True
    assert caches["events"]["rebuilt"] is False
    assert caches["events"]["source_version"] == 1
    assert caches["events"]["algorithm_signature"] == service.current_state().caches[
        "events"
    ].algorithm_signature

    old = service.state_summary()
    again = copy.deepcopy(patch_document)
    again["target_version"] = 3
    again.pop("checksum", None)
    expect_code("E_STALE_BASE", service.apply_patch, again)
    assert service.current_state().version == old["active_version"] == 2
    assert service.state_summary()["caches"] == old["caches"]


def test_query_binds_version_if_snapshot_is_replaced_mid_request(service, patch_document):
    entered = threading.Event()
    release = threading.Event()
    original_fetch = service.data.fetch_rows
    first_call = True
    guard = threading.Lock()

    def blocking_fetch(target, table, predicate):
        nonlocal first_call
        should_block = False
        with guard:
            if first_call:
                first_call = False
                should_block = True
        if should_block:
            entered.set()
            assert release.wait(10)
        return original_fetch(target, table, predicate)

    service.data.fetch_rows = blocking_fetch
    outcome: dict = {}

    def run_query():
        try:
            outcome["result"] = service.execute_query(
                {
                    "request_id": "mid-request-v1",
                    "logical_table": "orders",
                    "query_type": "eq",
                    "shard_key_value": 7,
                }
            )
        except BaseException as exc:  # pragma: no cover - diagnostic path
            outcome["error"] = exc

    thread = threading.Thread(target=run_query)
    thread.start()
    assert entered.wait(10)
    service.apply_patch(patch_document)
    release.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert "error" not in outcome
    result = outcome["result"]
    assert result["routing_version"] == 1
    assert result["current_version_after"] == 2
    assert result["version_replaced_during_request"] is True
    assert [(item["table"], item["shard"]) for item in result["physical_tables"]] == [
        ("orders_0003", 3)
    ]
    assert [row["id"] for row in result["rows"]] == [10, 1003]
    assert result["cache"]["cache_version"] == 1

    current = service.execute_query(
        {
            "request_id": "mid-request-v2",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
            "routing_version": 2,
        }
    )
    assert current["routing_version"] == 2
    assert [item["table"] for item in current["physical_tables"]] == ["orders_0002"]


def test_duplicate_request_id_executes_once_and_replays_after_restart(service):
    original_fetch = service.data.fetch_rows
    call_count = 0
    count_lock = threading.Lock()

    def slow_fetch(target, table, predicate):
        nonlocal call_count
        with count_lock:
            call_count += 1
        time.sleep(0.02)
        return original_fetch(target, table, predicate)

    service.data.fetch_rows = slow_fetch
    request = {
        "request_id": "idempotent-scan",
        "logical_table": "orders",
        "query_type": "scan",
        "limit": 100,
    }

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(lambda _: service.execute_query(dict(request)), range(24)))
    assert all(result["request_id"] == "idempotent-scan" for result in results)
    assert {tuple(row["id"] for row in result["rows"]) for result in results} == {
        (1, 9001, 2, 3, 7, 4, 5, 6, 10, 1002, 1003, 12, 13, 14, 15)
    }
    assert call_count == 4
    batch = service.store.get_query_batch("idempotent-scan")
    assert batch["status"] == "SUCCEEDED"
    persisted = service.store.list_query_batches()
    assert sum(1 for row in persisted if row["request_id"] == "idempotent-scan") == 1

    conflict = dict(request)
    conflict["limit"] = 1
    expect_code("E_REQUEST_CONFLICT", service.execute_query, conflict)
    assert call_count == 4

    metadata_path = service.store.path
    data_dir = service.data.data_dir
    service.close(graceful=True)
    reopened = ShardRouteService(metadata_db=metadata_path, data_dir=data_dir)
    try:
        replayed = reopened.execute_query(dict(request))
        assert replayed["idempotent_replay"] is True
        assert [row["id"] for row in replayed["rows"]] == [
            1,
            9001,
            2,
            3,
            7,
            4,
            5,
            6,
            10,
            1002,
            1003,
            12,
            13,
            14,
            15,
        ]
        assert call_count == 4
    finally:
        reopened.close(graceful=True)


def test_concurrent_readers_never_observe_mixed_cache_versions(service, patch_document):
    def read_loop(worker: int):
        seen = []
        for iteration in range(12):
            result = service.execute_query(
                {
                    "request_id": f"reader-{worker}-{iteration}",
                    "logical_table": "orders",
                    "query_type": "scan",
                    "limit": 1,
                }
            )
            assert result["routing_version"] in {1, 2}
            assert result["cache"]["cache_version"] == result["routing_version"]
            chosen = result["physical_tables"][0]["table"]
            expected = "orders_0000" if result["routing_version"] == 1 else "orders_0000"
            assert chosen == expected
            seen.append(result["routing_version"])
        return seen

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(read_loop, worker) for worker in range(10)]
        time.sleep(0.01)
        patch_result = service.apply_patch(patch_document)
        versions = [version for future in futures for version in future.result()]
    assert patch_result["active_version"] == 2
    assert set(versions).issubset({1, 2})
    assert service.current_state().version == 2
    assert all(cache.effective_version == 2 for cache in service.current_state().caches.values())


def test_single_writer_serializes_competing_patches(service, patch_document):
    first = copy.deepcopy(patch_document)
    second = copy.deepcopy(patch_document)
    second["target_version"] = 3
    second.pop("checksum", None)
    barrier = threading.Barrier(3)

    def apply(document):
        barrier.wait()
        try:
            return ("ok", service.apply_patch(document))
        except BusinessError as exc:
            return ("error", exc)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(apply, first)
        future_b = executor.submit(apply, second)
        barrier.wait()
        outcomes = [future_a.result(), future_b.result()]
    successes = [value for status, value in outcomes if status == "ok"]
    errors = [value for status, value in outcomes if status == "error"]
    assert len(successes) == 1
    assert len(errors) == 1
    assert errors[0].code == "E_STALE_BASE"
    assert successes[0]["active_version"] in {2, 3}
    assert service.current_state().version == successes[0]["active_version"]

