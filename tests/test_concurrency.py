from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from mv_workbench.errors import WorkbenchError


def wait_for_refreshing(service, view_name: str, timeout: float = 3.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        freshness = service.get_freshness(view_name)
        if freshness["state"] == "REFRESHING":
            return freshness
        time.sleep(0.02)
    raise AssertionError(f"view {view_name} never became REFRESHING")


def test_query_binds_old_version_while_refresh_is_in_flight(
    fresh_service, correction_batch
) -> None:
    fresh_service.submit_batch(correction_batch)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            fresh_service.refresh,
            "slow-refresh",
            mode="all_stale",
            delay_ms=500,
        )
        wait_for_refreshing(fresh_service, "v_region_revenue")
        during = fresh_service.query_view("v_region_revenue")
        assert during["bound_version"] == 1
        assert during["state"] == "REFRESHING"
        assert during["rows"] == [
            {"orders": 2, "region": "east", "revenue": 200.5},
            {"orders": 2, "region": "north", "revenue": 120.0},
            {"orders": 1, "region": "south", "revenue": 95.0},
            {"orders": 2, "region": "west", "revenue": 250.0},
        ]
        result = future.result(timeout=5)
    assert result["summary"]["partial"] == 0
    after = fresh_service.query_view("v_region_revenue")
    assert after["bound_version"] == 2
    assert after["rows"][0]["revenue"] == 260.0
    old = fresh_service.query_view("v_region_revenue", version=1)
    assert old["bound_version"] == 1
    assert old["rows"][0]["revenue"] == 200.5
    with pytest.raises(WorkbenchError) as captured:
        fresh_service.query_view("v_region_revenue", version=999)
    assert captured.value.code == "E_VERSION_RETIRED"


def test_duplicate_refresh_requests_reuse_exactly_one_result(fresh_service) -> None:
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            fresh_service.refresh, "same-concurrent", mode="all", delay_ms=150
        )
        second = executor.submit(
            fresh_service.refresh, "same-concurrent", mode="all", delay_ms=150
        )
        one = first.result(timeout=5)
        two = second.result(timeout=5)
    assert one == two
    assert one["commit_seq"] == two["commit_seq"]
    assert fresh_service.query_view("v_region_revenue")["bound_version"] == 2
    with pytest.raises(WorkbenchError) as captured:
        fresh_service.refresh("same-concurrent", mode="all_stale")
    assert captured.value.code == "E_CONFLICTING_REQUEST"


def test_atomic_refresh_states_are_visible_as_refreshing(fresh_service, correction_batch) -> None:
    fresh_service.submit_batch(correction_batch)
    affected = {
        item["view"]
        for item in fresh_service.diagnostics()["views"]
        if item["state"] == "STALE"
    }
    assert "v_return_reasons" not in affected
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            fresh_service.refresh, "visibility-check", mode="all_stale", delay_ms=350
        )
        wait_for_refreshing(fresh_service, "v_region_revenue")
        observed = {item["view"]: item["state"] for item in fresh_service.diagnostics()["views"]}
        assert all(observed[name] == "REFRESHING" for name in affected)
        assert observed["v_return_reasons"] == "FRESH"
        future.result(timeout=5)
    final = {item["view"]: item["state"] for item in fresh_service.diagnostics()["views"]}
    assert all(final[name] == "FRESH" for name in affected)


def test_multiple_concurrent_reads_observe_one_committed_version(
    fresh_service, correction_batch
) -> None:
    fresh_service.submit_batch(correction_batch)
    with ThreadPoolExecutor(max_workers=1) as writer:
        future = writer.submit(
            fresh_service.refresh, "concurrent-read-refresh", mode="all_stale", delay_ms=400
        )
        wait_for_refreshing(fresh_service, "v_region_revenue")
        with ThreadPoolExecutor(max_workers=6) as readers:
            queries = list(
                readers.map(
                    lambda _: fresh_service.query_view("v_region_revenue"),
                    range(12),
                )
            )
        assert {query["bound_version"] for query in queries} == {1}
        assert {query["state"] for query in queries} == {"REFRESHING"}
        assert all(query["rows"] == queries[0]["rows"] for query in queries)
        future.result(timeout=5)


def test_query_started_before_replacement_finishes_on_bound_snapshot(
    fresh_service,
) -> None:
    with ThreadPoolExecutor(max_workers=1) as query_pool:
        query_future = query_pool.submit(
            fresh_service.query_view, "v_region_revenue", delay_ms=500
        )
        time.sleep(0.12)
        replacement = fresh_service.refresh("replace-during-query", mode="all")
    query = query_future.result(timeout=5)
    assert replacement["summary"]["fresh"] == 7
    assert query["bound_version"] == 1
    assert query["state"] == "FRESH"
    assert query["rows"][0]["revenue"] == 200.5
    assert fresh_service.query_view("v_region_revenue")["bound_version"] == 2
