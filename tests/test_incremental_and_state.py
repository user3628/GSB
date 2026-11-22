from __future__ import annotations

from pathlib import Path

import pytest

from mv_workbench.errors import WorkbenchError

from .conftest import group_map


def test_stale_batch_is_rejected_without_state_change(
    fresh_service, correction_batch
) -> None:
    before_states = {
        row["view"]: (row["state"], row["materialized_version"])
        for row in fresh_service.list_views()
    }
    stale = dict(correction_batch)
    stale["base_version"] = 999
    with pytest.raises(WorkbenchError) as captured:
        fresh_service.submit_batch(stale)
    assert captured.value.code == "E_STALE_BASE"
    assert fresh_service.get_base_table("sales")["current_version"] == 1
    after_states = {
        row["view"]: (row["state"], row["materialized_version"])
        for row in fresh_service.list_views()
    }
    assert after_states == before_states


def test_illegal_transition_does_not_persist(fresh_service) -> None:
    before = fresh_service.get_freshness("v_region_revenue")
    with pytest.raises(WorkbenchError) as captured:
        fresh_service.transition_state(
            "v_region_revenue", "PARTIAL", reason="must be rejected"
        )
    assert captured.value.code == "E_INVALID_TRANSITION"
    after = fresh_service.get_freshness("v_region_revenue")
    assert after["state"] == before["state"] == "FRESH"
    assert after["materialized_version"] == before["materialized_version"]
    assert after["updated_at"] == before["updated_at"]


def test_correction_batch_may_contain_only_changed_columns(fresh_service) -> None:
    result = fresh_service.submit_batch(
        {
            "batch_id": "partial-column-correction",
            "table": "sales",
            "base_version": 1,
            "generated_at": "2026-01-16T12:00:00.000Z",
            "rows": [{"id": 1, "amount": 150.25}],
        }
    )
    assert result["committed_version"] == 2
    table = fresh_service.get_base_table("sales")
    row = next(row for row in table["rows"] if row["data"]["id"] == 1)
    assert row["data"] == {
        "id": 1,
        "region": "east",
        "channel": "web",
        "amount": 150.25,
        "qty": 2,
        "status": "active",
        "note": "priority",
    }
    assert row["row_version"] == 2


def test_batch_partial_aggregate_update_and_cascade(
    fresh_service, correction_batch, stale_batch
) -> None:
    old_revenue = fresh_service.query_view("v_region_revenue")
    old_peak = fresh_service.query_view("v_region_peak")
    old_revenue_west = group_map(old_revenue)['{"region":"west"}']
    assert old_revenue_west["rows"][0]["revenue"] == 250.0

    batch_result = fresh_service.submit_batch(correction_batch)
    assert batch_result["base_version"] == 1
    assert batch_result["committed_version"] == 2
    affected = {item["view"]: item for item in batch_result["affected_views"]}
    assert set(affected) == {
        "v_sales_active",
        "v_region_revenue",
        "v_region_peak",
        "v_channel_refund",
        "v_high_peak_regions",
        "v_revenue_audit",
    }
    assert all(item["state"] == "STALE" for item in affected.values())
    assert affected["v_channel_refund"]["depth"] == 1
    assert fresh_service.get_freshness("v_return_reasons")["state"] == "FRESH"

    refreshed = fresh_service.refresh("after-correction", mode="all_stale")
    by_view = {item["view"]: item for item in refreshed["views"]}
    revenue = by_view["v_region_revenue"]
    assert revenue["mode"] == "incremental_groups"
    assert set(revenue["aggregate_functions"]) == {"sum", "count"}
    assert {item["region"] for item in revenue["affected_groups"]} == {
        "east",
        "north",
        "south",
    }
    assert revenue["recomputed_groups"] == 3
    assert revenue["preserved_groups"] == 1

    peak = by_view["v_region_peak"]
    assert peak["mode"] == "full_recompute"
    assert set(peak["non_incremental_functions"]) == {"max", "quantile"}
    assert peak["recomputed_groups"] == 4
    assert "not closed under correction/deletion" in peak["reason"]

    new_revenue = fresh_service.query_view("v_region_revenue")
    new_peak = fresh_service.query_view("v_region_peak")
    new_revenue_west = group_map(new_revenue)['{"region":"west"}']
    assert new_revenue_west["digest"] == old_revenue_west["digest"]
    assert new_revenue_west["rows"] == old_revenue_west["rows"]
    assert group_map(new_revenue)['{"region":"east"}']["rows"][0]["revenue"] == 260.0
    assert group_map(new_peak)['{"region":"north"}']["rows"][0]["peak_amount"] == 45.0

    # Historical versions remain readable and carry old committed facts.
    old_again = fresh_service.query_view("v_region_revenue", version=1)
    assert group_map(old_again)['{"region":"east"}']["rows"][0]["revenue"] == 200.5
    assert new_revenue["bound_version"] == 2

    with pytest.raises(WorkbenchError) as captured:
        fresh_service.submit_batch(stale_batch)
    assert captured.value.code == "E_STALE_BASE"
    assert fresh_service.get_base_table("sales")["current_version"] == 2
    assert fresh_service.get_freshness("v_region_revenue")["materialized_version"] == 2


def test_dependency_gap_marks_partial_with_persisted_gap(
    fresh_service, correction_batch
) -> None:
    fresh_service.submit_batch(correction_batch)
    refresh = fresh_service.refresh(
        "downstream-only",
        mode="views",
        views=["v_revenue_audit"],
        include_downstream=False,
    )
    result = refresh["views"][0]
    assert result["view"] == "v_revenue_audit"
    assert result["state"] == "PARTIAL"
    assert result["version"] is None
    assert result["gaps"][0]["code"] == "DEPENDENCY_NOT_READY"
    assert result["gaps"][0]["view"] == "v_region_revenue"
    freshness = fresh_service.get_freshness("v_revenue_audit")
    assert freshness["state"] == "PARTIAL"
    assert freshness["gaps"] == result["gaps"]


def test_refresh_failure_marks_failed_and_downstream_views_partial(
    fresh_service, correction_batch
) -> None:
    fresh_service.submit_batch(correction_batch)
    failed = fresh_service.refresh(
        "diagnostic-failure",
        mode="all_stale",
        fail_views=["v_region_revenue"],
    )
    by_view = {item["view"]: item for item in failed["views"]}
    assert by_view["v_region_revenue"]["state"] == "PARTIAL"
    assert by_view["v_region_revenue"]["version"] is None
    assert by_view["v_region_revenue"]["gaps"][0]["code"] == "SIMULATED_REFRESH_FAILURE"
    assert by_view["v_revenue_audit"]["state"] == "PARTIAL"
    assert by_view["v_revenue_audit"]["gaps"][0]["code"] == "DEPENDENCY_NOT_READY"
    assert by_view["v_region_peak"]["state"] == "FRESH"
    assert fresh_service.get_freshness("v_region_revenue")["state"] == "PARTIAL"

    recovered = fresh_service.refresh("diagnostic-recovery", mode="all_stale")
    assert recovered["summary"]["partial"] == 0
    assert fresh_service.get_freshness("v_region_revenue")["state"] == "FRESH"
