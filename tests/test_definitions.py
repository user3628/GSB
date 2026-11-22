from __future__ import annotations


def test_definition_replacement_updates_graph_and_cascades(
    fresh_service, definitions_v2
) -> None:
    before_revenue = fresh_service.query_view("v_region_revenue")
    replacement = fresh_service.replace_definitions(definitions_v2)
    assert replacement["previous_graph_version"] == 1
    assert replacement["graph_version"] == 2
    assert "v_region_order_bands" in replacement["new_views"]
    assert "v_region_peak" in replacement["changed_views"]
    affected = {item["view"] for item in replacement["affected_views"]}
    assert {"v_region_peak", "v_high_peak_regions", "v_revenue_audit", "v_region_order_bands"} <= affected

    states = {row["view"]: row for row in fresh_service.list_views()}
    assert states["v_region_peak"]["state"] == "STALE"
    assert states["v_high_peak_regions"]["state"] == "STALE"
    assert states["v_revenue_audit"]["state"] == "STALE"
    assert states["v_region_order_bands"]["state"] == "STALE"
    assert states["v_region_revenue"]["state"] == "FRESH"
    assert states["v_region_revenue"]["materialized_version"] == before_revenue["bound_version"]

    history = fresh_service.get_graph()["history"]
    assert [item["graph_version"] for item in history] == [1, 2]
    old_definitions = fresh_service.get_view_definitions(1)["definitions"]
    old_views = {view["name"]: view for view in old_definitions["views"]}
    assert old_views["v_revenue_audit"]["operators"][0]["where"][0]["value"] == 100

    refreshed = fresh_service.refresh("after-definition-replacement", mode="all_stale")
    by_view = {item["view"]: item for item in refreshed["views"]}
    assert by_view["v_region_order_bands"]["mode"] == "initial_build"
    assert by_view["v_high_peak_regions"]["state"] == "FRESH"
    assert by_view["v_revenue_audit"]["state"] == "FRESH"
    assert fresh_service.query_view("v_revenue_audit")["rows"] == [
        {"region": "east", "revenue": 200.5},
        {"region": "west", "revenue": 250.0},
    ]
    assert fresh_service.query_view("v_region_order_bands")["rows"] == [
        {"orders": 2, "region": "east"},
        {"orders": 2, "region": "north"},
        {"orders": 2, "region": "west"},
    ]
