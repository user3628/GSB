from __future__ import annotations

from pathlib import Path

import pytest

from mv_workbench.errors import WorkbenchError
from mv_workbench.graph import build_graph
from mv_workbench.samples import load_json, sample_paths, write_samples

from .conftest import SAMPLES


def test_fixed_samples_are_byte_for_byte_reproducible(tmp_path: Path) -> None:
    write_samples(tmp_path)
    for path in sorted(SAMPLES.glob("*.json")):
        generated = tmp_path / path.name
        assert generated.exists(), path.name
        assert generated.read_bytes() == path.read_bytes(), path.name


def test_sample_snapshot_has_null_bitmap_versions_deletes_and_digests() -> None:
    document = load_json(sample_paths(SAMPLES)["base_tables"])
    sales = next(table for table in document["tables"] if table["name"] == "sales")
    assert sales["version"] == 1
    assert sales["generated_at"] == "2026-01-15T09:00:00.000Z"
    assert sales["null_bitmap"]["note"].count(True) == 1
    assert sales["row_versions"] == [1] * sales["row_count"]
    assert sales["deleted"] == [False] * sales["row_count"]
    assert all(len(digest) == 64 for digest in sales["row_digests"])
    assert len(sales["snapshot_digest"]) == 64


def test_graph_layers_and_edges_are_deterministic() -> None:
    definitions = load_json(sample_paths(SAMPLES)["view_definitions"])
    graph = build_graph(definitions, {"sales", "returns"})
    assert graph.layers == [
        {
            "layer": 0,
            "views": ["v_region_peak", "v_region_revenue", "v_return_reasons", "v_sales_active"],
        },
        {
            "layer": 1,
            "views": ["v_channel_refund", "v_high_peak_regions", "v_revenue_audit"],
        },
    ]
    assert {"from": "v_sales_active", "to": "v_channel_refund", "kind": "view"} in graph.edges
    assert {"from": "returns", "to": "v_return_reasons", "kind": "table"} in graph.edges
    assert graph.downstream({"v_region_peak"}) == {"v_region_peak", "v_high_peak_regions"}


def test_cycle_has_view_ids_and_does_not_change_graph(service) -> None:
    before = service.get_graph()
    cycle = {
        "format": "test",
        "views": [
            {
                "name": "cycle_a",
                "source": {"kind": "view", "name": "cycle_b"},
                "operators": [],
            },
            {
                "name": "cycle_b",
                "source": {"kind": "view", "name": "cycle_a"},
                "operators": [],
            },
        ],
    }
    with pytest.raises(WorkbenchError) as captured:
        service.replace_definitions(cycle)
    assert captured.value.code == "E_DEPENDENCY_CYCLE"
    assert set(captured.value.context["views"]) == {"cycle_a", "cycle_b"}
    after = service.get_graph()
    assert after["graph_version"] == before["graph_version"]
    assert after["digest"] == before["digest"]


def test_unknown_reference_has_no_side_effect(service) -> None:
    before = service.get_graph()
    unknown = {
        "format": "test",
        "views": [
            {
                "name": "unknown_view",
                "source": {"kind": "view", "name": "missing_view"},
                "operators": [],
            }
        ],
    }
    with pytest.raises(WorkbenchError) as captured:
        service.replace_definitions(unknown)
    assert captured.value.code == "E_REFERENCE_UNKNOWN"
    assert captured.value.context["references"][0]["from"] == "missing_view"
    assert service.get_graph()["graph_version"] == before["graph_version"]
