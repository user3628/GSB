from __future__ import annotations

import pytest

from app.errors import WorkbenchError
from app.graph import build_graph, downstream_closure
from app.models import IncrementFragment, NormalizedResource, RawRecord, RoundingRule


def _resources() -> list[NormalizedResource]:
    return [
        NormalizedResource(resource_id="root", budget=1, weight_permille=900, parents=[], expected_version=1),
        NormalizedResource(resource_id="mid-a", budget=1, weight_permille=500, parents=["root"], expected_version=1),
        NormalizedResource(resource_id="mid-b", budget=1, weight_permille=700, parents=["root"], expected_version=1),
        NormalizedResource(resource_id="leaf", budget=1, weight_permille=100,
                           parents=["mid-a", "mid-b"], expected_version=1),
    ]


def test_layers_respect_parent_order_and_stable_sort():
    result = build_graph(_resources())
    assert result.layers[0] == ["root"]
    assert result.layers[1] == ["mid-b", "mid-a"]  # weight desc, id tiebreak
    assert result.layers[2] == ["leaf"]
    by_id = {r.resource_id: r for r in result.resources}
    for resource in result.resources:
        for parent in resource.parents:
            assert by_id[parent].layer < resource.layer


def test_deterministic_repeat():
    first = build_graph(_resources()).batches
    second = build_graph(_resources()).batches
    assert [b.model_dump() for b in first] == [b.model_dump() for b in second]


def test_cycle_reports_resource_identifiers():
    cyclic = _resources()
    cyclic[0].parents = ["leaf"]
    with pytest.raises(WorkbenchError) as exc:
        build_graph(cyclic)
    assert exc.value.code == "E_DEPENDENCY_CYCLE"
    assert "leaf" in exc.value.details["cycle"]


def test_downstream_closure_using_numpy():
    resources = _resources()
    downstream = downstream_closure(resources, ["root"])
    assert downstream == {"mid-a", "mid-b", "leaf"}


def test_import_increment_stale_and_idempotent(service, samples_dir):
    service.import_snapshot((samples_dir / "snapshot_v1.bin").read_bytes())
    fragment = IncrementFragment(
        base_snapshot_seq=1,
        submit_id="inc-x",
        changes=[RawRecord(resource_id="svc-cache", parents=["svc-gateway"], budget=2, weight_float=0.5)],
    )
    result = service.apply_increment(fragment)
    assert result.new_snapshot_seq == 2
    assert "svc-cache" in result.affected_resources

    replay = service.apply_increment(fragment)
    assert replay.new_snapshot_seq == 2

    stale = IncrementFragment(
        base_snapshot_seq=1, submit_id="inc-y",
        changes=[RawRecord(resource_id="svc-other", parents=[], budget=1, weight_float=0.1)],
    )
    with pytest.raises(WorkbenchError) as exc:
        service.apply_increment(stale)
    assert exc.value.code == "E_STALE_BASE"


def test_cycle_import_is_whole_batch_rejected(service, samples_dir):
    service.import_snapshot((samples_dir / "snapshot_v1.bin").read_bytes())
    before = service.diagnostics()["stats"]
    with pytest.raises(WorkbenchError) as exc:
        service.import_snapshot((samples_dir / "snapshot_cycle.jsonl").read_bytes())
    assert exc.value.code == "E_DEPENDENCY_CYCLE"
    after = service.diagnostics()["stats"]
    assert before == after
