from __future__ import annotations

from pathlib import Path

import pytest

from schedsim.affinity import requirement_matches
from schedsim.engine import SchedulingEngine
from schedsim.errors import E_SPEC_INVALID, E_TOPOLOGY_KEY_MISSING, SchedError
from schedsim.models import LabelRequirement
from schedsim.quantities import (
    QuantityError,
    parse_cpu_millicores,
    parse_memory_bytes,
)
from schedsim.samples import SampleRepository
from tests.support import make_node, make_payload, make_pod

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_cpu_memory_exact_examples() -> None:
    assert parse_cpu_millicores("500m") == 500
    assert parse_cpu_millicores("2") == 2000
    assert parse_cpu_millicores("1.5") == 1500
    assert parse_memory_bytes("512Mi") == 512 * 1024**2
    assert parse_memory_bytes("1Gi") == 1024**3
    assert parse_memory_bytes("1G") == 1000**3
    assert parse_memory_bytes("123456") == 123456


@pytest.mark.parametrize(
    "parser,value",
    [
        (parse_cpu_millicores, "-1"),
        (parse_cpu_millicores, "1.5m"),
        (parse_cpu_millicores, "1Gi"),
        (parse_memory_bytes, "-2Mi"),
        (parse_memory_bytes, "1GiB"),
        (parse_memory_bytes, "1.1Mi"),
        (parse_memory_bytes, "999999999999999999999999999999Ei"),
    ],
)
def test_bad_quantities_are_rejected(parser, value) -> None:
    with pytest.raises(QuantityError):
        parser(value)


def test_invalid_boundary_sample_reports_field_paths_and_is_not_persisted(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = SampleRepository(REPO_ROOT / "samples").load("boundary_invalid")
    with pytest.raises(SchedError) as caught:
        engine.submit_batch(payload)
    error = caught.value
    assert error.code == E_SPEC_INVALID
    paths = {failure["path"] for failure in error.context["failures"]}
    assert "nodes[0].allocatable.cpu" in paths
    assert "nodes[0].allocatable.memory" in paths
    assert "pods[0].requests.cpu" in paths
    assert "pods[0].requests.memory" in paths
    assert "pods[0].nodeSelector.undeclared.example/key" in paths
    assert engine.storage.batch_exists("boundary-invalid") is False
    assert engine.storage.list_batches() == []


def test_unknown_node_label_key_fails_whole_batch(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [make_node("n1")],
        [make_pod("p1", node_selector={"not-on-any-node": "yes"})],
    )
    with pytest.raises(SchedError) as caught:
        engine.submit_batch(payload)
    assert caught.value.code == E_SPEC_INVALID
    assert any(
        item["path"] == "pods[0].nodeSelector.not-on-any-node"
        for item in caught.value.context["failures"]
    )
    assert not engine.storage.batch_exists("batch-1")


def test_missing_topology_key_uses_dedicated_business_code(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [make_node("n1")],
        [
            make_pod(
                "p1",
                affinity={
                    "podAntiAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "labelSelector": {"matchLabels": {"app": "x"}},
                                "topologyKey": "topology.example/undeclared",
                            }
                        ]
                    }
                },
            )
        ],
    )
    with pytest.raises(SchedError) as caught:
        engine.submit_batch(payload)
    assert caught.value.code == E_TOPOLOGY_KEY_MISSING
    assert any(
        item["path"].endswith("topologyKey")
        for item in caught.value.context["failures"]
    )
    assert not engine.storage.batch_exists("batch-1")


def test_all_required_node_affinity_operators() -> None:
    labels = {"zone": "2", "disk": "ssd"}
    assert requirement_matches(LabelRequirement("zone", "In", ("1", "2"), "x"), labels)
    assert requirement_matches(LabelRequirement("zone", "NotIn", ("1",), "x"), labels)
    assert requirement_matches(LabelRequirement("disk", "Exists", (), "x"), labels)
    assert requirement_matches(LabelRequirement("missing", "DoesNotExist", (), "x"), labels)
    assert requirement_matches(LabelRequirement("zone", "Gt", ("1",), "x"), labels)
    assert requirement_matches(LabelRequirement("zone", "Lt", ("3",), "x"), labels)
    assert not requirement_matches(LabelRequirement("zone", "Gt", ("2",), "x"), labels)
    assert not requirement_matches(LabelRequirement("zone", "Lt", ("2",), "x"), labels)

