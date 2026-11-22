from __future__ import annotations

import copy

import pytest

from hotroute import samples
from hotroute.engine import Engine
from hotroute.errors import BusinessError, E_GRAPH_INVALID
from hotroute.graph import validate_graph


def test_valid_graph_is_normalized_and_signature_stable():
    graph = validate_graph(samples.v1_graph())
    assert graph["outputs"] == ["act"]
    assert len(graph["nodes"]) == 3
    assert graph["vocab_refs"] == ["tok-v1"]


def test_invalid_shape_reports_first_node():
    graph = samples.v1_graph()
    graph["nodes"][1]["inputs"][0]["shape"] = [2, 7]
    with pytest.raises(BusinessError) as caught:
        validate_graph(graph)
    assert caught.value.code == E_GRAPH_INVALID
    assert caught.value.context["node_id"] == "norm"
    assert caught.value.context["field"] == "inputs[0]"


def test_invalid_dtype_reports_first_node():
    graph = samples.v1_graph()
    graph["nodes"][0]["inputs"][0]["dtype"] = "complex64"
    with pytest.raises(BusinessError) as caught:
        validate_graph(graph)
    assert caught.value.code == E_GRAPH_INVALID
    assert caught.value.context["node_id"] == "linear"
    assert caught.value.context["field"] == "inputs[0].dtype"


def test_invalid_quant_bits_enum():
    graph = samples.v1_graph()
    graph["nodes"][2]["quant_bits"] = 7
    with pytest.raises(BusinessError) as caught:
        validate_graph(graph)
    assert caught.value.code == E_GRAPH_INVALID
    assert caught.value.context["node_id"] == "act"
    assert caught.value.context["field"] == "quant_bits"


def test_operator_semantics_are_checked():
    graph = samples.v1_graph()
    graph["nodes"][0]["outputs"][0]["shape"] = [2, 5]
    with pytest.raises(BusinessError) as caught:
        validate_graph(graph)
    assert caught.value.code == E_GRAPH_INVALID
    assert caught.value.context["node_id"] == "linear"


def test_invalid_snapshot_is_not_persisted(engine: Engine):
    first = engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=1))
    invalid_graph = samples.v1_graph()
    invalid_graph["nodes"][0]["quant_bits"] = 3
    payload = samples.initial_snapshot(snapshot_seq=2)
    payload["models"].append(
        {"model_id": "broken-model", "version": "bad", "graph": invalid_graph}
    )
    with pytest.raises(BusinessError) as caught:
        engine.submit_snapshot(payload)
    assert caught.value.code == E_GRAPH_INVALID
    assert caught.value.context["node_id"] == "linear"
    state = engine.get_state()
    assert state["snapshot_seq"] == 1
    assert state["state_version"] == first["state_version"]
    assert state["active_models"][samples.MODEL_ID]["version"] == "v1"


def test_non_monotonic_snapshot_is_rejected(engine: Engine):
    first = engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=1))
    with pytest.raises(BusinessError) as caught:
        engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=1))
    assert caught.value.code == "E_SNAPSHOT_STALE"
    assert engine.get_state()["state_version"] == first["state_version"]


def test_invalid_snapshot_sequence_uses_snapshot_error(engine: Engine):
    payload = samples.initial_snapshot(snapshot_seq=1)
    payload["snapshot_seq"] = 0
    with pytest.raises(BusinessError) as caught:
        engine.submit_snapshot(payload)
    assert caught.value.code == "E_SNAPSHOT_INVALID"
    assert caught.value.context["field"] == "snapshot_seq"

