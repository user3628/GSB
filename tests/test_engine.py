from __future__ import annotations

import copy

import pytest

from hotroute import samples
from hotroute.engine import Engine
from hotroute.errors import (
    BusinessError,
    E_GRAY_RATIO_INVALID,
    E_REQUEST_INTERRUPTED,
    E_STALE_BASE,
    E_VOCAB_MISSING,
)


def _bootstrap(engine: Engine):
    return engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=1))


def test_quant_switch_migrates_and_shape_switch_rebuilds(engine: Engine):
    first = _bootstrap(engine)
    quant = engine.submit_increment(
        samples.quant_increment(first["state_version"], snapshot_seq=2)
    )
    quant_manifest = quant["migrations"][0]
    assert quant_manifest["diff"]["mask"] == 4
    assert quant_manifest["diff"]["in_place_migratable"] is True
    assert "preprocess_cache" in quant_manifest["migrate"]
    assert "inflight_index" in quant_manifest["migrate"]
    assert "preprocess_cache" not in quant_manifest["rebuild"]
    assert "inflight_index" not in quant_manifest["rebuild"]

    state = engine.get_state()
    assert state["snapshot_seq"] == 2
    assert all(artifact["snapshot_seq"] == 2 for artifact in state["artifacts"])

    shape = engine.submit_increment(samples.shape_increment(quant["state_version"], snapshot_seq=3))
    shape_manifest = shape["migrations"][0]
    assert shape_manifest["diff"]["mask"] & 1
    assert shape_manifest["diff"]["in_place_migratable"] is False
    assert "preprocess_cache" in shape_manifest["rebuild"]
    assert "inflight_index" in shape_manifest["rebuild"]
    state = engine.get_state()
    assert state["snapshot_seq"] == 3
    assert {artifact["snapshot_seq"] for artifact in state["artifacts"]} == {3}
    assert {route["version"] for route in state["routes"]} == {"v3-shape"}


def test_in_flight_request_keeps_version_across_switch(engine: Engine):
    first = _bootstrap(engine)
    quant = engine.submit_increment(
        samples.quant_increment(first["state_version"], snapshot_seq=2)
    )
    decision = engine.decide_gray("inflight-unit", samples.MODEL_ID)
    assert decision["target_version"] == "v2-quant"
    reference = engine.infer(
        "v2-reference-unit", samples.MODEL_ID, seed=314, version="v2-quant"
    )
    engine.submit_increment(samples.shape_increment(quant["state_version"], snapshot_seq=3))
    result = engine.infer("inflight-unit", samples.MODEL_ID, seed=314)
    assert result["bound_version"] == "v2-quant"
    assert result["result"]["result_digest"] == reference["result"]["result_digest"]
    repeated = engine.infer("inflight-unit", samples.MODEL_ID, seed=314)
    assert repeated["cached"] is True
    assert repeated["result"]["result_digest"] == result["result"]["result_digest"]


def test_gray_split_fixed_point_and_replay(engine: Engine):
    first = _bootstrap(engine)
    quant = engine.submit_increment(
        samples.quant_increment(first["state_version"], snapshot_seq=2)
    )
    engine.submit_increment(samples.shape_increment(quant["state_version"], snapshot_seq=3))
    first_decision = engine.decide_gray("gray-unit-1", samples.MODEL_ID)
    second_decision = engine.decide_gray("gray-unit-2", samples.MODEL_ID)
    assert first_decision["target_version"] == "v2-quant"
    assert second_decision["target_version"] == "v3-shape"
    assert first_decision["fixed_point"] == {"units": 500000, "scale": 1000000}
    assert first_decision["ratio_rounding_error"] == "0.0"
    replay = engine.decide_gray("gray-unit-1", samples.MODEL_ID)
    assert replay["target_version"] == first_decision["target_version"]
    assert replay["replayed"] is True


def test_invalid_gray_ratio_error_context(engine: Engine):
    _bootstrap(engine)
    with pytest.raises(BusinessError) as caught:
        engine.decide_gray("bad-ratio", samples.MODEL_ID, ratio=1.25)
    assert caught.value.code == E_GRAY_RATIO_INVALID
    assert caught.value.context["legal_interval"] == [0, 1]
    assert caught.value.context["current_ratio"] == "1"


def test_vocab_missing_does_not_execute_partial_graph(engine: Engine):
    payload = samples.initial_snapshot(snapshot_seq=1)
    payload["known_vocabs"] = ["tok-v1"]
    payload["models"][0]["graph"] = samples.v1_graph()
    payload["models"][0]["graph"]["vocab_refs"].append("missing-vocab")
    engine.submit_snapshot(payload)
    with pytest.raises(BusinessError) as caught:
        engine.infer("vocab-request", samples.MODEL_ID, seed=2)
    assert caught.value.code == E_VOCAB_MISSING
    assert caught.value.context["partial_inference_performed"] is False
    binding = engine.get_binding("vocab-request")
    assert binding["status"] == "pending"
    assert binding["result"] is None


def test_stale_base_does_not_change_committed_state(engine: Engine):
    first = _bootstrap(engine)
    quant = engine.submit_increment(
        samples.quant_increment(first["state_version"], snapshot_seq=2)
    )
    shape = engine.submit_increment(samples.shape_increment(quant["state_version"], snapshot_seq=3))
    before = engine.get_state()
    with pytest.raises(BusinessError) as caught:
        engine.submit_increment(
            samples.quant_increment(first["state_version"], snapshot_seq=4)
        )
    assert caught.value.code == E_STALE_BASE
    after = engine.get_state()
    assert after["snapshot_seq"] == before["snapshot_seq"] == 3
    assert after["state_version"] == before["state_version"] == shape["state_version"]


def test_full_snapshot_replaces_state_without_rebuilding_identical_artifacts(engine: Engine):
    first = _bootstrap(engine)
    before_state = engine.get_state()
    before_generations = {
        (artifact["kind"], artifact["generation"]) for artifact in before_state["artifacts"]
    }
    second_payload = copy.deepcopy(samples.initial_snapshot(snapshot_seq=2))
    second = engine.submit_snapshot(second_payload)
    manifest = second["migrations"][0]
    assert manifest["diff"]["mask"] == 0
    assert manifest["retained"] == [
        "route",
        "preprocess_cache",
        "inflight_index",
        "gray_plan",
    ]
    after_state = engine.get_state()
    after_generations = {
        (artifact["kind"], artifact["generation"]) for artifact in after_state["artifacts"]
    }
    assert after_generations == before_generations
    assert after_state["snapshot_seq"] == 2
    with pytest.raises(BusinessError) as caught:
        engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=2))
    assert caught.value.code == "E_SNAPSHOT_STALE"


def test_non_quant_graph_change_rebuilds_inflight_buffer(engine: Engine):
    first = _bootstrap(engine)
    graph = samples.v1_graph()
    graph["nodes"][2]["op"] = "tanh"
    payload = {
        "base_version": first["state_version"],
        "snapshot_seq": 2,
        "generated_at": samples.utc_now(),
        "models": [
            {"model_id": samples.MODEL_ID, "version": "op-change", "graph": graph}
        ],
        "active_models": {
            samples.MODEL_ID: {
                "version": "op-change",
                "previous_version": "v1",
                "gray_ratio": 1.0,
            }
        },
    }
    result = engine.submit_increment(payload)
    manifest = result["migrations"][0]
    assert manifest["diff"]["mask"] == 8
    assert manifest["diff"]["requires_buffer_rebuild"] is True
    assert "inflight_index" in manifest["rebuild"]
    assert "gray_plan" in manifest["rebuild"]


def test_full_snapshot_with_only_new_version_degrades_gray_old_to_new(engine: Engine):
    _bootstrap(engine)
    payload = samples.initial_snapshot(snapshot_seq=2)
    payload["models"] = [
        {
            "model_id": samples.MODEL_ID,
            "version": "v2-quant",
            "graph": samples.v2_quant_graph(),
        }
    ]
    payload["active_models"][samples.MODEL_ID] = {
        "version": "v2-quant",
        "gray_ratio": 0,
    }
    result = engine.submit_snapshot(payload)
    assert result["active_models"][samples.MODEL_ID]["previous_version"] == "v2-quant"
    decision = engine.decide_gray("only-new-full-snapshot", samples.MODEL_ID)
    assert decision["target_version"] == "v2-quant"


def test_full_snapshot_gray_requires_previous_graph(engine: Engine):
    first = _bootstrap(engine)
    payload = samples.initial_snapshot(snapshot_seq=2)
    payload["models"] = [
        {
            "model_id": samples.MODEL_ID,
            "version": "v2-quant",
            "graph": samples.v2_quant_graph(),
        }
    ]
    payload["active_models"][samples.MODEL_ID] = {
        "version": "v2-quant",
        "gray_ratio": 0.5,
    }
    with pytest.raises(BusinessError) as caught:
        engine.submit_snapshot(payload)
    assert caught.value.code == "E_VERSION_NOT_FOUND"
    state = engine.get_state()
    assert state["snapshot_seq"] == 1
    assert state["state_version"] == first["state_version"]


def test_pending_binding_survives_full_snapshot_graph_replacement(engine: Engine):
    _bootstrap(engine)
    decision = engine.decide_gray("pending-across-full-replace", samples.MODEL_ID)
    assert decision["target_version"] == "v1"
    payload = samples.initial_snapshot(snapshot_seq=2)
    payload["models"] = [
        {
            "model_id": samples.MODEL_ID,
            "version": "v2-quant",
            "graph": samples.v2_quant_graph(),
        }
    ]
    payload["active_models"][samples.MODEL_ID] = {
        "version": "v2-quant",
        "gray_ratio": 0,
    }
    engine.submit_snapshot(payload)
    inference = engine.infer("pending-across-full-replace", samples.MODEL_ID, seed=55)
    assert inference["bound_version"] == "v1"
    assert inference["bound_snapshot_seq"] == 1

