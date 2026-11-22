from __future__ import annotations

import copy

import pytest

from dep_engine import Engine, EngineError
from dep_engine.generator import DEMO_CONFIG


def test_stage_api_requires_exact_prerequisite_chain(engine: Engine):
    with pytest.raises(EngineError) as caught:
        engine.run_stage(0, "validation")
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["missing_stage"] == "featureization"
    assert caught.value.details["valid_stages"] == [
        "featureization",
        "decoding",
        "validation",
        "aggregation",
    ]

    first = engine.run_stage(0, "featureization")
    assert first["status"] == "computed"
    assert first["sequence"] == 1

    reused = engine.run_stage(0, "featureization")
    assert reused["status"] == "reused"
    assert reused["sequence"] == 1

    with pytest.raises(EngineError) as caught:
        engine.run_stage(0, "validation")
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["missing_stage"] == "decoding"
    assert caught.value.details["available_stages"] == ["featureization"]

    assert engine.run_stage(0, "decoding")["status"] == "computed"
    assert engine.run_stage(0, "validation")["status"] == "computed"
    assert engine.run_stage(0, "aggregation")["status"] == "computed"
    assert engine.get_stage_artifact(0, "aggregation")["stage"] == "aggregation"


def test_stage_api_rejects_unknown_stage_and_out_of_range(engine: Engine):
    with pytest.raises(EngineError) as caught:
        engine.run_stage(0, "not-a-stage")
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["available_stages"] == []

    with pytest.raises(EngineError) as caught:
        engine.get_stage_artifact(99, "featureization")
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["valid_stages"]


def test_stage_api_targets_only_config_affected_stage(engine: Engine):
    engine.build()
    changed = copy.deepcopy(DEMO_CONFIG)
    changed["max_dependency_distance"] = 8
    engine.update_config(changed, build=False)

    with pytest.raises(EngineError) as caught:
        engine.run_stage(0, "aggregation")
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["stale_stage"] == "validation"

    validation = engine.run_stage(0, "validation")
    assert validation["status"] == "computed"
    aggregation = engine.run_stage(0, "aggregation")
    assert aggregation["status"] == "computed"
    assert engine.run_stage(0, "decoding")["status"] == "reused"


def test_graceful_stop_marker_consumed_after_current_shard(engine: Engine):
    engine.request_stop()
    result = engine.build()
    assert result["status"] == "stopped"
    assert result["processed_shards"] == 0
    assert engine.stop_marker.exists() is False

    resumed = engine.build()
    assert resumed["status"] == "complete"
    assert len(resumed["computed"]) == 12
