from __future__ import annotations

import copy
from typing import Any

import pytest

from cabin_voice.canonical import canonical_bytes
from cabin_voice.errors import CabinError
from cabin_voice.projection import build_activity_projection, build_core_result


def _assignment(state: dict[str, Any], shard_id: str, word: str) -> dict[str, Any]:
    values = state["stages"]["arbitrate"]["artifact"]["shards"][shard_id]["assignments"]
    return next(item for item in values if item["word"] == word)


def test_edit_versions_audit_split_merge_and_invalid_interval(ready_service) -> None:
    service = ready_service
    scope = service.edit(
        "select_scope",
        parameters={},
        expected_version=0,
        shard_id="shard-001",
    )
    assert scope["edit_version"] == 1
    split = service.edit(
        "split_activity",
        parameters={"zone_id": "driver", "at_ms": 400},
        expected_version=1,
        shard_id="shard-001",
    )
    assert split["edit_version"] == 2
    state = service.workspace.load_state()
    timeline = build_activity_projection(state)
    driver = timeline["shard-001"]["zones"]["driver"]
    assert [item["start_ms"] for item in driver[:2]] == [220, 400]
    assert driver[0]["end_ms"] == 400
    merged = service.edit(
        "merge_activity",
        parameters={"zone_id": "driver", "at_ms": 400},
        expected_version=2,
        shard_id="shard-001",
    )
    assert merged["edit_version"] == 3
    state = service.workspace.load_state()
    timeline = build_activity_projection(state)
    assert len(timeline["shard-001"]["zones"]["driver"]) == 1
    audit = state["edit"]["audit"]
    assert [entry["edit_version"] for entry in audit] == [1, 2, 3]
    assert [entry["operation"] for entry in audit] == [
        "select_scope",
        "split_activity",
        "merge_activity",
    ]
    with pytest.raises(CabinError) as caught:
        service.edit(
            "split_activity",
            parameters={"zone_id": "driver", "at_ms": 0},
            expected_version=3,
            shard_id="shard-001",
        )
    error = caught.value
    assert error.code == "E_INVALID_INTERVAL"
    assert error.details["shard_id"] == "shard-001"
    assert error.details["zone_id"] == "driver"
    assert error.details["start_ms"] == 0


def test_word_hit_edit_and_stale_duplicate(ready_service) -> None:
    service = ready_service
    scope = service.edit("select_scope", parameters={}, expected_version=0, shard_id="shard-001")
    upsert = service.edit(
        "upsert_word_hit",
        parameters={
            "zone_id": "rear_right",
            "word": "打开空调",
            "start_ms": 280,
            "end_ms": 450,
            "strength": 0.55,
        },
        expected_version=scope["edit_version"],
        shard_id="shard-001",
    )
    assert upsert["edit_version"] == 2
    state = service.workspace.load_state()
    changed = next(
        hit
        for hit in state["edit"]["word_hits"]["shard-001"]
        if hit["zone_id"] == "rear_right" and hit["word"] == "打开空调"
    )
    assert changed["strength"] == 0.55
    deleted = service.edit(
        "delete_word_hit",
        parameters={"hit_id": changed["hit_id"]},
        expected_version=2,
        shard_id="shard-001",
    )
    assert deleted["edit_version"] == 3
    with pytest.raises(CabinError) as caught:
        service.edit(
            "set_priority",
            parameters={"zone_id": "driver", "priority": 2},
            expected_version=0,
            shard_id="shard-001",
        )
    assert caught.value.code == "E_STALE_VERSION"
    assert caught.value.details["current_version"] == 3
    with pytest.raises(CabinError) as caught:
        service.edit(
            "set_priority",
            parameters={"zone_id": "driver", "priority": 2},
            expected_version=3,
            shard_id="shard-002",
        )
    assert caught.value.code == "E_STALE_VERSION"
    assert caught.value.details["current_scope_shard_id"] == "shard-001"


def test_priority_edit_reuses_vad_and_matches_full(ready_service) -> None:
    service = ready_service
    state = service.workspace.load_state()
    before = _assignment(state, "shard-003", "打开座椅加热")
    assert before["responsible_zone"] == "rear_left"
    vad_seq = state["stages"]["vad"]["snapshot_seq"]
    vad_digest = state["stages"]["vad"]["artifact_digest"]
    scope = service.edit("select_scope", parameters={}, expected_version=0, shard_id="shard-003")
    edit = service.edit(
        "set_priority",
        parameters={"zone_id": "rear_right", "priority": 1},
        expected_version=scope["edit_version"],
        shard_id="shard-003",
    )
    assert edit["edit_version"] == 2
    result = service.run(verify=True)
    assert result["recomputed_stages"] == ["arbitrate"]
    assert result["reused_stages"] == ["vad"]
    assert result["result_digest"] == result["verified_full_digest"]
    state = service.workspace.load_state()
    assert state["stages"]["vad"]["snapshot_seq"] == vad_seq
    assert state["stages"]["vad"]["artifact_digest"] == vad_digest
    after = _assignment(state, "shard-003", "打开座椅加热")
    assert after["responsible_zone"] == "rear_right"
    assert after["responsible_zone"] != before["responsible_zone"]


def test_arbitration_config_reuses_vad_and_vad_config_recomputes_both(ready_service) -> None:
    service = ready_service
    scope = service.edit("select_scope", parameters={}, expected_version=0, shard_id="shard-002")
    config_edit = service.edit(
        "config_patch",
        parameters={"patch": {"arbitration": {"review_margin": 0.05}}},
        expected_version=scope["edit_version"],
        shard_id="shard-002",
    )
    assert config_edit["config_version"] == 1
    result = service.run(verify=True)
    assert result["reused_stages"] == ["vad"]
    assert result["recomputed_stages"] == ["arbitrate"]
    assert result["result_digest"] == result["verified_full_digest"]
    frame_edit = service.edit(
        "config_patch",
        parameters={"patch": {"vad": {"hop_ms": 8.0}}},
        expected_version=config_edit["edit_version"],
        shard_id="shard-002",
    )
    assert frame_edit["config_version"] == 2
    result = service.run(verify=True)
    assert result["recomputed_stages"] == ["vad", "arbitrate"]
    assert result["reused_stages"] == []
    assert result["result_digest"] == result["verified_full_digest"]
    state = service.workspace.load_state()
    assert state["stages"]["vad"]["artifact"]["shards"]["shard-001"]["zones"]["driver"]["hop_samples"] == 128
    geometry_edit = service.edit(
        "config_patch",
        parameters={
            "patch": {
                "arbitration": {
                    "geometry_m": {
                        "driver": [0.0, 0.0, 0.2],
                        "passenger": [2.5, 0.0, 0.2],
                        "rear_left": [0.0, 2.5, 0.2],
                        "rear_right": [2.5, 2.5, 0.2],
                    }
                }
            }
        },
        expected_version=frame_edit["edit_version"],
        shard_id="shard-002",
    )
    assert geometry_edit["config_version"] == 3
    result = service.run(verify=True)
    assert result["reused_stages"] == ["vad"]
    assert result["recomputed_stages"] == ["arbitrate"]
    assert result["result_digest"] == result["verified_full_digest"]


def test_word_hit_edit_reuses_vad_and_matches_full(ready_service) -> None:
    service = ready_service
    scope = service.edit("select_scope", parameters={}, expected_version=0, shard_id="shard-001")
    upsert = service.edit(
        "upsert_word_hit",
        parameters={
            "zone_id": "rear_right",
            "word": "关闭车窗",
            "start_ms": 1600,
            "end_ms": 1730,
            "strength": 0.97,
        },
        expected_version=scope["edit_version"],
        shard_id="shard-001",
    )
    assert upsert["edit_version"] == 2
    result = service.run(verify=True)
    assert result["reused_stages"] == ["vad"]
    assert result["recomputed_stages"] == ["arbitrate"]
    assert result["result_digest"] == result["verified_full_digest"]
    state = service.workspace.load_state()
    added = next(
        hit
        for hit in state["edit"]["word_hits"]["shard-001"]
        if hit["zone_id"] == "rear_right" and hit["word"] == "关闭车窗"
    )
    deleted = service.edit(
        "delete_word_hit",
        parameters={"hit_id": added["hit_id"]},
        expected_version=2,
        shard_id="shard-001",
    )
    assert deleted["edit_version"] == 3
    result = service.run(verify=True)
    assert result["reused_stages"] == ["vad"]
    assert result["recomputed_stages"] == ["arbitrate"]
    assert result["result_digest"] == result["verified_full_digest"]


def test_incremental_mismatch_reports_first_difference(ready_service, monkeypatch) -> None:
    service = ready_service
    original = service.engine._full_core_result

    def corrupting_full(state: dict[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(original(state))
        value["activity"]["shard-001"]["unexpected"] = 1
        return value

    monkeypatch.setattr(service.engine, "_full_core_result", corrupting_full)
    with pytest.raises(CabinError) as caught:
        service.run(verify=True)
    assert caught.value.code == "E_INCREMENTAL_MISMATCH"
    difference = caught.value.details["first_difference"]
    assert isinstance(difference["offset"], int)
    assert difference["offset"] >= 0
    assert caught.value.details["incremental_digest"] != caught.value.details["full_digest"]


def test_cache_hit_and_miss_payloads_are_byte_identical(ready_service) -> None:
    service = ready_service
    first = service.query("tdoa", shard_id="shard-001", time_ms=400, budget_ms=1000)
    second = service.query("tdoa", shard_id="shard-001", time_ms=400, budget_ms=1000)
    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert canonical_bytes(first["result"]) == canonical_bytes(second["result"])
    assert first["result_digest"] == second["result_digest"]
    assert first["result"]["events"][0]["pair_delay_details"]


def test_edit_version_invalidates_cached_query_result(ready_service) -> None:
    service = ready_service
    first = service.query("active", shard_id="shard-001", time_ms=350, budget_ms=1000)
    scope = service.edit("select_scope", parameters={}, expected_version=0, shard_id="shard-001")
    service.edit(
        "set_priority",
        parameters={"zone_id": "driver", "priority": 2},
        expected_version=scope["edit_version"],
        shard_id="shard-001",
    )
    second = service.query("active", shard_id="shard-001", time_ms=350, budget_ms=1000)
    assert second["cache_hit"] is False
    assert second["result_digest"] != first["result_digest"]
    assert second["result"]["active_intervals"][0]["priority"] == 2


def test_latency_budget_error_has_required_fields(ready_service) -> None:
    service = ready_service
    service.query("active", shard_id="shard-001", time_ms=350, budget_ms=1000)
    with pytest.raises(CabinError) as caught:
        service.query("active", shard_id="shard-001", time_ms=350, budget_ms=0)
    error = caught.value
    assert error.code == "E_LATENCY_BUDGET"
    assert error.details["query_id"].startswith("qry-")
    assert error.details["elapsed_ms"] >= 0
    assert error.details["budget_ms"] == 0
    assert error.details["query_kind"] == "active"


def test_query_after_edit_requires_incremental_arbitration(ready_service) -> None:
    service = ready_service
    scope = service.edit("select_scope", parameters={}, expected_version=0, shard_id="shard-003")
    service.edit(
        "set_priority",
        parameters={"zone_id": "rear_right", "priority": 1},
        expected_version=scope["edit_version"],
        shard_id="shard-003",
    )
    with pytest.raises(CabinError) as caught:
        service.query("assignment", shard_id="shard-003", time_ms=900, budget_ms=1000)
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["reason"] == "stage_fingerprint_mismatch"
    service.run(verify=True)
    result = service.query("assignment", shard_id="shard-003", time_ms=900, budget_ms=1000)
    assert result["result"]["events"][0]["responsible_zone"] == "rear_right"


def test_invalid_stage_for_jump_and_query_before_pipeline(service) -> None:
    with pytest.raises(CabinError) as caught:
        service.run(stage="arbitrate")
    assert caught.value.code == "E_INVALID_STAGE"
    with pytest.raises(CabinError) as caught:
        service.query("active", shard_id="shard-001", time_ms=100, budget_ms=1000)
    assert caught.value.code == "E_INVALID_STAGE"
