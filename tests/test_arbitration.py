from __future__ import annotations

from cabin_voice.projection import build_core_result


def _by_word(state, shard_id: str, word: str):
    assignments = state["stages"]["arbitrate"]["artifact"]["shards"][shard_id]["assignments"]
    return next(item for item in assignments if item["word"] == word)


def test_review_threshold_prevents_forced_assignment(ready_service) -> None:
    state = ready_service.workspace.load_state()
    assignment = _by_word(state, "shard-001", "打开空调")
    assert assignment["status"] == "review"
    assert assignment["responsible_zone"] is None
    assert assignment["top_candidate"] in assignment["participating_zones"]
    assert assignment["score_gap"] < assignment["review_margin"]
    assert len(assignment["component_scores"]) >= 2
    assert assignment["pair_delay_details"]
    pair = assignment["pair_delay_details"][0]
    assert set(pair) >= {
        "zones",
        "observed_delay_ms",
        "expected_delay_ms",
        "distance_difference_m",
        "error_ms",
        "compatibility",
    }


def test_unique_assignment_and_component_scores(ready_service) -> None:
    state = ready_service.workspace.load_state()
    assignment = _by_word(state, "shard-002", "调高音量")
    assert assignment["status"] == "assigned"
    assert assignment["responsible_zone"] == "driver"
    assert assignment["score_gap"] >= assignment["review_margin"]
    for score in assignment["component_scores"].values():
        assert set(score) >= {
            "priority",
            "priority_score",
            "hit_strength",
            "tdoa_score",
            "total",
            "pair_scores",
        }
    core = build_core_result(state)
    assert core["assignments"]["shard-002"]
    assert core["tdoa"]["shard-002"][0]["pair_delay_details"]
