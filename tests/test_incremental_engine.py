from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest

from dep_engine import Engine, EngineError
from dep_engine.canonical import atomic_write_json, canonical_bytes
from dep_engine.generator import DEMO_CONFIG


def test_stop_resume_matches_full_and_reuses_everything(engine: Engine):
    stopped = engine.build(stop_after_shards=1)
    assert stopped["status"] == "stopped"
    assert stopped["processed_shards"] == 1
    assert stopped["checkpoint"]["last_sequence"] == 4

    resumed = engine.build()
    assert resumed["status"] == "complete"
    assert len(resumed["computed"]) == 8
    assert len(resumed["reused"]) == 4
    assert resumed["checkpoint"]["last_sequence"] == 12

    verification = engine.verify_against_full()
    assert verification["full_match"] is True

    once_more = engine.build()
    assert once_more["status"] == "complete"
    assert once_more["computed"] == []
    assert len(once_more["reused"]) == 12
    assert once_more["checkpoint"]["last_sequence"] == 12

    state = engine.store().scan()
    assert [record["sequence"] for record in state.records] == list(range(1, 13))
    for shard_index in range(3):
        assert state.last_shard_sequence[shard_index] == 4


@pytest.mark.parametrize(
    ("field", "value", "earliest"),
    [
        ("score_weights", {"shift": 0.63, "left_reduce": 0.91, "right_reduce": 0.78}, "decoding"),
        ("max_dependency_distance", 8, "validation"),
        ("smoothing_strength", 0.09, "decoding"),
        ("confidence_threshold", 0.03, "decoding"),
        ("unique_root", False, "validation"),
    ],
)
def test_config_change_selects_exact_earliest_stage(
    engine: Engine, field: str, value, earliest: str
):
    engine.build()
    changed = copy.deepcopy(DEMO_CONFIG)
    changed[field] = value
    result = engine.update_config(changed, build=False)
    plan = result["plan"]
    assert plan["global_earliest_invalid_stage"] == earliest
    for shard in plan["shards"]:
        assert shard["earliest_invalid_stage"] == earliest
        assert earliest not in shard["reused_stages"]
        assert shard["stages"][earliest]["reason"] == "config_changed"
        assert field in shard["stages"][earliest]["changed_components"]

    built = engine.build()
    computed_stages = {item["stage"] for item in built["computed"]}
    assert earliest in computed_stages
    if earliest == "validation":
        assert "featureization" not in computed_stages
        assert "decoding" not in computed_stages
    if earliest == "decoding":
        assert "featureization" not in computed_stages
    verification = engine.verify_against_full()
    assert verification["full_match"] is True


def test_content_replacement_invalidates_only_affected_shard(engine: Engine):
    engine.build()
    replacement = "新的 sentence 内容 。\n第二行 text 包含 API 。\n"
    result = engine.replace_shard(1, replacement, build=False)
    plan = result["plan"]
    earliest = {
        shard["shard_index"]: shard["earliest_invalid_stage"]
        for shard in plan["shards"]
    }
    assert earliest == {0: None, 1: "featureization", 2: None}
    assert plan["shards"][1]["stages"]["featureization"]["reason"] == "input_content_changed"

    built = engine.build()
    computed_shards = {item["shard_index"] for item in built["computed"]}
    assert computed_shards == {1}
    assert len(built["computed"]) == 4
    assert all(
        item["shard_index"] in {0, 2} for item in built["reused"]
    )
    verification = engine.verify_against_full()
    assert verification["full_match"] is True


def test_incremental_mismatch_reports_first_shard_and_token(engine: Engine, tmp_path: Path):
    engine.build()
    reference = tmp_path / "reference-views"
    shutil.copytree(engine.views_dir, reference)
    trees = json.loads((reference / "trees.json").read_text(encoding="utf-8"))
    trees["sentences"][0]["heads"][0] = 999
    atomic_write_json(reference / "trees.json", trees)
    with pytest.raises(EngineError) as caught:
        engine.compare_views(reference)
    assert caught.value.code == "E_INCREMENTAL_MISMATCH"
    first_diff = caught.value.details["first_diff"]
    assert first_diff["artifact"] == "trees"
    assert first_diff["shard_index"] == 0
    assert first_diff["token_index"] == 0
    assert first_diff["field"] == "head_index"


def test_public_queries_and_out_of_range_error(engine: Engine):
    engine.build()
    trees = engine.get_trees()
    diagnostics = engine.get_diagnostics()
    summary = engine.get_summary()
    fingerprints = engine.get_fingerprints()
    assert len(trees["sentences"]) == 21
    assert len(diagnostics["diagnostics"]) == 21
    assert summary["sentence_count"] == 21
    assert summary["token_count"] == 207
    assert summary["error_counts"] == {
        "E_EMPTY_SENTENCE": 1,
        "E_TREE_INVALID": 2,
    }
    assert summary["constraint_violation_counts"] == {"MAX_DEPENDENCY_DISTANCE": 5}
    assert len(fingerprints["shards"]) == 3
    assert fingerprints["invalidation"]["all_reusable"] is True
    with pytest.raises(EngineError) as caught:
        engine.get_trees(shard_index=99)
    assert caught.value.code == "E_INVALID_STAGE"
    assert "featureization" in caught.value.details["valid_stages"]


def test_sample_contains_required_mixed_and_invalid_cases(engine: Engine):
    engine.build()
    diagnostics = engine.get_diagnostics()["diagnostics"]
    errors = [item for item in diagnostics if item["status"] != "ok"]
    assert any(item["code"] == "E_EMPTY_SENTENCE" for item in errors)
    assert any(item["code"] == "E_TREE_INVALID" for item in errors)
    assert any(
        violation["type"] == "MAX_DEPENDENCY_DISTANCE"
        for item in errors
        for violation in item["violations"]
    )
    manifest = engine.load_manifest()
    assert manifest.shard_count == 3
    assert manifest.raw["total_sentences"] == 21
    assert manifest.raw["total_tokens"] == 207
    for entry in manifest.shards:
        sentences, tokens = manifest.measured_counts(entry.index)
        assert sentences == entry.sentence_count
        assert tokens == entry.token_count
    text = " ".join(manifest.shard_text(index) for index in range(3))
    assert "Python" in text and "English" in text and "Codex" in text


def test_fixed_demo_report_is_repeatable(workspace: Path):
    from dep_engine.demo import run_demo

    first = run_demo(workspace)
    first_bytes = canonical_bytes(first)
    second = run_demo(workspace)
    assert canonical_bytes(second) == first_bytes
    assert first["consistency"]["incremental_equals_full"] is True
    expected = json.loads(
        (Path(__file__).resolve().parents[1] / "expected" / "demo_report.json").read_text(
            encoding="utf-8"
        )
    )
    assert first == expected
