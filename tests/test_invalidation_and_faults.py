from __future__ import annotations

import copy
from pathlib import Path

import pytest

from mmbackfill.canonical import read_json, read_jsonl
from mmbackfill.errors import EngineError
from tests.conftest import make_engine


VALID_IDS = ["txt-a", "txt-b", "txt-c", "txt-d", "txt-e"]


def test_invalid_layouts_have_stable_codes_and_do_not_mutate_state(tmp_path: Path, demo_dataset: Path) -> None:
    engine = make_engine(tmp_path / "workspace", demo_dataset)
    original_layout = engine.workspace.layout_path.read_bytes()
    for filename, code in (
        ("layout_zero_capacity.json", "E_INVALID_LAYOUT"),
        ("layout_layer_limit_below_capacity.json", "E_INVALID_LAYOUT"),
        ("layout_dimension_mismatch.json", "E_DIM_MISMATCH"),
        ("layout_nan_fusion.json", "E_NUMERIC"),
    ):
        with pytest.raises(EngineError) as caught:
            engine.install_layout(read_json(demo_dataset / "faults" / filename))
        assert caught.value.code == code
    assert engine.workspace.layout_path.read_bytes() == original_layout
    assert engine.status()["last_batch_seq"] == 0


def test_full_shard_unknown_ref_and_bad_vectors_do_not_commit(tmp_path: Path, demo_dataset: Path) -> None:
    full = make_engine(
        tmp_path / "full",
        demo_dataset,
        layout_path=demo_dataset / "faults" / "layout_full_no_standby.json",
        texts_path=demo_dataset / "faults" / "texts_full_no_standby.jsonl",
    )
    with pytest.raises(EngineError) as full_error:
        full.submit_batch(batch_id="full", text_ids=["txt-full-1", "txt-full-2", "txt-full-3"], expected_sequence=0, order="id")
    assert full_error.value.code == "E_SHARD_FULL"
    assert {item["text_id"] for item in full_error.value.entries} == {"txt-full-3"}
    assert full.status()["last_batch_seq"] == 0

    engine = make_engine(tmp_path / "bad-vectors", demo_dataset)
    with pytest.raises(EngineError) as unknown:
        engine.submit_batch(batch_id="unknown", text_ids=["txt-unknown"], expected_sequence=0, order="id")
    assert unknown.value.code == "E_UNKNOWN_REF"
    assert engine.status()["last_batch_seq"] == 0

    for filename, code in (("vectors_dimension_mismatch.json", "E_DIM_MISMATCH"), ("vectors_nan.json", "E_NUMERIC")):
        overrides = read_json(demo_dataset / "faults" / filename)
        with pytest.raises(EngineError) as caught:
            engine.submit_batch(batch_id=filename, text_ids=["txt-a"], expected_sequence=0, order="id", vector_overrides=overrides)
        assert caught.value.code == code
        assert caught.value.entries[0]["text_id"] == "txt-a"
    assert engine.status()["last_batch_seq"] == 0

    duplicate_engine = make_engine(tmp_path / "duplicate", demo_dataset)
    duplicate_vector = [1.0] * 280
    with pytest.raises(EngineError) as duplicate:
        duplicate_engine.submit_batch(
            batch_id="duplicate",
            text_ids=["txt-a", "txt-b"],
            expected_sequence=0,
            order="id",
            vector_overrides={"txt-a": duplicate_vector, "txt-b": duplicate_vector},
        )
    assert duplicate.value.code == "E_DUPLICATE_ROUTE"
    assert {item["text_id"] for item in duplicate.value.conflicts} == {"txt-a", "txt-b"}
    assert duplicate_engine.status()["last_batch_seq"] == 0


def test_sample_change_and_fusion_change_publish_invalidations(tmp_path: Path, demo_dataset: Path) -> None:
    engine = make_engine(tmp_path / "workspace", demo_dataset)
    engine.submit_batch(batch_id="base", text_ids=VALID_IDS, expected_sequence=0, order="id")
    old_index = copy.deepcopy(engine.index())

    texts = read_jsonl(demo_dataset / "texts.jsonl")
    changed = [dict(row) for row in texts]
    changed[0]["text"] = "完全替换 RED-BLUE changed content"
    engine.import_texts(changed)
    result = engine.submit_batch(batch_id="changed", text_ids=["txt-a"], expected_sequence=1, order="id")
    event = result["invalidated"]
    assert event is not None
    assert event["stage"] == "vector_build"
    assert event["reason"] == "sample_content_changed"
    assert "txt-a" in event["affected_entries"]
    assert event["invalidated_shards"]
    assert event["new_fingerprints"]["vector_build"] != event["old_fingerprints"]["vector_build"]
    new_index = engine.index()
    old_entry = old_index["shards"][0]["entries"][0]
    new_entry = new_index["shards"][0]["entries"][0]
    assert old_entry["text_id"] == new_entry["text_id"] == "txt-a"
    assert old_entry["vector_digest"] != new_entry["vector_digest"]

    target_changed = [dict(row) for row in changed]
    target_changed[1]["target_shard"] = "s1"
    engine.import_texts(target_changed)
    target_result = engine.submit_batch(batch_id="target-changed", text_ids=["txt-b"], expected_sequence=2, order="id")
    target_event = target_result["invalidated"]
    assert target_event is not None
    assert target_event["stage"] == "backfill_planning"
    assert target_event["reason"] == "sample_target_shard_changed"
    assert target_event["old_fingerprints"]["vector_build"] == target_event["new_fingerprints"]["vector_build"]
    assert "txt-b" in target_event["affected_entries"]

    new_layout = read_json(demo_dataset / "layout.json")
    new_layout["layout_id"] = "fusion-v2"
    new_layout["vector"]["fusion"]["image_weight"] = 0.70
    new_layout["vector"]["fusion"]["text_weight"] = 0.30
    install_result = engine.install_layout(new_layout)
    fusion_event = install_result["invalidated"]
    assert fusion_event["stage"] == "vector_build"
    assert fusion_event["reason"] == "layout_config_changed"
    assert fusion_event["old_fingerprints"]["vector_build"] != fusion_event["new_fingerprints"]["vector_build"]
    assert fusion_event["reused_shards"]
    assert set(fusion_event["reused_shards"]).issubset(set(fusion_event["invalidated_shards"])) is False
    ledger = engine.invalidations()
    assert len(ledger["events"]) >= 3
