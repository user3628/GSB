import json
import os

import pytest

from layout_order.config import DEFAULT_CONFIG, merge_config
from layout_order.engine import PipelineEngine
from layout_order.errors import LayoutError
from layout_order.generator import generate_documents
from layout_order.hashing import canonical_bytes
from layout_order.stages import normalize as normalize_stage


@pytest.fixture()
def documents():
    return generate_documents()


def _run_full(tmp_path, documents, config=None):
    engine = PipelineEngine(str(tmp_path), config)
    reports = engine.run(documents)
    return engine, reports


def test_generator_deterministic_and_covers_scenarios(documents):
    assert len(documents) >= 4
    ids = {doc["doc_id"] for doc in documents}
    assert {"doc_single_zh", "doc_double_en", "doc_double_mixed",
            "doc_single_mixed_fragments"} <= ids
    again = generate_documents()
    assert canonical_bytes(again) == canonical_bytes(documents)
    for doc in documents:
        assert doc["reference_order"]
        assert sorted(doc["reference_order"]) == sorted(b["box_id"] for b in doc["boxes"])


def test_full_run_completes_four_stages(tmp_path, documents):
    engine, reports = _run_full(tmp_path, documents)
    assert all(report["status"] == "completed" for report in reports)
    for doc in documents:
        status = engine.document_status(doc["doc_id"])
        assert all(status["stages"][stage]["present"] for stage in
                   ("normalize", "column", "order", "summary"))
        result = engine.store.load_stage(doc["doc_id"], "summary")["product"]
        assert len(result["result_digest"]) == 64


def test_byte_identical_across_runs(tmp_path, documents):
    engine_a, _ = _run_full(tmp_path / "a", documents)
    engine_b, _ = _run_full(tmp_path / "b", documents)
    for doc in documents:
        for stage in ("normalize", "column", "order", "summary"):
            pa = engine_a.store.load_stage(doc["doc_id"], stage)["product"]
            pb = engine_b.store.load_stage(doc["doc_id"], stage)["product"]
            assert canonical_bytes(pa) == canonical_bytes(pb), (doc["doc_id"], stage)


def test_double_column_and_spanning_detection(tmp_path, documents):
    engine, _ = _run_full(tmp_path, documents)
    column = engine.store.load_stage("doc_double_en", "column")["product"]
    assert column["column_count"] == 2
    assert column["evidence"]["voting_box_count"] >= 4
    mixed = engine.store.load_stage("doc_double_mixed", "column")["product"]
    assert mixed["column_count"] == 2
    assert "d3-b02" in mixed["spanning_box_ids"]
    assert "d3-s02" in mixed["spanning_box_ids"]
    single = engine.store.load_stage("doc_single_zh", "column")["product"]
    assert single["column_count"] == 1


def test_columns_are_non_overlapping_and_cover_width(tmp_path, documents):
    engine, _ = _run_full(tmp_path, documents)
    for doc in documents:
        column = engine.store.load_stage(doc["doc_id"], "column")["product"]
        intervals = column["columns"]
        effective = column["effective_layout"]
        assert intervals[0]["x0"] == pytest.approx(effective["x0"])
        assert intervals[-1]["x1"] == pytest.approx(effective["x1"])
        for prev, nxt in zip(intervals, intervals[1:]):
            assert prev["x1"] <= nxt["x0"]


def test_fragment_merging(tmp_path, documents):
    engine, _ = _run_full(tmp_path, documents)
    normalized = engine.store.load_stage("doc_single_mixed_fragments", "normalize")["product"]
    ids = {box["box_id"] for box in normalized["boxes"]}
    assert "d4-b04a" not in ids and "d4-b04b" not in ids
    merged = [box for box in normalized["boxes"] if box["merged"]]
    assert len(merged) == 2
    member_sets = [set(box["source_box_ids"]) for box in merged]
    assert {"d4-b04a", "d4-b04b"} in member_sets
    assert {"d4-b05a", "d4-b05b"} in member_sets
    assert normalized["stats"]["merged_group_count"] == 2


def test_header_footer_classification(tmp_path, documents):
    engine, _ = _run_full(tmp_path, documents)
    for doc in documents:
        normalized = engine.store.load_stage(doc["doc_id"], "normalize")["product"]
        roles = {box["box_id"]: box["role"] for box in normalized["boxes"]}
        header = next(box_id for box_id in roles if box_id.endswith("b01"))
        assert roles[header] == "header"
        footer_source = doc["reference_order"][-1]
        footer_block = next(
            box_id for box_id in roles
            if box_id == footer_source or any(
                footer_source == sid
                for sid in next(b["source_box_ids"] for b in normalized["boxes"]
                                if b["box_id"] == box_id)
            )
        )
        assert roles[footer_block] == "footer"


def test_order_sequence_and_quality(tmp_path, documents):
    engine, _ = _run_full(tmp_path, documents)
    for doc in documents:
        order = engine.store.load_stage(doc["doc_id"], "order")["product"]
        result = engine.store.load_stage(doc["doc_id"], "summary")["product"]
        sequence_ids = [item["block_id"] for item in order["block_sequence"]]
        assert len(sequence_ids) == len(set(sequence_ids))
        quality = result["order_quality"]
        assert quality["reference_available"] is True
        assert quality["inversion_count"] == 0
        assert quality["order_accuracy"] == 1.0
        assert 0.0 <= order["confidence"]["overall"] <= 1.0


def test_order_rule_left_column_before_right(tmp_path, documents):
    engine, _ = _run_full(tmp_path, documents)
    order = engine.store.load_stage("doc_double_en", "order")["product"]
    ids = [item["block_id"] for item in order["block_sequence"]]
    assert ids.index("d2-l1") < ids.index("d2-r1")
    assert ids.index("d2-l4") < ids.index("d2-r1")
    assert ids[0] == "d2-b01" and ids[-1] == "d2-b10"


def test_invalid_box_codes(tmp_path, documents):
    bad = json.loads(json.dumps(documents[0]))
    bad["doc_id"] = "bad_out_of_range"
    bad["boxes"][1]["bbox_norm"][2] = 1.2
    engine = PipelineEngine(str(tmp_path))
    with pytest.raises(LayoutError) as exc:
        engine.run_document(bad)
    assert exc.value.code == "E_BOX_INVALID"
    assert exc.value.details["doc_id"] == "bad_out_of_range"
    assert exc.value.details["box_id"] == bad["boxes"][1]["box_id"]

    zero = json.loads(json.dumps(documents[0]))
    zero["doc_id"] = "bad_zero_area"
    zero["boxes"][0]["bbox_norm"][2] = zero["boxes"][0]["bbox_norm"][0]
    with pytest.raises(LayoutError) as exc2:
        engine.run_document(zero)
    assert exc2.value.code == "E_BOX_INVALID"
    assert exc2.value.details["box_id"] == zero["boxes"][0]["box_id"]


def test_stop_and_resume_byte_identical(tmp_path, documents):
    ws_resume = tmp_path / "resume"
    ws_full = tmp_path / "full"

    engine = PipelineEngine(str(ws_resume))
    target = next(doc for doc in documents if doc["doc_id"] == "doc_double_mixed")
    stop_after_column = lambda: engine.store.load_stage(
        target["doc_id"], "column") is not None
    report = engine.run_document(target, stop_after_column)
    assert report["status"] == "stopped"
    assert report["completed_through"] == "column"
    assert engine.store.load_stage(target["doc_id"], "summary") is None

    final_report = engine.run_document(target)
    assert final_report["status"] == "completed"

    full_engine = PipelineEngine(str(ws_full))
    full_engine.run_document(target)
    for stage in ("normalize", "column", "order", "summary"):
        resumed = engine.store.load_stage(target["doc_id"], stage)["product"]
        full = full_engine.store.load_stage(target["doc_id"], stage)["product"]
        assert canonical_bytes(resumed) == canonical_bytes(full)


def test_version_change_invalidation_propagation(tmp_path, documents):
    ws = tmp_path / "inc"
    engine = PipelineEngine(str(ws))
    # 选择含碎片合并的文档：合并阈值变化必然改变规范化产物
    target = next(doc for doc in documents if doc["doc_id"] == "doc_single_mixed_fragments")
    engine.run_document(target)
    original_fps = engine.document_status(target["doc_id"])["stage_fingerprints"]

    new_config = merge_config({"normalize": {"merge_gap_px": 6.0}})
    engine2 = PipelineEngine(str(ws), new_config)
    report = engine2.run_document(target)
    assert report["status"] == "completed"
    events = engine2.store.read_invalidation_log()
    assert events and events[-1]["earliest_invalid_stage"] == "normalize"
    assert set(events[-1]["removed_stages"]) == {"normalize", "column", "order", "summary"}
    new_fps = engine2.document_status(target["doc_id"])["stage_fingerprints"]
    assert all(new_fps[s] != original_fps[s] for s in original_fps)


def test_upstream_change_only_rebuilds_affected_chain(tmp_path, documents):
    ws = tmp_path / "inc2"
    engine = PipelineEngine(str(ws))
    target = next(doc for doc in documents if doc["doc_id"] == "doc_double_en")
    engine.run_document(target)
    before = {stage: engine.store.load_stage(target["doc_id"], stage)["stage_fingerprint"]
              for stage in ("normalize", "column", "order", "summary")}

    # 仅改变顺序置信度权重：最早受影响阶段为 order，normalize/column 指纹保持不变
    new_config = merge_config({"order": {"confidence_weights": {
        "vertical_gap": 0.8, "column_membership": 0.2}}})
    engine2 = PipelineEngine(str(ws), new_config)
    engine2.run_document(target)
    after = {stage: engine2.store.load_stage(target["doc_id"], stage)["stage_fingerprint"]
             for stage in ("normalize", "column", "order", "summary")}
    assert after["normalize"] == before["normalize"]
    assert after["column"] == before["column"]
    assert after["order"] != before["order"]
    assert after["summary"] != before["summary"]
    events = engine2.store.read_invalidation_log()
    assert events[-1]["earliest_invalid_stage"] == "order"


def test_incremental_matches_full_build(tmp_path, documents):
    ws = tmp_path / "inc3"
    engine = PipelineEngine(str(ws))
    target = next(doc for doc in documents if doc["doc_id"] == "doc_single_zh")
    engine.run_document(target)
    changed = json.loads(json.dumps(target))
    changed["boxes"][2]["text"] = changed["boxes"][2]["text"] + "（增量更新内容）"
    report = engine.update_document(changed)
    assert report["verified_against_full_build"] is True


def test_stale_and_invalid_stage_publish(tmp_path, documents):
    engine = PipelineEngine(str(tmp_path))
    target = documents[0]
    engine.store.save_input(target)
    normalized = normalize_stage.run(target, DEFAULT_CONFIG)
    fp = "a" * 16
    # 跳过 normalize 直接发布 column -> E_INVALID_STAGE
    with pytest.raises(LayoutError) as exc:
        engine.store.publish(target["doc_id"], "column", fp,
                             DEFAULT_CONFIG["config_version"], fp, normalized)
    assert exc.value.code == "E_INVALID_STAGE"

    engine.store.publish(target["doc_id"], "normalize", fp,
                         DEFAULT_CONFIG["config_version"], fp, normalized)
    # 重复发布不同指纹 -> E_STALE_CHECKPOINT，已有状态不变
    with pytest.raises(LayoutError) as exc2:
        engine.store.publish(target["doc_id"], "normalize", fp,
                             DEFAULT_CONFIG["config_version"], "b" * 16, normalized)
    assert exc2.value.code == "E_STALE_CHECKPOINT"
    kept = engine.store.load_stage(target["doc_id"], "normalize")
    assert kept["stage_fingerprint"] == fp
    assert kept["seq"] == 1

    # 跨文档错位发布
    with pytest.raises(LayoutError) as exc3:
        engine.store.publish(target["doc_id"], "normalize", fp,
                             DEFAULT_CONFIG["config_version"], fp, normalized,
                             claimed_doc_id="other_doc")
    assert exc3.value.code == "E_STALE_CHECKPOINT"


def test_missing_reference_does_not_infer_quality(tmp_path, documents):
    target = json.loads(json.dumps(documents[0]))
    target["doc_id"] = "doc_no_reference"
    del target["reference_order"]
    engine = PipelineEngine(str(tmp_path))
    engine.run_document(target)
    quality = engine.store.load_stage(target["doc_id"], "summary")["product"]["order_quality"]
    assert quality["reference_available"] is False
    assert quality["order_accuracy"] is None


def test_atomic_write_leaves_no_temp_files(tmp_path, documents):
    engine, _ = _run_full(tmp_path / "atomic", documents)
    for root, _, files in os.walk(engine.workspace):
        assert not any(name.endswith(".tmp") for name in files)
