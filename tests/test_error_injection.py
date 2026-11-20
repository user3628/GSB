import copy
import json
import os

import pytest

from layout_order.config import DEFAULT_CONFIG
from layout_order.engine import PipelineEngine
from layout_order.errors import LayoutError
from layout_order.generator import generate_documents
from layout_order.stages import normalize as normalize_stage


@pytest.fixture()
def documents():
    return generate_documents()


@pytest.fixture()
def engine(tmp_path):
    return PipelineEngine(str(tmp_path))


def test_fixture_catalog_is_consistent():
    path = os.path.join(os.path.dirname(__file__), "..", "fixtures", "error_injections.json")
    catalog = json.loads(open(path, encoding="utf-8").read())
    assert len(catalog["cases"]) >= 7
    assert {case["expect_code"] for case in catalog["cases"]} >= {
        "E_BOX_INVALID", "E_COLUMN_UNASSIGNED", "E_INVALID_STAGE", "E_STALE_CHECKPOINT"}


def test_negative_coordinate(engine, documents):
    bad = copy.deepcopy(documents[0])
    bad["boxes"][0]["bbox_norm"][0] = -0.05
    with pytest.raises(LayoutError) as exc:
        engine.run_document(bad)
    assert exc.value.code == "E_BOX_INVALID"
    assert exc.value.details["box_id"] == bad["boxes"][0]["box_id"]


def test_empty_body_boxes(engine, documents):
    bad = copy.deepcopy(documents[0])
    bad["doc_id"] = "doc_empty_body"
    # 仅保留页眉页脚（role 不会是 body），正文框数量为零
    bad["boxes"] = [box for box in bad["boxes"]
                    if box["box_id"] in (bad["reference_order"][0], bad["reference_order"][-1])]
    bad["reference_order"] = [box["box_id"] for box in bad["boxes"]]
    engine.store.save_input(bad)
    normalized = normalize_stage.run(bad, DEFAULT_CONFIG)
    engine.store.publish(bad["doc_id"], "normalize", "in",
                         DEFAULT_CONFIG["config_version"], "fp", normalized)
    from layout_order.stages import column as column_stage
    with pytest.raises(LayoutError) as exc:
        column_stage.run(normalized, DEFAULT_CONFIG)
    assert exc.value.code == "E_COLUMN_UNASSIGNED"
    assert exc.value.details["doc_id"] == "doc_empty_body"


def test_unknown_stage_name(engine, documents):
    engine.store.save_input(documents[0])
    with pytest.raises(LayoutError) as exc:
        engine.store.publish(documents[0]["doc_id"], "no_such_stage", "in",
                             DEFAULT_CONFIG["config_version"], "fp", {})
    assert exc.value.code == "E_INVALID_STAGE"


def test_duplicate_box_id(engine, documents):
    bad = copy.deepcopy(documents[0])
    bad["boxes"][1]["box_id"] = bad["boxes"][0]["box_id"]
    with pytest.raises(LayoutError) as exc:
        engine.run_document(bad)
    assert exc.value.code == "E_BOX_INVALID"


def test_incremental_mismatch_is_detectable(tmp_path, documents, monkeypatch):
    engine = PipelineEngine(str(tmp_path))
    target = copy.deepcopy(next(doc for doc in documents
                                if doc["doc_id"] == "doc_single_zh"))
    engine.run_document(target)
    # 破坏隔离基线：让临时工作区构建结果与主工作区不一致，验证比对能发现
    original_build = engine._build_isolated

    def tampered_baseline(document):
        baseline = original_build(document)
        baseline["digests"]["summary"] = "deadbeef"
        return baseline

    monkeypatch.setattr(engine, "_build_isolated", tampered_baseline)
    with pytest.raises(LayoutError) as exc:
        engine.update_document(target)
    assert exc.value.code == "E_INCREMENTAL_MISMATCH"
    assert "summary" in exc.value.details["mismatch_stages"]


def test_rejected_publish_keeps_state(engine, documents):
    target = documents[0]
    engine.store.save_input(target)
    product = normalize_stage.run(target, DEFAULT_CONFIG)
    engine.store.publish(target["doc_id"], "normalize", "in",
                         DEFAULT_CONFIG["config_version"], "fp-a", product)
    with pytest.raises(LayoutError):
        engine.store.publish(target["doc_id"], "normalize", "in",
                             DEFAULT_CONFIG["config_version"], "fp-b", product)
    record = engine.store.load_stage(target["doc_id"], "normalize")
    assert record["stage_fingerprint"] == "fp-a"


def test_idempotent_republish_same_fingerprint(engine, documents):
    target = documents[0]
    engine.store.save_input(target)
    product = normalize_stage.run(target, DEFAULT_CONFIG)
    first = engine.store.publish(target["doc_id"], "normalize", "in",
                                 DEFAULT_CONFIG["config_version"], "fp-x", product)
    second = engine.store.publish(target["doc_id"], "normalize", "in",
                                  DEFAULT_CONFIG["config_version"], "fp-x", product)
    assert first["seq"] == second["seq"] == 1
