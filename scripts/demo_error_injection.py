#!/usr/bin/env python
"""按 fixtures/error_injections.json 的意图执行错误注入并核对业务错误码。"""

from __future__ import annotations

import copy
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from layout_order.config import DEFAULT_CONFIG
from layout_order.engine import PipelineEngine
from layout_order.errors import LayoutError
from layout_order.generator import generate_documents
from layout_order.stages import column as column_stage
from layout_order.stages import normalize as normalize_stage


def main() -> int:
    docs = {doc["doc_id"]: doc for doc in generate_documents()}
    failures = 0

    def expect(code: str, fn, label: str) -> None:
        nonlocal failures
        try:
            fn()
        except LayoutError as exc:
            status = "PASS" if exc.code == code else "FAIL"
            if status == "FAIL":
                failures += 1
            print(f"[{status}] {label}: got={exc.code} expect={code} "
                  f"details={json.dumps(exc.details, ensure_ascii=False)}")
            return
        failures += 1
        print(f"[FAIL] {label}: 未抛出错误，期望 {code}")

    with tempfile.TemporaryDirectory() as workspace:
        engine = PipelineEngine(workspace)

        bad = copy.deepcopy(docs["doc_single_zh"])
        bad["doc_id"] = "inj_out_of_range"
        bad["boxes"][1]["bbox_norm"][2] = 1.2
        expect("E_BOX_INVALID", lambda: engine.run_document(bad), "越界坐标")

        bad = copy.deepcopy(docs["doc_single_zh"])
        bad["doc_id"] = "inj_zero_area"
        bad["boxes"][2]["bbox_norm"][2] = bad["boxes"][2]["bbox_norm"][0]
        expect("E_BOX_INVALID", lambda: engine.run_document(bad), "零面积框")

        bad = copy.deepcopy(docs["doc_single_zh"])
        bad["doc_id"] = "inj_empty_body"
        bad["boxes"] = [box for box in bad["boxes"]
                        if box["box_id"] in (bad["reference_order"][0],
                                             bad["reference_order"][-1])]
        bad["reference_order"] = [box["box_id"] for box in bad["boxes"]]
        normalized = normalize_stage.run(bad, DEFAULT_CONFIG)
        expect("E_COLUMN_UNASSIGNED",
               lambda: column_stage.run(normalized, DEFAULT_CONFIG), "空正文框")

        target = docs["doc_single_zh"]
        engine.store.save_input(target)
        product = normalize_stage.run(target, DEFAULT_CONFIG)
        expect("E_INVALID_STAGE",
               lambda: engine.store.publish(
                   target["doc_id"], "column", "in",
                   DEFAULT_CONFIG["config_version"], "fp", product),
               "非法阶段跳转")

        engine.store.publish(target["doc_id"], "normalize", "in",
                             DEFAULT_CONFIG["config_version"], "fp-a", product)
        expect("E_STALE_CHECKPOINT",
               lambda: engine.store.publish(
                   target["doc_id"], "normalize", "in",
                   DEFAULT_CONFIG["config_version"], "fp-b", product),
               "同序号重复发布不同指纹")
        expect("E_STALE_CHECKPOINT",
               lambda: engine.store.publish(
                   target["doc_id"], "normalize", "in",
                   DEFAULT_CONFIG["config_version"], "fp-a", product,
                   claimed_doc_id="other_doc"),
               "跨文档错位发布")
        expect("E_INVALID_STAGE",
               lambda: engine.store.publish(
                   target["doc_id"], "unknown_stage", "in",
                   DEFAULT_CONFIG["config_version"], "fp", product),
               "未知阶段名")

    print("ERROR_INJECTION_RESULT:", "ALL_PASS" if failures == 0 else f"{failures}_FAILED")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
