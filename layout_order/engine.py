"""四阶段管线编排、版本指纹失效传播与增量/全量一致性校验。"""

from __future__ import annotations

import os
from copy import deepcopy
from typing import Any, Callable

from .config import (
    DEFAULT_CONFIG,
    STAGES,
    STAGE_COLUMN,
    STAGE_NORMALIZE,
    STAGE_ORDER,
    STAGE_SUMMARY,
    stage_config,
    validate_config,
)
from .errors import LayoutError
from .hashing import canonical_bytes, digest
from .stages import column as column_stage
from .stages import normalize as normalize_stage
from .stages import order as order_stage
from .stages import summary as summary_stage
from .store import CheckpointStore

StopCheck = Callable[[], bool]


class PipelineEngine:
    def __init__(self, workspace: str, config: dict[str, Any] | None = None):
        self.config = config or deepcopy(DEFAULT_CONFIG)
        validate_config(self.config)
        self.store = CheckpointStore(store_workspace := workspace)
        self.workspace = os.path.abspath(store_workspace)

    # ---------- 指纹 ----------
    def _stage_fingerprint(self, stage: str, input_fp: str) -> str:
        return digest(
            {
                "input_fingerprint": input_fp,
                "stage": stage,
                "config": stage_config(self.config, stage),
            }
        )

    def _expected_fingerprints(self, document: dict[str, Any]) -> dict[str, str]:
        fp1_in = normalize_stage.input_fingerprint(document)
        fp1 = self._stage_fingerprint(STAGE_NORMALIZE, fp1_in)
        return {"_normalize_input": fp1_in, STAGE_NORMALIZE: fp1}

    # ---------- 单文档运行（可恢复/增量） ----------
    def run_document(
        self,
        document: dict[str, Any],
        stop_check: StopCheck | None = None,
    ) -> dict[str, Any]:
        doc_id = document["doc_id"]
        stop_check = stop_check or (lambda: False)
        with self.store.lock_document(doc_id):
            self.store.save_input(document)
            return self._run_locked(document, stop_check)

    def _run_locked(self, document: dict[str, Any], stop_check: StopCheck) -> dict[str, Any]:
        doc_id = document["doc_id"]
        products: dict[str, Any] = {}

        # ---- 阶段一 ----
        fp1_in = normalize_stage.input_fingerprint(document)
        fp1 = self._stage_fingerprint(STAGE_NORMALIZE, fp1_in)
        normalized = self._reuse_or_rebuild(
            doc_id, STAGE_NORMALIZE, fp1_in, fp1,
            lambda: normalize_stage.run(document, self.config),
            products,
        )
        if stop_check():
            return self._stop_report(doc_id, completed_through=STAGE_NORMALIZE)

        # ---- 阶段二 ----
        fp2_in = column_stage.input_fingerprint(normalized)
        fp2 = self._stage_fingerprint(STAGE_COLUMN, fp2_in)
        column_result = self._reuse_or_rebuild(
            doc_id, STAGE_COLUMN, fp2_in, fp2,
            lambda: column_stage.run(normalized, self.config),
            products,
        )
        if stop_check():
            return self._stop_report(doc_id, completed_through=STAGE_COLUMN)

        # ---- 阶段三 ----
        fp3_in = order_stage.input_fingerprint(normalized, column_result)
        fp3 = self._stage_fingerprint(STAGE_ORDER, fp3_in)
        order_result = self._reuse_or_rebuild(
            doc_id, STAGE_ORDER, fp3_in, fp3,
            lambda: order_stage.run(normalized, column_result, self.config),
            products,
        )
        if stop_check():
            return self._stop_report(doc_id, completed_through=STAGE_ORDER)

        # ---- 阶段四 ----
        fp4_in = summary_stage.input_fingerprint(
            document, normalized, column_result, order_result
        )
        fp4 = self._stage_fingerprint(STAGE_SUMMARY, fp4_in)
        result = self._reuse_or_rebuild(
            doc_id, STAGE_SUMMARY, fp4_in, fp4,
            lambda: summary_stage.run(
                document, normalized, column_result, order_result, self.config
            ),
            products,
        )
        self.store.export_result(doc_id, result)
        return {"doc_id": doc_id, "status": "completed", "result_digest": result["result_digest"]}

    def _reuse_or_rebuild(
        self,
        doc_id: str,
        stage: str,
        input_fp: str,
        expected_fp: str,
        producer: Callable[[], dict[str, Any]],
        products: dict[str, Any],
    ) -> dict[str, Any]:
        existing = self.store.load_stage(doc_id, stage)
        if existing is not None and existing["stage_fingerprint"] == expected_fp:
            products[stage] = existing["product"]
            return existing["product"]
        if existing is not None:
            removed = self.store.invalidate_from_locked(
                doc_id,
                stage,
                reason="fingerprint_mismatch",
                fingerprints={"expected": expected_fp, "existing": existing["stage_fingerprint"]},
            )
            assert stage in removed
        product = producer()
        self.store.publish(
            doc_id=doc_id,
            stage=stage,
            input_fingerprint_value=input_fp,
            config_version=self.config["config_version"],
            stage_fingerprint=expected_fp,
            product=product,
        )
        products[stage] = product
        return product

    def _stop_report(self, doc_id: str, completed_through: str) -> dict[str, Any]:
        return {
            "doc_id": doc_id,
            "status": "stopped",
            "completed_through": completed_through,
            "resume_hint": "重新调用 run/run-document 即从下一阶段恢复",
        }

    # ---------- 全量运行 ----------
    def run(
        self,
        documents: list[dict[str, Any]],
        stop_check: StopCheck | None = None,
    ) -> list[dict[str, Any]]:
        stop_check = stop_check or (lambda: False)
        reports: list[dict[str, Any]] = []
        for document in sorted(documents, key=lambda doc: doc["doc_id"]):
            if stop_check():
                reports.append({
                    "doc_id": document["doc_id"],
                    "status": "stopped_before_start",
                })
                continue
            reports.append(self.run_document(document, stop_check))
        return reports

    # ---------- 单文档增量更新 + 与全量构建逐字节比对 ----------
    def update_document(self, document: dict[str, Any]) -> dict[str, Any]:
        doc_id = document["doc_id"]
        with self.store.lock_document(doc_id):
            self.store.save_input(document)
            incremental_report = self._run_locked(document, lambda: False)
            incremental_digests = self._all_product_digests(doc_id)
            incremental_result = self.store.load_stage(doc_id, STAGE_SUMMARY)["product"]

            baseline = self._build_isolated(document)
            baseline_digests = baseline["digests"]
            mismatch_stages = [
                stage for stage in STAGES
                if incremental_digests[stage] != baseline_digests[stage]
            ]
            if mismatch_stages:
                raise LayoutError(
                    "E_INCREMENTAL_MISMATCH",
                    f"文档 {doc_id} 增量结果与全量构建不一致",
                    {
                        "doc_id": doc_id,
                        "mismatch_stages": mismatch_stages,
                        "incremental": incremental_digests,
                        "baseline": baseline_digests,
                    },
                )
            if not _byte_equal(incremental_result, baseline["result"]):
                raise LayoutError(
                    "E_INCREMENTAL_MISMATCH",
                    f"文档 {doc_id} 最终结果逐字节比对失败",
                    {"doc_id": doc_id, "mismatch_stages": [STAGE_SUMMARY]},
                )
            return {
                "doc_id": doc_id,
                "status": "completed",
                "result_digest": incremental_report["result_digest"],
                "verified_against_full_build": True,
                "stage_digests": incremental_digests,
            }

    def _build_isolated(self, document: dict[str, Any]) -> dict[str, Any]:
        import tempfile

        with tempfile.TemporaryDirectory(prefix="layout-baseline-") as tmp:
            baseline_engine = PipelineEngine(tmp, self.config)
            baseline_engine.run_document(document)
            digests = {
                stage: baseline_engine.store.load_stage(document["doc_id"], stage)["product_digest"]
                for stage in STAGES
            }
            result = baseline_engine.store.load_stage(document["doc_id"], STAGE_SUMMARY)["product"]
            return {"digests": digests, "result": result}

    def _all_product_digests(self, doc_id: str) -> dict[str, str]:
        return {
            stage: self.store.load_stage(doc_id, stage)["product_digest"]
            for stage in STAGES
        }

    # ---------- 查询 ----------
    def document_status(self, doc_id: str) -> dict[str, Any]:
        records = {}
        fingerprints = {}
        for stage in STAGES:
            record = self.store.load_stage(doc_id, stage)
            records[stage] = self.store.stage_status(doc_id)[stage]
            if record is not None:
                fingerprints[stage] = record["stage_fingerprint"]
        return {
            "doc_id": doc_id,
            "config_version": self.config["config_version"],
            "stages": records,
            "stage_fingerprints": fingerprints,
            "invalidation_events": [
                event for event in self.store.read_invalidation_log()
                if event["doc_id"] == doc_id
            ],
        }


def _byte_equal(a: Any, b: Any) -> bool:
    return canonical_bytes(a) == canonical_bytes(b)
