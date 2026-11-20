"""检查点持久化、并发发布保护与失效记录。

目录布局（workspace 根下）::

    checkpoints/<doc_id>/stage-<n>-<stage>.json   原子检查点记录
    checkpoints/<doc_id>/input.json                原始输入快照
    locks/<doc_id>.lock                            跨进程文件锁
    fingerprints/invalidation.jsonl                追加式失效原因日志
    exports/                                       最终结果导出

所有写入均为「临时文件 + fsync + os.replace」原子替换，中断只可能留下
最终记录或旧记录，不会出现半写 JSON。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import threading
from typing import Any, Iterator

from .config import STAGES, STAGE_SEQ
from .errors import LayoutError
from .hashing import canonical_bytes, canonical_json, digest_bytes, digest_file


class CheckpointStore:
    def __init__(self, workspace: str):
        self.workspace = os.path.abspath(workspace)
        self.checkpoint_dir = os.path.join(self.workspace, "checkpoints")
        self.lock_dir = os.path.join(self.workspace, "locks")
        self.fingerprint_dir = os.path.join(self.workspace, "fingerprints")
        self.export_dir = os.path.join(self.workspace, "exports")
        # 同进程可重入：flock 绑定打开文件描述，重复加锁会死锁，
        # 因此用线程本地计数保证嵌套上下文只在最外层真正加/放锁。
        self._reentrant = threading.local()
        for path in (self.checkpoint_dir, self.lock_dir, self.fingerprint_dir, self.export_dir):
            os.makedirs(path, exist_ok=True)

    # ---------- 路径与锁 ----------
    def doc_dir(self, doc_id: str) -> str:
        path = os.path.join(self.checkpoint_dir, doc_id)
        os.makedirs(path, exist_ok=True)
        return path

    def stage_path(self, doc_id: str, stage: str) -> str:
        return os.path.join(self.doc_dir(doc_id), f"stage-{STAGE_SEQ[stage]}-{stage}.json")

    def input_path(self, doc_id: str) -> str:
        return os.path.join(self.doc_dir(doc_id), "input.json")

    @contextlib.contextmanager
    def lock_document(self, doc_id: str) -> Iterator[None]:
        held = getattr(self._reentrant, "held", None)
        if held is None:
            held = {}
            self._reentrant.held = held
        if doc_id in held:
            held[doc_id] += 1
            try:
                yield
            finally:
                held[doc_id] -= 1
                if held[doc_id] == 0:
                    del held[doc_id]
            return

        lock_path = os.path.join(self.lock_dir, f"{doc_id}.lock")
        handle = open(lock_path, "a+")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        held[doc_id] = 1
        try:
            yield
        finally:
            held[doc_id] -= 1
            if held[doc_id] == 0:
                del held[doc_id]
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    # ---------- 原子写 ----------
    @staticmethod
    def atomic_write(path: str, payload: dict[str, Any]) -> None:
        tmp_path = f"{path}.tmp"
        data = canonical_json(payload).encode("utf-8") + b"\n"
        with open(tmp_path, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        directory = os.path.dirname(path) or "."
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def save_input(self, document: dict[str, Any]) -> str:
        path = self.input_path(document["doc_id"])
        if not os.path.exists(path):
            self.atomic_write(path, document)
        return digest_file(path)

    # ---------- 读取 ----------
    def load_input(self, doc_id: str) -> dict[str, Any]:
        path = self.input_path(doc_id)
        if not os.path.exists(path):
            raise LayoutError(
                "E_DOC_NOT_FOUND",
                f"工作区不存在文档 {doc_id} 的输入快照",
                {"doc_id": doc_id},
            )
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def list_documents(self) -> list[str]:
        if not os.path.isdir(self.checkpoint_dir):
            return []
        return sorted(
            name for name in os.listdir(self.checkpoint_dir)
            if os.path.isdir(os.path.join(self.checkpoint_dir, name))
        )

    def load_stage(self, doc_id: str, stage: str) -> dict[str, Any] | None:
        path = self.stage_path(doc_id, stage)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def stage_status(self, doc_id: str) -> dict[str, Any]:
        status = {}
        for stage in STAGES:
            record = self.load_stage(doc_id, stage)
            if record is None:
                status[stage] = {"present": False}
            else:
                status[stage] = {
                    "present": True,
                    "seq": record["seq"],
                    "stage_fingerprint": record["stage_fingerprint"],
                    "input_fingerprint": record["input_fingerprint"],
                    "config_version": record["config_version"],
                    "product_digest": record["product_digest"],
                }
        return status

    # ---------- 发布（并发保护 + 序号校验 + 阶段跳转校验） ----------
    def publish(
        self,
        doc_id: str,
        stage: str,
        input_fingerprint_value: str,
        config_version: str,
        stage_fingerprint: str,
        product: dict[str, Any],
        claimed_doc_id: str | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGE_SEQ:
            raise LayoutError(
                "E_INVALID_STAGE",
                f"未知阶段 {stage}",
                {"doc_id": doc_id, "stage": stage},
            )
        if claimed_doc_id is not None and claimed_doc_id != doc_id:
            raise LayoutError(
                "E_STALE_CHECKPOINT",
                "跨文档错位发布被拒绝",
                {"expected_doc_id": doc_id, "claimed_doc_id": claimed_doc_id, "stage": stage},
            )

        with self.lock_document(doc_id):
            seq = STAGE_SEQ[stage]
            # 前置阶段必须存在，禁止非法阶段跳转
            if seq > 1:
                prev_stage = STAGES[seq - 2]
                prev_record = self.load_stage(doc_id, prev_stage)
                if prev_record is None:
                    raise LayoutError(
                        "E_INVALID_STAGE",
                        f"阶段 {stage} 缺失前置阶段 {prev_stage} 产物",
                        {"doc_id": doc_id, "stage": stage, "missing_stage": prev_stage},
                    )
            existing = self.load_stage(doc_id, stage)
            if existing is not None:
                if existing["seq"] != seq:
                    raise LayoutError(
                        "E_STALE_CHECKPOINT",
                        "检查点序号与阶段定义不一致",
                        {"doc_id": doc_id, "stage": stage, "existing_seq": existing["seq"],
                         "expected_seq": seq},
                    )
                if existing["stage_fingerprint"] == stage_fingerprint:
                    # 幂等重放：完全相同的指纹允许直接返回，不改动可用状态
                    return existing
                # 同序号覆盖需先经失效流程；这里拒绝直接覆盖落后/重复发布
                raise LayoutError(
                    "E_STALE_CHECKPOINT",
                    f"阶段 {stage} 已存在不同指纹的检查点，需先失效重建",
                    {"doc_id": doc_id, "stage": stage, "seq": seq},
                )

            # 序号连续推进：当前文档已发布的最大序号必须恰为 seq-1
            present_seqs = [
                STAGE_SEQ[name]
                for name in STAGES
                if self.load_stage(doc_id, name) is not None
            ]
            expected_prev = seq - 1
            if present_seqs and max(present_seqs) != expected_prev:
                raise LayoutError(
                    "E_STALE_CHECKPOINT",
                    "检查点序号不连续，发布被拒绝",
                    {"doc_id": doc_id, "stage": stage, "seq": seq,
                     "max_present_seq": max(present_seqs)},
                )

            product_digest = digest_bytes(canonical_bytes(product))
            record = {
                "doc_id": doc_id,
                "stage": stage,
                "seq": seq,
                "input_fingerprint": input_fingerprint_value,
                "config_version": config_version,
                "stage_fingerprint": stage_fingerprint,
                "product_digest": product_digest,
                "product_summary": _product_summary(stage, product),
                "product": product,
            }
            self.atomic_write(self.stage_path(doc_id, stage), record)
            return record

    # ---------- 失效 ----------
    def invalidate_from(self, doc_id: str, stage: str, reason: str,
                        fingerprints: dict[str, str]) -> list[str]:
        """使指定阶段及其全部下游产物失效并删除，返回被失效阶段列表。"""
        if stage not in STAGE_SEQ:
            raise LayoutError(
                "E_INVALID_STAGE",
                f"未知阶段 {stage}",
                {"doc_id": doc_id, "stage": stage},
            )
        with self.lock_document(doc_id):
            return self.invalidate_from_locked(doc_id, stage, reason, fingerprints)

    def invalidate_from_locked(self, doc_id: str, stage: str, reason: str,
                               fingerprints: dict[str, str]) -> list[str]:
        """调用方已持有文档锁时使用，避免重复释放文件锁。"""
        start = STAGE_SEQ[stage]
        removed: list[str] = []
        for target in STAGES:
            if STAGE_SEQ[target] >= start:
                path = self.stage_path(doc_id, target)
                if os.path.exists(path):
                    os.remove(path)
                    removed.append(target)
        self._append_invalidation(doc_id, stage, reason, fingerprints, removed)
        return removed

    def _append_invalidation(self, doc_id: str, stage: str, reason: str,
                             fingerprints: dict[str, str], removed: list[str]) -> None:
        log_path = os.path.join(self.fingerprint_dir, "invalidation.jsonl")
        entry = {
            "doc_id": doc_id,
            "earliest_invalid_stage": stage,
            "reason": reason,
            "stage_fingerprints": fingerprints,
            "removed_stages": removed,
        }
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(canonical_json(entry) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def read_invalidation_log(self) -> list[dict[str, Any]]:
        log_path = os.path.join(self.fingerprint_dir, "invalidation.jsonl")
        if not os.path.exists(log_path):
            return []
        with open(log_path, "r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    # ---------- 导出 ----------
    def export_result(self, doc_id: str, result: dict[str, Any]) -> str:
        path = os.path.join(self.export_dir, f"{doc_id}.result.json")
        self.atomic_write(path, result)
        return path


def _product_summary(stage: str, product: dict[str, Any]) -> dict[str, Any]:
    if stage == "normalize":
        return {"stats": product["stats"], "box_ids": [b["box_id"] for b in product["boxes"]]}
    if stage == "column":
        return {
            "column_count": product["column_count"],
            "voting_box_count": product["evidence"]["voting_box_count"],
            "spanning_box_ids": product["spanning_box_ids"],
        }
    if stage == "order":
        return {
            "block_count": product["stats"]["block_count"],
            "confidence_overall": product["confidence"]["overall"],
            "block_ids": [b["block_id"] for b in product["block_sequence"]],
        }
    if stage == "summary":
        return {
            "result_digest": product["result_digest"],
            "page_summary": product["page_summary"],
            "order_accuracy": product["order_quality"].get("order_accuracy"),
        }
    return {}
